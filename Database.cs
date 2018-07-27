using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using SchemaScripter.Models;

namespace SchemaScripter
{
    /// <summary>
    /// This is derived/built based on StackOverflow's StackExchange.DataExplorer project
    /// https://github.com/StackExchange/StackExchange.DataExplorer
    /// </summary>
    public class Database : IDatabase, IDisposable
    {
        private readonly string _connectionString;
        private readonly ILogger Logger;
        private IDbConnection _connection;
        private IDbTransaction _transaction;
        private const int DefaultCommandTimeout = 30;

        public IDbConnection Connection => _connection ?? (_connection = CreateConnection());

        public Database(IOptions<AppConfig> appConfig, ILogger<IDatabase> logger)
        {
            _connectionString = $"Server={appConfig.Value.Server};User ID={appConfig.Value.User};Password={appConfig.Value.Password};";
            //logger.LogDebug(_connectionString);
            Logger = logger;
        }
        
        private IDbConnection CreateConnection()
        {
            DbConnection connection = new SqlConnection(_connectionString);
            connection.Open();
            return connection;
        }

        public void BeginTransaction(IsolationLevel isolation = IsolationLevel.ReadCommitted)
        {
            if (_transaction == null)
                _transaction = Connection.BeginTransaction(isolation);
        }

        public virtual void CommitTransaction()
        {
            _transaction?.Commit();
            _transaction = null;
        }

        public virtual void RollbackTransaction()
        {
            _transaction?.Rollback();
            _transaction = null;
        }

        public void Dispose()
        {
            if (_connection != null && _connection.State != ConnectionState.Closed)
            {
                _transaction?.Rollback();
                _connection.Close();
                _connection = null;
            }
        }

        public async Task<(bool IsSuccess, string ErrorMessage)> CheckAvailabilityAsync()
        { 
            try 
            { 
                await Connection.ExecuteAsync("SELECT 123");
                return (true, null);
            } 
            catch (Exception e)
            { 
                if (e.Message.Contains("server was not found"))
                    return (false, "Server was not found.");
                if (e.Message.StartsWith("Login failed"))
                    return (false, e.Message);
                return (false, null);
            } 
        }

        public async Task<bool> UseDatabaseAsync(string Database)
        { 
            try 
            { 
                await Connection.ExecuteAsync($"USE [{Database}]");
                return true;
            } 
            catch 
            { 
                return false; 
            } 
        }

        public async Task<List<Table>> SelectTablesAsync()
        {
            const string sql = @"
;WITH PartitionColumns AS (
    SELECT
         pc_t.object_id
        ,definitions = '[' + STRING_AGG(CONVERT(NVARCHAR(MAX),pc_c.name), '], [') WITHIN GROUP (ORDER BY pc_ic.partition_ordinal ASC) + ']'
    FROM sys.tables AS pc_t
        JOIN sys.indexes pc_i ON pc_i.object_id = pc_t.object_id AND pc_i.index_id < 2
        JOIN sys.index_columns pc_ic ON pc_ic.partition_ordinal > 0 AND pc_ic.index_id = pc_i.index_id AND pc_ic.object_id = pc_t.object_id
        JOIN sys.columns pc_c ON pc_c.object_id = pc_ic.object_id AND pc_c.column_id = pc_ic.column_id
    GROUP BY pc_t.object_id
), TableFilegroup AS (
    SELECT
         fg_t.object_id
        ,ISNULL(fg_ps.name,fg_fg.name) AS filegroup_name
    FROM sys.tables fg_t
    JOIN sys.indexes fg_i ON fg_i.object_id = fg_t.object_id AND fg_i.index_id < 2
    JOIN sys.partitions fg_p ON fg_p.object_id = fg_i.object_id AND fg_p.index_id = fg_i.index_id
    LEFT OUTER JOIN sys.partition_schemes fg_ps ON fg_ps.data_space_id = fg_i.data_space_id
    LEFT OUTER JOIN sys.destination_data_spaces fg_dds ON fg_dds.partition_scheme_id = fg_ps.data_space_id AND fg_dds.destination_id = fg_p.partition_number
    JOIN sys.filegroups fg_fg ON fg_fg.data_space_id = COALESCE(fg_dds.data_space_id, fg_i.data_space_id)
    GROUP BY fg_t.object_id, ISNULL(fg_ps.name,fg_fg.name)
), TableColumns AS (
    SELECT
        c.object_id
        ,definitions = STRING_AGG(
                           -- Leading tab (force to NVARCHAR(MAX) to alleviate 8000 byte limit on STRING_AGG)
                           CONVERT(NVARCHAR(MAX), CHAR(9))
                           -- Column name and type
                           + '[' + c.name + '] '
                           + CASE
                                 WHEN c.is_computed = 1 THEN ' AS ' + cc.definition + CASE WHEN cc.is_persisted = 1 THEN ' PERSISTED' + CASE WHEN c.is_nullable = 1 THEN '' ELSE ' NOT NULL' END ELSE '' END
                                 ELSE
                                     -- Type name
                                     + '[' + ty.name + ']'
                                     -- Handle variable-length
                                     + CASE WHEN ty.name IN ('binary','varbinary','char','varchar') THEN '(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CONVERT(varchar, c.max_length) END + ')' ELSE '' END
                                     -- Handle variable-length (unicode, must divide by 2 on max_length)
                                     + CASE WHEN ty.name IN ('nchar', 'nvarchar') THEN '(' + CASE WHEN c.max_length = -1 THEN 'max' ELSE CONVERT(varchar, c.max_length / 2) END + ')' ELSE '' END
                                     -- Handle precision + scale
                                     + CASE WHEN ty.name IN ('numeric','decimal') THEN '(' + CONVERT(varchar, c.precision) + ', ' + CONVERT(varchar, c.scale) + ')' ELSE '' END
                                     -- Handle scale
                                     + CASE WHEN ty.name IN ('datetime2','datetimeoffset','time') THEN '(' + CONVERT(varchar, c.scale) + ')' ELSE '' END
                                     -- Handle collation
                                     + CASE WHEN c.is_computed = 0 AND c.collation_name IS NOT NULL THEN ' COLLATE ' + c.collation_name ELSE '' END
                                     -- Handle IDENTITY
                                     + CASE
                                           WHEN c.is_identity = 1 THEN ' IDENTITY(' + CONVERT(varchar, ic.seed_value) + ',' + CONVERT(varchar, ic.increment_value) + ')' + CASE WHEN ic.is_not_for_replication = 1 THEN ' NOT FOR REPLICATION' ELSE '' END
                                           ELSE ''
                                      END
                                     -- Handle ROWGUIDCOL
                                     + CASE WHEN c.is_rowguidcol = 1 THEN ' ROWGUIDCOL ' ELSE '' END
                                     -- NULL/NOT NULL
                                     + CASE WHEN c.is_nullable = 1 THEN ' NULL' ELSE ' NOT NULL' END
                             END
                       , ',' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY c.column_id ASC)
    FROM sys.columns c
        JOIN sys.types ty ON c.system_type_id = ty.system_type_id AND ty.name != 'sysname'
        LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
    GROUP BY c.object_id
), PrimaryKey AS (
    SELECT
         pk_i.object_id
        ,definition = ',' + CHAR(13) + CHAR(10)
                      + CASE
                            WHEN PATINDEX('PK__' + SUBSTRING(pk_t.name, 0, 8) + '__%', pk_i.name) > 0 THEN ''
                            ELSE ' CONSTRAINT [' + pk_i.name + '] ' END
                      + 'PRIMARY KEY ' + pk_i.type_desc COLLATE DATABASE_DEFAULT + ' ' + CHAR(13) + CHAR(10)
                      + '(' + CHAR(13) + CHAR(10)
                      + STRING_AGG(
                             -- Leading tab (force to NVARCHAR(MAX) to alleviate 8000 byte limit on STRING_AGG)
                             CONVERT(NVARCHAR(MAX), CHAR(9))
                             -- PK Name and order
                             + '[' + pk_c.name + ']' + CASE WHEN pk_ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                        , ',' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY pk_ic.key_ordinal ASC) + CHAR(13) + CHAR(10)
                      + ')WITH ('
                      + 'PAD_INDEX = ' + CASE WHEN pk_i.is_padded = 1 THEN 'ON' ELSE 'OFF' END + ', '
                      + 'STATISTICS_NORECOMPUTE = ' + CASE WHEN pk_s.no_recompute = 1 THEN 'ON' ELSE 'OFF' END + ', '
                      + 'IGNORE_DUP_KEY = ' + CASE WHEN pk_i.ignore_dup_key = 1 THEN 'ON' ELSE 'OFF' END + ', '
                      + 'ALLOW_ROW_LOCKS = ' + CASE WHEN pk_i.allow_row_locks = 1 THEN 'ON' ELSE 'OFF' END + ', '
                      + 'ALLOW_PAGE_LOCKS = ' + CASE WHEN pk_i.allow_page_locks = 1 THEN 'ON' ELSE 'OFF' END
                      + CASE WHEN pk_i.fill_factor > 0 THEN ', FILLFACTOR = ' + CONVERT(varchar, pk_i.fill_factor) ELSE '' END
                      + CASE WHEN pk_p.data_compression_desc != 'NONE' THEN ', DATA_COMPRESSION = ' + pk_p.data_compression_desc ELSE '' END
                      + ') ON [' + pk_ds.name + ']' + CASE WHEN pk_pc.object_id IS NOT NULL THEN '(' + pk_pc.definitions + ')' ELSE '' END
    FROM sys.indexes pk_i
        JOIN sys.index_columns pk_ic ON pk_i.object_id = pk_ic.object_id AND pk_i.index_id = pk_ic.index_id
        JOIN sys.columns pk_c ON pk_ic.object_id = pk_c.object_id AND pk_ic.column_id = pk_c.column_id
        JOIN sys.data_spaces pk_ds ON pk_i.data_space_id = pk_ds.data_space_id
        JOIN sys.stats pk_s ON pk_s.object_id = pk_i.object_id AND pk_s.stats_id = pk_i.index_id
        JOIN sys.tables pk_t ON pk_t.object_id = pk_i.object_id
        LEFT JOIN sys.partitions pk_p ON pk_p.object_id = pk_i.object_id AND pk_p.index_id = pk_i.index_id AND pk_p.partition_number = 1
        LEFT JOIN PartitionColumns pk_pc ON pk_pc.object_id = pk_i.object_id
    WHERE pk_i.is_primary_key = 1
    GROUP BY pk_i.object_id, pk_i.name, pk_i.type_desc, pk_i.is_padded, pk_i.ignore_dup_key, pk_i.allow_row_locks, pk_i.allow_page_locks, pk_i.fill_factor, pk_ds.name, pk_s.no_recompute, pk_t.name, pk_p.data_compression_desc, pk_pc.object_id, pk_pc.definitions
), UniqueConstraints AS (
    SELECT
         uq.object_id
        ,definitions = STRING_AGG(CONVERT(NVARCHAR(MAX), uq.definition), CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY uq.definition ASC)
    FROM
    (
        SELECT
             uq_i.object_id
            ,definition = ' CONSTRAINT [' + uq_i.name + '] UNIQUE ' + uq_i.type_desc COLLATE DATABASE_DEFAULT + ' ' + CHAR(13) + CHAR(10)
                          + '(' + CHAR(13) + CHAR(10)
                          + STRING_AGG(
                                 -- Leading tab (force to NVARCHAR(MAX) to alleviate 8000 byte limit on STRING_AGG)
                                 CONVERT(NVARCHAR(MAX), CHAR(9))
                                 -- PK Name and order
                                 + '[' + uq_c.name + ']' + CASE WHEN uq_ic.is_descending_key = 1 THEN ' DESC' ELSE ' ASC' END
                            , ',' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY uq_ic.key_ordinal ASC) + CHAR(13) + CHAR(10)
                          + ')WITH ('
                          + 'PAD_INDEX = ' + CASE WHEN uq_i.is_padded = 1 THEN 'ON' ELSE 'OFF' END + ', '
                          + 'STATISTICS_NORECOMPUTE = ' + CASE WHEN uq_s.no_recompute = 1 THEN 'ON' ELSE 'OFF' END + ', '
                          + 'IGNORE_DUP_KEY = ' + CASE WHEN uq_i.ignore_dup_key = 1 THEN 'ON' ELSE 'OFF' END + ', '
                          + 'ALLOW_ROW_LOCKS = ' + CASE WHEN uq_i.allow_row_locks = 1 THEN 'ON' ELSE 'OFF' END + ', '
                          + 'ALLOW_PAGE_LOCKS = ' + CASE WHEN uq_i.allow_page_locks = 1 THEN 'ON' ELSE 'OFF' END
                          + CASE WHEN uq_i.fill_factor > 0 THEN ', FILLFACTOR = ' + CONVERT(varchar, uq_i.fill_factor) ELSE '' END
                          + CASE WHEN uq_p.data_compression_desc != 'NONE' THEN ', DATA_COMPRESSION = ' + uq_p.data_compression_desc ELSE '' END
                          + ') ON [' + uq_ds.name + ']' + CHAR(13) + CHAR(10)
        FROM sys.indexes uq_i
            JOIN sys.index_columns uq_ic ON uq_i.object_id = uq_ic.object_id AND uq_i.index_id = uq_ic.index_id
            JOIN sys.columns uq_c ON uq_ic.object_id = uq_c.object_id AND uq_ic.column_id = uq_c.column_id
            JOIN sys.data_spaces uq_ds ON uq_i.data_space_id = uq_ds.data_space_id
            JOIN sys.stats uq_s ON uq_s.object_id = uq_i.object_id AND uq_s.stats_id = uq_i.index_id
            JOIN sys.tables uq_t ON uq_t.object_id = uq_i.object_id
            LEFT JOIN sys.partitions uq_p ON uq_p.object_id = uq_i.object_id AND uq_p.index_id = uq_i.index_id AND uq_p.partition_number = 1
        WHERE uq_i.is_unique_constraint = 1
        GROUP BY uq_i.object_id, uq_i.name, uq_i.type_desc, uq_i.is_padded, uq_i.ignore_dup_key, uq_i.allow_row_locks, uq_i.allow_page_locks, uq_i.fill_factor, uq_ds.name, uq_s.no_recompute, uq_t.name, uq_p.data_compression_desc
    ) uq
    GROUP BY uq.object_id
), DefaultConstraints AS (
    SELECT
         dc.parent_object_id
        ,definitions = STRING_AGG(
                             -- Force to NVARCHAR(MAX) on initial item to alleviate 8000 byte limit on STRING_AGG
                             CONVERT(NVARCHAR(MAX), 'ALTER TABLE [') + dc_s.name + '].[' + dc_t.name + '] ADD'
                           + CASE
                                 WHEN PATINDEX('DF__' + SUBSTRING(dc_t.name, 0, 9) + '__%', dc.name) > 0 THEN ''
                                 ELSE '  CONSTRAINT [' + dc.name + ']'
                             END
                           + '  DEFAULT ' + dc.definition
                           + ' FOR [' + dc_c.name + ']'
                       , CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY dc.parent_column_id ASC)
                       + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)
    FROM sys.default_constraints dc
        JOIN sys.tables dc_t ON dc.parent_object_id = dc_t.object_id
        JOIN sys.schemas dc_s ON dc_s.schema_id = dc_t.schema_id
        JOIN sys.columns dc_c ON dc_c.object_id = dc_t.object_id AND dc.parent_column_id = dc_c.column_id
    GROUP BY dc.parent_object_id
), ForeignKeyConstraintColumns AS (
    SELECT
         c.object_id
        ,fkc.constraint_object_id
        ,STRING_AGG(name, '], [') WITHIN GROUP (ORDER BY fkc.constraint_column_id ASC) AS names
    FROM sys.foreign_key_columns fkc
    JOIN sys.columns c ON fkc.parent_object_id = c.object_id AND fkc.parent_column_id = c.column_id
    GROUP BY c.object_id, fkc.constraint_object_id
), ForeignKeyReferencedColumns AS (
    SELECT
         c.object_id
        ,fkc.constraint_object_id
        ,STRING_AGG(name, '], [') WITHIN GROUP (ORDER BY fkc.referenced_column_id ASC) AS names
    FROM sys.foreign_key_columns fkc
    JOIN sys.columns c ON fkc.referenced_object_id = c.object_id AND fkc.referenced_column_id = c.column_id
    GROUP BY c.object_id, fkc.constraint_object_id
), ForeignKeyConstraints AS (
    SELECT
         fk.parent_object_id
        ,Definitions = STRING_AGG(
                             -- Force to NVARCHAR(MAX) on initial item to alleviate 8000 byte limit on STRING_AGG
                             CONVERT(NVARCHAR(MAX), 'ALTER TABLE [') + fk_s.name + '].[' + fk_t.name + ']'
                           + '  WITH ' + CASE WHEN fk.is_not_trusted = 1 THEN 'NOCHECK' ELSE 'CHECK' END
                           + ' ADD  CONSTRAINT [' + fk.name + ']'
                           + ' FOREIGN KEY([' + fk_c_c.names + '])' + CHAR(13) + CHAR(10)
                           + 'REFERENCES [' + fk_r_s.name + '].[' + fk_r_t.name + '] ([' + fk_r_c.names + '])' + CHAR(13) + CHAR(10)
                           + CASE WHEN fk.update_referential_action > 0 THEN 'ON UPDATE ' + fk.update_referential_action_desc COLLATE DATABASE_DEFAULT + CHAR(13) + CHAR(10) ELSE '' END
                           + CASE WHEN fk.delete_referential_action > 0 THEN 'ON DELETE ' + fk.delete_referential_action_desc COLLATE DATABASE_DEFAULT + CHAR(13) + CHAR(10) ELSE '' END
                           + 'GO' + CHAR(13) + CHAR(10)
                           + 'ALTER TABLE [' + fk_s.name + '].[' + fk_t.name + '] CHECK CONSTRAINT [' + fk.name + ']'
                       , CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY fk.name ASC)
                       + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)
    FROM sys.foreign_keys fk
        JOIN sys.tables fk_t ON fk.parent_object_id = fk_t.object_id
        JOIN sys.schemas fk_s ON fk_s.schema_id = fk_t.schema_id
        JOIN sys.tables fk_c_t ON fk_c_t.object_id = fk.parent_object_id
        JOIN ForeignKeyConstraintColumns fk_c_c ON fk.parent_object_id = fk_c_c.object_id AND fk.object_id = fk_c_c.constraint_object_id
        JOIN sys.tables fk_r_t ON fk_r_t.object_id = fk.referenced_object_id
        JOIN sys.schemas fk_r_s ON fk_r_s.schema_id = fk_r_t.schema_id
        JOIN ForeignKeyReferencedColumns fk_r_c ON fk_r_t.object_id = fk_r_c.object_id AND fk.object_id = fk_r_c.constraint_object_id
    GROUP BY fk.parent_object_id
), CheckConstraints AS (
    SELECT
         cc.parent_object_id
        ,definitions = STRING_AGG(
                             -- Force to NVARCHAR(MAX) on initial item to alleviate 8000 byte limit on STRING_AGG
                             CONVERT(NVARCHAR(MAX), 'ALTER TABLE [') + cc_s.name + '].[' + cc_t.name + ']'
                           + '  WITH ' + CASE WHEN cc.is_not_trusted = 1 THEN 'NOCHECK' ELSE 'CHECK' END
                           + ' ADD  CONSTRAINT [' + cc.name + ']'
                           + ' CHECK  (' + cc.definition + ')' + CHAR(13) + CHAR(10)
                           + 'GO' + CHAR(13) + CHAR(10)
                           + 'ALTER TABLE [' + cc_s.name + '].[' + cc_t.name + '] CHECK CONSTRAINT [' + cc.name + ']'
                       , CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY cc.object_id ASC)
                       + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)
    FROM sys.check_constraints cc
        JOIN sys.tables cc_t ON cc.parent_object_id = cc_t.object_id
        JOIN sys.schemas cc_s ON cc_s.schema_id = cc_t.schema_id
    GROUP BY cc.parent_object_id
), IndexColumns AS (
    SELECT
         ic_i.object_id
        ,ic_i.index_id
        ,definitions = '(' + CHAR(13) + CHAR(10)
                       + STRING_AGG(
                                    -- Leading tab (force to NVARCHAR(MAX) to alleviate 8000 byte limit on STRING_AGG)
                                    CONVERT(NVARCHAR(MAX), CHAR(9))
                                    -- Column name and type
                                    + '[' + ic_c.name + '] '
                                    + CASE WHEN ic_ic.is_descending_key = 0 THEN 'ASC' ELSE 'DESC' END
                                    , ',' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY ic_ic.key_ordinal ASC)
                       + + CHAR(13) + CHAR(10) + ')'
    FROM sys.indexes ic_i
        JOIN sys.index_columns ic_ic ON ic_ic.index_id = ic_i.index_id AND ic_ic.object_id = ic_i.object_id
        JOIN sys.columns ic_c ON ic_c.object_id = ic_ic.object_id AND ic_c.column_id = ic_ic.column_id
    WHERE ic_i.is_primary_key = 0 AND ic_ic.is_included_column = 0 AND (ic_ic.partition_ordinal = 0 OR ic_ic.key_ordinal > 1)
    GROUP BY ic_i.object_id, ic_i.index_id
), IndexIncludedColumns AS (
    SELECT
         ic_i.object_id
        ,ic_i.index_id
        ,definitions = CHAR(13) + CHAR(10) + 'INCLUDE ( '
                       + STRING_AGG(
                                    -- Leading tab (force to NVARCHAR(MAX) to alleviate 8000 byte limit on STRING_AGG)
                                    CONVERT(NVARCHAR(MAX), CHAR(9))
                                    -- Column name and type
                                    + '[' + ic_c.name + ']'
                                    , ',' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY ic_ic.key_ordinal ASC)
                       + ') '
    FROM sys.indexes ic_i
        JOIN sys.index_columns ic_ic ON ic_ic.index_id = ic_i.index_id AND ic_ic.object_id = ic_i.object_id
        JOIN sys.columns ic_c ON ic_c.object_id = ic_ic.object_id AND ic_c.column_id = ic_ic.column_id
    WHERE ic_i.is_primary_key = 0 AND ic_i.is_unique_constraint = 0 AND ic_ic.is_included_column = 1
    GROUP BY ic_i.object_id, ic_i.index_id
), Indexes AS (
    SELECT
         i.object_id
        ,definitions = STRING_AGG(
                                 -- Force to NVARCHAR(MAX) on initial item to alleviate 8000 byte limit on STRING_AGG
                                 CONVERT(NVARCHAR(MAX), '')
                               + CASE WHEN i_ap.index_id IS NOT NULL THEN 'SET ANSI_PADDING ON' + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10) ELSE '' END
                               + 'CREATE '
                               + CASE WHEN i.is_unique = 1 THEN 'UNIQUE ' ELSE '' END
                               + i.type_desc COLLATE DATABASE_DEFAULT
                               + ' INDEX [' + i.name + ']'
                               + ' ON [' + i_s.name + '].[' + i_t.name + ']' + CHAR(13) + CHAR(10)
                               + cte_ic.definitions
                               + ISNULL(cte_iic.definitions,'')
                               + CASE WHEN i.has_filter = 1 THEN CHAR(13) + CHAR(10) + 'WHERE ' + i.filter_definition + CHAR(13) + CHAR(10) ELSE '' END
                               + 'WITH ('
                               + 'PAD_INDEX = ' + CASE WHEN i.is_padded = 1 THEN 'ON' ELSE 'OFF' END + ', '
                               + 'STATISTICS_NORECOMPUTE = ' + CASE WHEN i_st.no_recompute = 1 THEN 'ON' ELSE 'OFF' END + ', '
                               + 'SORT_IN_TEMPDB = OFF, ' -- This is always OFF (at least for our case), and it's not stored in any metadata
                               + CASE WHEN i.is_unique = 1 THEN 'IGNORE_DUP_KEY = ' + CASE WHEN i.ignore_dup_key = 1 THEN 'ON' ELSE 'OFF' END + ', ' ELSE '' END
                               + 'DROP_EXISTING = OFF, ' -- This is always OFF
                               + 'ONLINE = OFF, ' -- This is always OFF
                               + 'ALLOW_ROW_LOCKS = ' + CASE WHEN i.allow_row_locks = 1 THEN 'ON' ELSE 'OFF' END + ', '
                               + 'ALLOW_PAGE_LOCKS = ' + CASE WHEN i.allow_page_locks = 1 THEN 'ON' ELSE 'OFF' END
                               + CASE WHEN i.fill_factor > 0 THEN ', FILLFACTOR = ' + CONVERT(varchar, i.fill_factor) ELSE '' END
                               + CASE WHEN i_p.data_compression_desc != 'NONE' THEN ', DATA_COMPRESSION = ' + i_p.data_compression_desc ELSE '' END
                               + ') ON [' + i_ds.name + ']' + CASE WHEN partitioning_column.column_name IS NOT NULL THEN '(' + partitioning_column.column_name + ')' ELSE '' END
                               + CASE WHEN i.is_disabled = 1 THEN CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10) +  'ALTER INDEX [' + i.name + '] ON [' + i_s.name +'].[' + i_t.name + '] DISABLE' ELSE '' END
                           , CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)) WITHIN GROUP (ORDER BY i.name ASC)
                           + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)
    FROM sys.indexes i
        JOIN sys.tables i_t ON i_t.object_id = i.object_id
        JOIN sys.schemas i_s ON i_s.schema_id = i_t.schema_id
        JOIN sys.stats i_st ON i_st.object_id = i.object_id AND i_st.stats_id = i.index_id
        JOIN sys.data_spaces i_ds ON i.data_space_id = i_ds.data_space_id
        JOIN IndexColumns cte_ic ON cte_ic.object_id = i.object_id AND cte_ic.index_id = i.index_id
        LEFT JOIN IndexIncludedColumns cte_iic ON cte_iic.object_id = i.object_id AND cte_iic.index_id = i.index_id
        LEFT JOIN sys.partitions i_p ON i_p.object_id = i.object_id AND i_p.index_id = i.index_id AND i_p.partition_number = 1
        LEFT JOIN (
            SELECT ap_i.object_id, ap_i.index_id, MAX(CASE WHEN ap_c.is_ansi_padded = 1 THEN 1 ELSE 0 END) AS is_ansi_padded
            FROM sys.indexes ap_i
                JOIN sys.index_columns ap_ic ON ap_i.object_id = ap_ic.object_id AND ap_i.index_id = ap_ic.index_id
                JOIN sys.columns ap_c ON ap_c.object_id = ap_i.object_id AND ap_c.column_id = ap_ic.column_id
            GROUP BY ap_i.object_id, ap_i.index_id
        ) i_ap ON i.object_id = i_ap.object_id AND i.index_id = i_ap.index_id AND i_ap.is_ansi_padded = 1
        OUTER APPLY (
            SELECT MAX(QUOTENAME(c.name)) AS column_name
            FROM sys.index_columns AS ic
            JOIN sys.columns AS c ON ic.column_id=c.column_id AND ic.object_id=c.object_id
            WHERE ic.object_id = i.object_id AND ic.index_id=i.index_id AND ic.partition_ordinal = 1
        ) AS partitioning_column
    WHERE i.is_primary_key = 0 AND i.is_unique_constraint = 0
    GROUP BY i.object_id
), Triggers AS (
    SELECT
         tr.parent_id
        ,definitions =  'SET ANSI_NULLS ' + CASE WHEN tr_m.uses_ansi_nulls = 1 THEN 'ON' ELSE 'OFF' END + CHAR(13) + CHAR(10)
                      + 'GO' + CHAR(13) + CHAR(10)
                      + 'SET QUOTED_IDENTIFIER ' + CASE WHEN tr_m.uses_quoted_identifier = 1 THEN 'ON' ELSE 'OFF' END + CHAR(13) + CHAR(10)
                      + 'GO' + CHAR(13) + CHAR(10)
                      + tr_m.definition + CASE WHEN RIGHT(tr_m.definition, 1) = CHAR(10) THEN '' ELSE CHAR(13) + CHAR(10) END
                      + 'GO' + CHAR(13) + CHAR(10)
                      + CASE
                            WHEN tr.is_disabled = 1 THEN ''
                            ELSE 'ALTER TABLE [' + tr_s.name + '].[' + tr_t.name + '] ENABLE TRIGGER [' + tr.name + ']' + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10)
                        END
    FROM sys.triggers tr
        JOIN sys.sql_modules tr_m ON tr_m.object_id = tr.object_id
        JOIN sys.objects tr_obj ON tr_obj.object_id = tr_m.object_id
        JOIN sys.tables tr_t ON tr_t.object_id = tr.parent_id
        JOIN sys.schemas tr_s ON tr_s.schema_id = tr_t.schema_id
    WHERE tr_obj.type ='TR'
), TableCompression AS (
    SELECT
         tc_o.object_id
        ,tc_p.data_compression_desc
    FROM sys.partitions tc_p
        JOIN sys.objects tc_o ON tc_p.object_id = tc_o.object_id
    WHERE data_compression > 0
)
SELECT
     t.object_id AS ObjectId
    ,s.name AS SchemaName
    ,t.name AS TableName
    ,  'SET ANSI_NULLS ' + CASE WHEN t.uses_ansi_nulls = 1 THEN 'ON' ELSE 'OFF' END + CHAR(13) + CHAR(10)
     + 'GO' + CHAR(13) + CHAR(10)
     + 'SET QUOTED_IDENTIFIER ON' + CHAR(13) + CHAR(10)
     + 'GO' + CHAR(13) + CHAR(10)
     + 'CREATE TABLE [' + s.name + '].[' + t.name + '](' + CHAR(13) + CHAR(10)
     + ISNULL(cte_tc.definitions,'')
     + ISNULL(cte_pk.definition,'')
     + CASE WHEN cte_uq.definitions IS NOT NULL THEN ',' ELSE '' END + CHAR(13) + CHAR(10)
     + ISNULL(cte_uq.definitions,'')
     + ') ON [' + cte_fg.filegroup_name + ']' + CASE WHEN cte_pc.object_id IS NOT NULL THEN '(' + cte_pc.definitions + ')' ELSE '' END
     + CASE WHEN cte_pc.object_id IS NULL AND lob_ds.data_space_id IS NOT NULL THEN ' TEXTIMAGE_ON [' + lob_ds.name + ']' ELSE '' END + CHAR(13) + CHAR(10)
     + CASE WHEN CHARINDEX('DATA_COMPRESSION', cte_pk.definition) <= 0 AND cte_tcomp.object_id IS NOT NULL AND cte_tcomp.data_compression_desc != 'NONE' THEN 'WITH' + CHAR(13) + CHAR(10) +'(' + CHAR(13) + CHAR(10) + 'DATA_COMPRESSION = ' + cte_tcomp.data_compression_desc + CHAR(13) + CHAR(10) + ')' + CHAR(13) + CHAR(10) ELSE '' END
     + 'GO' + CHAR(13) + CHAR(10)
     + CASE WHEN t.lock_escalation > 0 THEN 'ALTER TABLE [' + s.name + '].[' + t.name + '] SET (LOCK_ESCALATION = ' + t.lock_escalation_desc + ')' + CHAR(13) + CHAR(10) + 'GO' + CHAR(13) + CHAR(10) ELSE '' END
     + ISNULL(cte_i.definitions,'')
     + ISNULL(cte_d.definitions,'')
     + ISNULL(cte_fk.definitions,'')
     + ISNULL(cte_tr.definitions,'')
     + ISNULL(cte_cc.definitions,'')
     AS Definition
FROM sys.tables t
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    JOIN TableFilegroup cte_fg ON cte_fg.object_id = t.object_id
    LEFT JOIN PartitionColumns cte_pc ON cte_pc.object_id = t.object_id
    LEFT JOIN sys.data_spaces lob_ds on lob_ds.data_space_id = t.lob_data_space_id
    LEFT JOIN TableColumns cte_tc ON cte_tc.object_id = t.object_id
    LEFT JOIN PrimaryKey cte_pk ON cte_pk.object_id = t.object_id
    LEFT JOIN UniqueConstraints cte_uq ON cte_uq.object_id = t.object_id
    LEFT JOIN DefaultConstraints cte_d ON cte_d.parent_object_id = t.object_id
    LEFT JOIN ForeignKeyConstraints cte_fk ON cte_fk.parent_object_id = t.object_id
    LEFT JOIN CheckConstraints cte_cc ON cte_cc.parent_object_id = t.object_id
    LEFT JOIN Indexes cte_i ON cte_i.object_id = t.object_id
    LEFT JOIN Triggers cte_tr ON cte_tr.parent_id = t.object_id
    LEFT JOIN TableCompression cte_tcomp ON cte_tcomp.object_id = t.object_id
WHERE
    -- No system objects
    t.is_ms_shipped = 0
    AND t.object_id NOT IN (SELECT major_id FROM sys.extended_properties WHERE NAME = N'microsoft_database_tools_support' AND minor_id = 0 AND class = 1)
ORDER BY
    s.name,
    t.name
";

            return (await Connection.QueryAsync<Table>(sql))?.AsList();
        }

        public async Task<List<StoredProcedure>> SelectStoredProceduresAsync()
        {
            const string sql = @"
SELECT
     p.object_id AS ObjectId
    ,s.name AS SchemaName
    ,p.name AS ProcedureName
    ,m.definition AS Definition
    ,m.uses_ansi_nulls AS UsesAnsiNulls
    ,m.uses_quoted_identifier AS UsesQuotedIdentifier
FROM sys.procedures p
    JOIN sys.schemas s ON s.schema_id = p.schema_id
    JOIN sys.sql_modules m ON m.object_id = p.object_id
WHERE
    -- No system objects
    p.is_ms_shipped = 0
    AND p.object_id NOT IN (SELECT major_id FROM sys.extended_properties WHERE NAME = N'microsoft_database_tools_support' AND minor_id = 0 AND class = 1)
ORDER BY
    s.name,
    p.name
";

            return (await Connection.QueryAsync<StoredProcedure>(sql))?.AsList();
        }
        
        public async Task<List<View>> SelectViewsAsync()
        {
            const string sql = @"
SELECT
     v.object_id AS ObjectId
    ,s.name AS SchemaName
    ,v.name AS ViewName
    ,m.definition AS Definition
    ,m.uses_ansi_nulls AS UsesAnsiNulls
    ,m.uses_quoted_identifier AS UsesQuotedIdentifier
FROM sys.views v
    JOIN sys.schemas s ON s.schema_id = v.schema_id
    JOIN sys.sql_modules m ON m.object_id = v.object_id
WHERE
    -- No system objects
    v.is_ms_shipped = 0
    AND v.object_id NOT IN (SELECT major_id FROM sys.extended_properties WHERE NAME = N'microsoft_database_tools_support' AND minor_id = 0 AND class = 1)
ORDER BY
    s.name,
    v.name
";

            return (await Connection.QueryAsync<View>(sql))?.AsList();
        }
        
        public async Task<List<Function>> SelectFunctionsAsync()
        {
            const string sql = @"
SELECT
     o.object_id AS ObjectId
    ,s.name AS SchemaName
    ,o.name AS FunctionName
    ,m.definition AS Definition
    ,m.uses_ansi_nulls AS UsesAnsiNulls
    ,m.uses_quoted_identifier AS UsesQuotedIdentifier
FROM sys.objects o
    JOIN sys.schemas s ON s.schema_id = o.schema_id
    JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE
    -- No system objects
    o.is_ms_shipped = 0
    AND o.object_id NOT IN (SELECT major_id FROM sys.extended_properties WHERE NAME = N'microsoft_database_tools_support' AND minor_id = 0 AND class = 1)
    AND o.type = 'FN'
ORDER BY
    s.name,
    o.name

";

            return (await Connection.QueryAsync<Function>(sql))?.AsList();
        }
    }

    public interface IDatabase
    {
        IDbConnection Connection { get; }
        void BeginTransaction(IsolationLevel isolation = IsolationLevel.ReadCommitted);
        void CommitTransaction();
        void RollbackTransaction();
        void Dispose();

        Task<(bool IsSuccess, string ErrorMessage)> CheckAvailabilityAsync();
        Task<bool> UseDatabaseAsync(string Database);
        Task<List<Table>> SelectTablesAsync();
        Task<List<StoredProcedure>> SelectStoredProceduresAsync();
        Task<List<View>> SelectViewsAsync();
        Task<List<Function>> SelectFunctionsAsync();
    }
}
