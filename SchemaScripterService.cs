using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Sdk.Sfc;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Smo.Agent;
using StackExchange.Profiling;

namespace SchemaScripter
{
    public class SchemaScripterService : IHostedService
    {
        private readonly IApplicationLifetime ApplicationLifetime;
        private readonly ILogger Logger;
        private readonly AppConfig Config;
        private readonly IDatabase DB;
        private MiniProfiler Profiler;

        private static readonly Regex Proc_SchemaNameRegex = new Regex(".*CREATE[ ]+PROCEDURE[ ]+[\\[]{0,1}([a-zA-Z0-9_ ]+)[\\]]{0,1}\\.", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex Proc_ProcNameRegex = new Regex(".*CREATE[ ]+PROCEDURE[ ]+[\\[]{0,1}[a-zA-Z0-9_ ]+[\\]]{0,1}\\.[\\[]{0,1}([a-zA-Z0-9_ ]+)[\\]]{0,1}", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex View_SchemaNameRegex = new Regex(".*CREATE[ ]+VIEW[ ]+[\\[]{0,1}([a-zA-Z0-9_ ]+)[\\]]{0,1}\\.", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex View_ViewNameRegex = new Regex(".*CREATE[ ]+VIEW[ ]+[\\[]{0,1}[a-zA-Z0-9_ ]+[\\]]{0,1}\\.[\\[]{0,1}([a-zA-Z0-9_ ]+)[\\]]{0,1}", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex Function_SchemaNameRegex = new Regex(".*CREATE[ ]+FUNCTION[ ]+[\\[]{0,1}([a-zA-Z0-9_ ]+)[\\]]{0,1}\\.", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        private static readonly Regex Function_FunctionNameRegex = new Regex(".*CREATE[ ]+FUNCTION[ ]+[\\[]{0,1}[a-zA-Z0-9_ ]+[\\]]{0,1}\\.[\\[]{0,1}([a-zA-Z0-9_ ]+)[\\]]{0,1}", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public SchemaScripterService(IApplicationLifetime appLifetime, ILogger<SchemaScripterService> logger, IOptions<AppConfig> appConfig, IDatabase database)
        {
            ApplicationLifetime = appLifetime;
            Logger = logger;
            Config = appConfig.Value;
            DB = database;
        }
        
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Logger.LogDebug("SchemaScripter started");
            
            var hasConfigError = false;
            if (string.IsNullOrWhiteSpace(Config.Server))
            {
                Logger.LogError($"Invalid {nameof(Config.Server)}");
                hasConfigError = true;
            }
            if (string.IsNullOrWhiteSpace(Config.User))
            {
                Logger.LogError($"Invalid {nameof(Config.User)}");
                hasConfigError = true;
            }
            if (string.IsNullOrWhiteSpace(Config.Password))
            {
                Logger.LogError($"Invalid {nameof(Config.Password)}");
                hasConfigError = true;
            }
            if (string.IsNullOrWhiteSpace(Config.Database))
            {
                Logger.LogError($"Invalid {nameof(Config.Database)}");
                hasConfigError = true;
            }
            if (string.IsNullOrWhiteSpace(Config.ExportFolder))
            {
                Logger.LogError($"Invalid {nameof(Config.ExportFolder)}");
                hasConfigError = true;
            }

            if (hasConfigError)
            {
                ApplicationLifetime.StopApplication();
                return;
            }
            
            var availability = await DB.CheckAvailabilityAsync();
            if (!availability.IsSuccess)
            {
                Logger.LogError(availability.ErrorMessage ?? "Error connecting to server");
                ApplicationLifetime.StopApplication();
                return;
            }
            if (!await DB.UseDatabaseAsync(Config.Database))
            {
                Logger.LogError($"Cannot connect to database {Config.Database}");
                ApplicationLifetime.StopApplication();
                return;
            }
            
            Profiler = MiniProfiler.StartNew(nameof(SchemaScripterService));

            using (Profiler.Step("Tables"))
            {
                await ExportTablesAsync(cancellationToken);
            }

            using (Profiler.Step("Stored Procedures"))
            {
                await ExportStoredProceduresAsync(cancellationToken);
            }
            
            using (Profiler.Step("Views"))
            {
                await ExportViewsAsync(cancellationToken);
            }
            
            using (Profiler.Step("Functions"))
            {
                await ExportFunctionsAsync(cancellationToken);
            }
            
            ExportViaSMO();

            await Profiler.StopAsync();
            Logger.LogDebug(Profiler.RenderPlainText());
            
            ApplicationLifetime.StopApplication();
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            DB.Dispose();
            return Task.CompletedTask;
        }
        
        private async Task ExportTablesAsync(CancellationToken cancellationToken)
        {
            try
            {
                var tables = await DB.SelectTablesAsync();
                if (tables == null)
                {
                    Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportTablesAsync)}: No tables found");
                    return;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportTablesAsync)}: {tables.Count} tables found. Exporting...");

                var numExported = 0;
                foreach (var table in tables)
                {
                    await File.WriteAllTextAsync(Path.Combine(Config.ExportFolder, "Tables", $"{table.SchemaName}.{table.TableName}.sql"), table.Definition, Encoding.UTF8, cancellationToken);
                    numExported++;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportTablesAsync)}: {numExported} tables exported");
            }
            catch (Exception e)
            {
                Logger.LogError(e, null, null);
                throw;
            }
        }

        private async Task ExportStoredProceduresAsync(CancellationToken cancellationToken)
        {
            try
            {
                var procs = await DB.SelectStoredProceduresAsync();
                if (procs == null)
                {
                    Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportStoredProceduresAsync)}: No stored procedures found");
                    return;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportStoredProceduresAsync)}: {procs.Count} stored procedures found. Exporting...");

                var sb = new StringBuilder();
                var numExported = 0;
                var lengthDiff = 0;
                foreach (var proc in procs)
                {
                    sb.Clear();
                    sb.Append(proc.Definition);

                    // Object definitions in sys.sql_modules can be out of date due to sp_rename or alter schema situations.
                    // So, check it and change it if needed
                    var match = Proc_SchemaNameRegex.Match(proc.Definition);
                    if (match.Success)
                    {
                        // Logger.LogDebug($"Schema regex matched: {match.Groups[1].Value}, {proc.SchemaName}, {match.Groups[1].Length}");
                        if (!match.Groups[1].Value.Equals(proc.SchemaName, StringComparison.OrdinalIgnoreCase))
                        {
                            var usesBrackets = proc.Definition[match.Groups[1].Index-1] == '[';
                            var index = match.Groups[1].Index - (usesBrackets ? 1 : 0);
                            var initialLength = sb.Length;
                            // Logger.LogDebug($"Removing: {sb.ToString(index, match.Groups[1].Length + (usesBrackets ? 2 : 0))}");
                            sb.Remove(index, match.Groups[1].Length + (usesBrackets ? 2 : 0));
                            // Logger.LogDebug($"Inserting: {(usesBrackets ? "[" : null)}{proc.SchemaName}]{(usesBrackets ? "]" : null)} between {sb.ToString(index - 5, 5)} and {sb.ToString(index, 5)}");
                            sb.Insert(index, $"{(usesBrackets ? "[" : null)}{proc.SchemaName}{(usesBrackets ? "]" : null)}");
                            lengthDiff = sb.Length - initialLength;
                        }
                    }
                    match = Proc_ProcNameRegex.Match(proc.Definition);
                    if (match.Success)
                    {
                        // Logger.LogDebug($"Name regex matched: {match.Groups[1].Value}, {proc.ProcedureName}, {match.Groups[1].Length}");
                        if (!match.Groups[1].Value.Equals(proc.ProcedureName, StringComparison.OrdinalIgnoreCase))
                        {
                            var usesBrackets = proc.Definition[match.Groups[1].Index-1] == '[';
                            var index = match.Groups[1].Index + lengthDiff - (usesBrackets ? 1 : 0);
                            //Logger.LogDebug($"Removing: {sb.ToString(index, match.Groups[1].Length + (usesBrackets ? 2 : 0))}");
                            sb.Remove(index, match.Groups[1].Length + (usesBrackets ? 2 : 0));
                            //Logger.LogDebug($"Inserting: {(usesBrackets ? "[" : null)}{proc.ProcedureName}]{(usesBrackets ? "]" : null)} between {sb.ToString(index - 5, 5)} and {sb.ToString(index, 5)}");
                            sb.Insert(index, $"{(usesBrackets ? "[" : null)}{proc.ProcedureName}{(usesBrackets ? "]" : null)}");
                        }
                    }

                    sb.Insert(0, $"SET QUOTED_IDENTIFIER {(proc.UsesQuotedIdentifier ? "ON" : "OFF")}\r\nGO\r\n");
                    sb.Insert(0, $"SET ANSI_NULLS {(proc.UsesAnsiNulls ? "ON" : "OFF")}\r\nGO\r\n");
                    
                    if (sb[sb.Length-1] != '\r' && sb[sb.Length-1] != '\n')
                        sb.Append("\r\n");
                    sb.Append("GO\r\n");

                    await File.WriteAllTextAsync(Path.Combine(Config.ExportFolder, "Stored Procedures", $"{proc.SchemaName}.{proc.ProcedureName}.sql"), sb.ToString(), Encoding.UTF8, cancellationToken);
                    numExported++;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportStoredProceduresAsync)}: {numExported} stored procedures exported");
            }
            catch (Exception e)
            {
                Logger.LogError(e, null, null);
                throw;
            }
        }
        
        private async Task ExportViewsAsync(CancellationToken cancellationToken)
        {
            try
            {
                var views = await DB.SelectViewsAsync();
                if (views == null)
                {
                    Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportViewsAsync)}: No views found");
                    return;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportViewsAsync)}: {views.Count} views found. Exporting...");

                var sb = new StringBuilder();
                var numExported = 0;
                var lengthDiff = 0;
                foreach (var view in views)
                {
                    sb.Clear();
                    sb.Append(view.Definition);

                    // Object definitions in sys.sql_modules can be out of date due to sp_rename or alter schema situations.
                    // So, check it and change it if needed
                    var match = View_SchemaNameRegex.Match(view.Definition);
                    if (match.Success)
                    {
                        if (!match.Groups[1].Value.Equals(view.SchemaName, StringComparison.OrdinalIgnoreCase))
                        {
                            var usesBrackets = view.Definition[match.Groups[1].Index-1] == '[';
                            var index = match.Groups[1].Index - (usesBrackets ? 1 : 0);
                            var initialLength = sb.Length;
                            sb.Remove(index, match.Groups[1].Length + (usesBrackets ? 2 : 0));
                            sb.Insert(index, $"{(usesBrackets ? "[" : null)}{view.SchemaName}{(usesBrackets ? "]" : null)}");
                            lengthDiff = sb.Length - initialLength;
                        }
                    }
                    match = View_ViewNameRegex.Match(view.Definition);
                    if (match.Success)
                    {
                        if (!match.Groups[1].Value.Equals(view.ViewName, StringComparison.OrdinalIgnoreCase))
                        {
                            var usesBrackets = view.Definition[match.Groups[1].Index-1] == '[';
                            var index = match.Groups[1].Index + lengthDiff - (usesBrackets ? 1 : 0);
                            sb.Remove(index, match.Groups[1].Length + (usesBrackets ? 2 : 0));
                            sb.Insert(index, $"{(usesBrackets ? "[" : null)}{view.ViewName}{(usesBrackets ? "]" : null)}");
                        }
                    }

                    sb.Insert(0, $"SET QUOTED_IDENTIFIER {(view.UsesQuotedIdentifier ? "ON" : "OFF")}\r\nGO\r\n");
                    sb.Insert(0, $"SET ANSI_NULLS {(view.UsesAnsiNulls ? "ON" : "OFF")}\r\nGO\r\n");
                    
                    if (sb[sb.Length-1] != '\r' && sb[sb.Length-1] != '\n')
                        sb.Append("\r\n");
                    sb.Append("GO\r\n");

                    await File.WriteAllTextAsync(Path.Combine(Config.ExportFolder, "Views", $"{view.SchemaName}.{view.ViewName}.sql"), sb.ToString(), Encoding.UTF8, cancellationToken);
                    numExported++;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportViewsAsync)}: {numExported} views exported");
            }
            catch (Exception e)
            {
                Logger.LogError(e, null, null);
                throw;
            }
        }
        
        private async Task ExportFunctionsAsync(CancellationToken cancellationToken)
        {
            try
            {
                var functions = await DB.SelectFunctionsAsync();
                if (functions == null)
                {
                    Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportFunctionsAsync)}: No functions found");
                    return;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportFunctionsAsync)}: {functions.Count} functions found. Exporting...");

                var sb = new StringBuilder();
                var numExported = 0;
                var lengthDiff = 0;
                foreach (var function in functions)
                {
                    sb.Clear();
                    sb.Append(function.Definition);

                    // Object definitions in sys.sql_modules can be out of date due to sp_rename or alter schema situations.
                    // So, check it and change it if needed
                    var match = Function_SchemaNameRegex.Match(function.Definition);
                    if (match.Success)
                    {
                        if (!match.Groups[1].Value.Equals(function.SchemaName, StringComparison.OrdinalIgnoreCase))
                        {
                            var usesBrackets = function.Definition[match.Groups[1].Index-1] == '[';
                            var index = match.Groups[1].Index - (usesBrackets ? 1 : 0);
                            var initialLength = sb.Length;
                            sb.Remove(index, match.Groups[1].Length + (usesBrackets ? 2 : 0));
                            sb.Insert(index, $"{(usesBrackets ? "[" : null)}{function.SchemaName}{(usesBrackets ? "]" : null)}");
                            lengthDiff = sb.Length - initialLength;
                        }
                    }
                    match = Function_FunctionNameRegex.Match(function.Definition);
                    if (match.Success)
                    {
                        if (!match.Groups[1].Value.Equals(function.FunctionName, StringComparison.OrdinalIgnoreCase))
                        {
                            var usesBrackets = function.Definition[match.Groups[1].Index-1] == '[';
                            var index = match.Groups[1].Index + lengthDiff - (usesBrackets ? 1 : 0);
                            sb.Remove(index, match.Groups[1].Length + (usesBrackets ? 2 : 0));
                            sb.Insert(index, $"{(usesBrackets ? "[" : null)}{function.FunctionName}{(usesBrackets ? "]" : null)}");
                        }
                    }

                    sb.Insert(0, $"SET QUOTED_IDENTIFIER {(function.UsesQuotedIdentifier ? "ON" : "OFF")}\r\nGO\r\n");
                    sb.Insert(0, $"SET ANSI_NULLS {(function.UsesAnsiNulls ? "ON" : "OFF")}\r\nGO\r\n");
                    
                    if (sb[sb.Length-1] != '\r' && sb[sb.Length-1] != '\n')
                        sb.Append("\r\n");
                    sb.Append("GO\r\n");

                    await File.WriteAllTextAsync(Path.Combine(Config.ExportFolder, "Functions", $"{function.SchemaName}.{function.FunctionName}.sql"), sb.ToString(), Encoding.UTF8, cancellationToken);
                    numExported++;
                }
                Logger.LogInformation($"{nameof(SchemaScripterService)}:{nameof(ExportFunctionsAsync)}: {numExported} functions exported");
            }
            catch (Exception e)
            {
                Logger.LogError(e, null, null);
                throw;
            }
        }

        private void ExportViaSMO()
        {
            try
            {
                var connection = new ServerConnection(Config.Server, Config.User, Config.Password);
                Logger.LogDebug($"Connecting to server {Config.Server}...");
                var server = new Server(connection);
                server.SetDefaultInitFields(true);
                Logger.LogDebug($"Getting database {Config.Database}...");
                var db = server.Databases[Config.Database];
                Logger.LogDebug("Building scripter...");
                var scripter = new Scripter(server)
                {
                    PrefetchObjects = true,
                    Options = new ScriptingOptions
                    {
                        AllowSystemObjects = false,
                        IncludeDatabaseContext = false,
                        IncludeIfNotExists = false,
                        ClusteredIndexes = true,
                        Default = true,
                        DriAll = true,
                        Indexes = true,
                        Triggers = true,
                        NonClusteredIndexes = true,
                        IncludeHeaders = false,
                        ToFileOnly = true,
                        AppendToFile = false,
                        ScriptDrops = false,
                        Encoding = Encoding.UTF8,
                        WithDependencies = false,
                        ScriptData = false,
                        ScriptSchema = true,
                        ChangeTracking = false,
                        EnforceScriptingOptions = true,
                    }
                };

                const string filenameFormat = "{0}.sql";
/*
                using (Profiler.Step("Schemas"))
                {
                    Logger.LogDebug("Fetching schemas...");
                    db.PrefetchObjects(typeof(Schema), scripter.Options);
                    var objects = db.Schemas;
                    Logger.LogDebug("Exporting schemas...");
                    var folder = Path.Combine(Config.ExportFolder, "Schemas");
                    Directory.CreateDirectory(folder);
                    foreach (Schema obj in objects)
                    {
                        if (obj.IsSystemObject)
                            continue;

                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, obj.Name));
                        scripter.Script(new[] { obj.Urn });
                    }
                }

                using (Profiler.Step("Tables"))
                {
                    Logger.LogDebug("Fetching tables...");
                    db.PrefetchObjects(typeof(Table), scripter.Options);
                    var objects = db.Tables;
                    Logger.LogDebug("Exporting tables...");
                    var folder = Path.Combine(Config.ExportFolder, "Tables");
                    Directory.CreateDirectory(folder);
                    var triggerFolder = Path.Combine(folder, "Triggers");
                    Directory.CreateDirectory(triggerFolder);
                    foreach (Table obj in objects)
                    {
                        if (obj.IsSystemObject)
                            continue;

                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, $"{obj.Schema}.{obj.Name}"));
                        scripter.Script(new[] { obj.Urn });

                        if (obj.Triggers == null)
                            continue;

                        foreach (Trigger trigger in obj.Triggers)
                        {
                            if (trigger.IsSystemObject)
                                continue;

                            scripter.Options.FileName = Path.Combine(triggerFolder, string.Format(filenameFormat, trigger.Name));
                            scripter.Script(new[] { trigger.Urn });
                        }
                    }
                }

                using (Profiler.Step("StoredProcedures"))
                {
                    Logger.LogDebug("Fetching stored procedures...");
                    db.PrefetchObjects(typeof(StoredProcedure), scripter.Options);
                    var objects = db.StoredProcedures;
                    Logger.LogDebug("Exporting stored procedures...");
                    var folder = Path.Combine(Config.ExportFolder, "Stored Procedures");
                    Directory.CreateDirectory(folder);
                    foreach (StoredProcedure obj in objects)
                    {
                        if (obj.IsSystemObject)
                            continue;

                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, $"{obj.Schema}.{obj.Name}"));
                        scripter.Script(new[] { obj.Urn });
                    }
                }

                using (Profiler.Step("Views"))
                {
                    Logger.LogDebug("Fetching views...");
                    db.PrefetchObjects(typeof(View), scripter.Options);
                    var objects = db.Views;
                    Logger.LogDebug("Exporting views...");
                    var folder = Path.Combine(Config.ExportFolder, "Views");
                    Directory.CreateDirectory(folder);
                    foreach (View obj in objects)
                    {
                        if (obj.IsSystemObject)
                            continue;

                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, $"{obj.Schema}.{obj.Name}"));
                        scripter.Script(new[] { obj.Urn });
                    }
                }

                using (Profiler.Step("UserDefinedFunctions"))
                {
                    Logger.LogDebug("Fetching user-defined functions...");
                    db.PrefetchObjects(typeof(UserDefinedFunction), scripter.Options);
                    var objects = db.UserDefinedFunctions;
                    Logger.LogDebug("Exporting user-defined functions...");
                    var folder = Path.Combine(Config.ExportFolder, "Functions");
                    Directory.CreateDirectory(folder);
                    foreach (UserDefinedFunction obj in objects)
                    {
                        if (obj.IsSystemObject)
                            continue;

                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, $"{obj.Schema}.{obj.Name}"));
                        scripter.Script(new[] { obj.Urn });
                    }
                }
*/
                using (Profiler.Step("UserDefinedTypes"))
                {
                    Logger.LogDebug("Fetching user-defined types...");
                    db.PrefetchObjects(typeof(UserDefinedType), scripter.Options);
                    var objects = db.UserDefinedTypes;
                    Logger.LogDebug("Exporting user-defined types...");
                    var folder = Path.Combine(Config.ExportFolder, "Types");
                    Directory.CreateDirectory(folder);
                    foreach (UserDefinedType obj in objects)
                    {
                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, $"{obj.Schema}.{obj.Name}"));
                        scripter.Script(new[] { obj.Urn });
                    }
                }

                using (Profiler.Step("UserDefinedDataTypes"))
                {
                    Logger.LogDebug("Fetching user-defined data types...");
                    db.PrefetchObjects(typeof(UserDefinedDataType), scripter.Options);
                    var objects = db.UserDefinedDataTypes;
                    Logger.LogDebug("Exporting user-defined data types...");
                    var folder = Path.Combine(Config.ExportFolder, "Data Types");
                    Directory.CreateDirectory(folder);
                    foreach (UserDefinedDataType obj in objects)
                    {
                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, $"{obj.Schema}.{obj.Name}"));
                        scripter.Script(new[] { obj.Urn });
                    }
                }

                using (Profiler.Step("UserDefinedTableTypes"))
                {
                    Logger.LogDebug("Fetching user-defined table types...");
                    db.PrefetchObjects(typeof(UserDefinedTableType), scripter.Options);
                    var objects = db.UserDefinedTableTypes;
                    Logger.LogDebug("Exporting user-defined table types...");
                    var folder = Path.Combine(Config.ExportFolder, "Table Types");
                    Directory.CreateDirectory(folder);
                    foreach (UserDefinedTableType obj in objects)
                    {
                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, $"{obj.Schema}.{obj.Name}"));
                        scripter.Script(new[] { obj.Urn });
                    }
                }

                using (Profiler.Step("Triggers"))
                {
                    Logger.LogDebug("Fetching triggers...");
                    var objects = db.Triggers;
                    Logger.LogDebug("Exporting triggers...");
                    var folder = Path.Combine(Config.ExportFolder, "Triggers");
                    Directory.CreateDirectory(folder);
                    foreach (Trigger obj in objects)
                    {
                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, obj.Name));
                        scripter.Script(new[] { obj.Urn });
                    }
                }

                using (Profiler.Step("Jobs"))
                {
                    Logger.LogDebug("Fetching jobs...");
                    var objects = server.JobServer.Jobs;
                    Logger.LogDebug("Exporting jobs...");
                    var folder = Path.Combine(Config.ExportFolder, "Jobs");
                    Directory.CreateDirectory(folder);
                    foreach (Job obj in objects)
                    {
                        scripter.Options.FileName = Path.Combine(folder, string.Format(filenameFormat, obj.Name));
                        scripter.Script(new[] { obj.Urn });
                    }
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, null, null);
                throw;
            }
        }
    }
}