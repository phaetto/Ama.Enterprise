namespace Ama.Enterprise.CRDT.Distributed.ShowCase;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Services;
using Ama.Enterprise.P2p.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Entry point for demonstrating Multiple Distributed CRDTs actively synchronizing over a single P2P Gossip mesh.
/// </summary>
/// <example>
/// tset 1 device1 true
/// fset 1 true 100
/// </example>
public static class Program
{
    private static readonly object ConsoleLock = new();
    private static int currentPort;

    public static async Task Main(string[] args)
    {
        if (args.Length == 0 || !int.TryParse(args[0], out currentPort))
        {
            currentPort = 8080 + Random.Shared.Next(0, 1000);
        }

        var replicaId = $"node-{currentPort}";
        var services = new ServiceCollection();

        services.AddLogging(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Information);
            builder.AddFilter("Microsoft", LogLevel.Warning);
            builder.AddFilter("System", LogLevel.Warning);
            builder.ClearProviders();
            builder.AddProvider(new LockedConsoleLoggerProvider());
        });

        // Add core CRDT distributed services and scope
        services.AddDistributedCrdtCore(options =>
        {
            options.ReplicaId = replicaId;
            options.ActiveSyncEnabled = true;
        });

        // Register the showcase JSON AOT and CRDT AOT contexts directly into the generic CRDT pipeline
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(ShowCaseJsonContext.Default)
                .AddCrdtAotContext(new ShowCaseCrdtAotContext())
                .AddCrdtSerializableType<TaskItem>("task-item")
                .AddCrdtSerializableType<DeviceStatus>("device-status");

        // Document 1: Task List mapping
        services.AddDistributedDocument<TaskListState>("task-list-doc");
        services.AddScoped<ITaskManager, TaskManager>();

        // Document 2: Fleet Status mapping
        services.AddDistributedDocument<FleetState>("fleet-doc");
        services.AddScoped<IFleetManager, FleetManager>();

        // Register the background multi-document orchestration and route inbound intents from the network
        services.AddDistributedCrdtP2p("internal");

        // Network Layer bindings
        services
            .AddP2pMesh("internal")
            .AddGossipNetwork(options =>
            {
                options.ListenPort = currentPort;
                options.ListenHost = "localhost";
                options.GossipInterval = TimeSpan.FromMilliseconds(500);
            }).AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = "239.255.0.2"; // Separate multicast channel to avoid feature flags showcase collisions
                options.MulticastPort = 8036;
                options.DiscoveryInterval = TimeSpan.FromSeconds(1);
                options.DiscoveryTimeout = TimeSpan.FromSeconds(1);
            });

        await using var provider = services.BuildServiceProvider();
        var logger = provider.GetRequiredService<ILoggerFactory>().CreateLogger("ShowCase");
        
        var scopeProvider = provider.GetRequiredService<DistributedCrdtScopeProvider>();
        var taskManager = scopeProvider.Scope.ServiceProvider.GetRequiredService<ITaskManager>();
        var fleetManager = scopeProvider.Scope.ServiceProvider.GetRequiredService<IFleetManager>();
        
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        // UI refresh triggers bounded to specific documents structural change notifications
        taskManager.StateChanged += (sender, eventArgs) => DrawState(taskManager.GetTasks(), fleetManager.GetDevices());
        fleetManager.StateChanged += (sender, eventArgs) => DrawState(taskManager.GetTasks(), fleetManager.GetDevices());

        try
        {
            logger.LogInformation("Starting Multi-CRDT showcase node on port {Port}...", currentPort);

            foreach (var service in hostedServices)
            {
                await service.StartAsync(cts.Token).ConfigureAwait(false);
            }

            DrawMenu();
            DrawState(taskManager.GetTasks(), fleetManager.GetDevices());

            while (!cts.Token.IsCancellationRequested)
            {
                var input = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(input))
                {
                    continue;
                }

                var parts = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var command = parts[0].ToLowerInvariant();

                try
                {
                    switch (command)
                    {
                        case "tset":
                            if (parts.Length >= 4 && bool.TryParse(parts[3], out var isDone))
                            {
                                await taskManager.SetTaskAsync(parts[1], parts[2], isDone, cts.Token).ConfigureAwait(false);
                            }
                            else
                            {
                                WriteLineLocked("Usage: tset <id> <desc_without_spaces> <true|false>");
                            }
                            break;

                        case "tdel":
                            if (parts.Length >= 2)
                            {
                                await taskManager.RemoveTaskAsync(parts[1], cts.Token).ConfigureAwait(false);
                            }
                            else
                            {
                                WriteLineLocked("Usage: tdel <id>");
                            }
                            break;

                        case "fset":
                            if (parts.Length >= 4 && bool.TryParse(parts[2], out var isOnline) && int.TryParse(parts[3], out var battery))
                            {
                                await fleetManager.SetDeviceAsync(parts[1], isOnline, battery, cts.Token).ConfigureAwait(false);
                            }
                            else
                            {
                                WriteLineLocked("Usage: fset <id> <true|false> <battery_int>");
                            }
                            break;

                        case "fdel":
                            if (parts.Length >= 2)
                            {
                                await fleetManager.RemoveDeviceAsync(parts[1], cts.Token).ConfigureAwait(false);
                            }
                            else
                            {
                                WriteLineLocked("Usage: fdel <id>");
                            }
                            break;

                        case "clone":
                            CloneProcess(logger);
                            break;

                        case "exit":
                            cts.Cancel();
                            break;

                        default:
                            WriteLineLocked("Unknown command.");
                            DrawMenu();
                            break;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing command.");
                }
            }
        }
        finally
        {
            logger.LogInformation("Shutting down services. Please wait...");
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private static void CloneProcess(ILogger logger)
    {
        var nextPort = 8080 + Random.Shared.Next(0, 1000);
        var processPath = Environment.ProcessPath;

        if (string.IsNullOrEmpty(processPath))
        {
            logger.LogWarning("Unable to determine process path for cloning.");
            return;
        }

        Process.Start(new ProcessStartInfo
        {
            FileName = processPath,
            Arguments = nextPort.ToString(),
            UseShellExecute = true
        });

        logger.LogInformation("Cloned new cluster node instance on port {NextPort}.", nextPort);
    }

    private static void DrawMenu()
    {
        lock (ConsoleLock)
        {
            Console.WriteLine("=================================================");
            Console.WriteLine($" Multi-CRDT Peer Node - Listening on Port {currentPort}");
            Console.WriteLine("=================================================");
            Console.WriteLine("Commands:");
            Console.WriteLine(" tset <id> <desc> <true|false> - Adds or Updates a Task item");
            Console.WriteLine(" tdel <id>                     - Removes a Task item");
            Console.WriteLine(" fset <id> <true|false> <batt> - Adds or Updates a Device status");
            Console.WriteLine(" fdel <id>                     - Removes a Device status");
            Console.WriteLine(" clone                         - Spawns a new node in a new window");
            Console.WriteLine(" exit                          - Shuts down node gracefully");
            Console.WriteLine("=================================================\n");
        }
    }

    private static void DrawState(IReadOnlyDictionary<string, TaskItem> tasks, IReadOnlyDictionary<string, DeviceStatus> devices)
    {
        lock (ConsoleLock)
        {
            Console.WriteLine("\n--- [Cluster State Synchronized] ---");
            
            Console.WriteLine(" [Tasks CRDT State]");
            if (tasks.Count == 0)
            {
                Console.WriteLine("   (No tasks currently defined)");
            }
            else
            {
                foreach (var task in tasks.OrderBy(t => t.Key))
                {
                    Console.WriteLine($"   => [{task.Key}]: {task.Value.Description} (Done: {task.Value.IsDone})");
                }
            }

            Console.WriteLine("\n [Fleet CRDT State]");
            if (devices.Count == 0)
            {
                Console.WriteLine("   (No devices currently defined)");
            }
            else
            {
                foreach (var device in devices.OrderBy(d => d.Key))
                {
                    Console.WriteLine($"   => [{device.Key}]: Online: {device.Value.IsOnline}, Battery: {device.Value.BatteryLevel}%");
                }
            }

            Console.WriteLine("------------------------------------\n> ");
        }
    }

    private static void WriteLineLocked(string message)
    {
        lock (ConsoleLock)
        {
            Console.WriteLine(message);
        }
    }

    private sealed class LockedConsoleLoggerProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new LockedConsoleLogger(categoryName);
        public void Dispose() { }
    }

    private sealed class LockedConsoleLogger(string categoryName) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            var message = formatter(state, exception);
            if (string.IsNullOrEmpty(message) && exception is null)
            {
                return;
            }

            var shortCategory = categoryName.Split('.').LastOrDefault() ?? categoryName;
            var logLine = $"[{logLevel.ToString().ToUpperInvariant()}] {shortCategory}: {message}";
            
            if (exception is not null)
            {
                logLine += Environment.NewLine + exception;
            }

            WriteLineLocked(logLine);
        }
    }
}