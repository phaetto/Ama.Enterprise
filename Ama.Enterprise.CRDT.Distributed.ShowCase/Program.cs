namespace Ama.Enterprise.CRDT.Distributed.ShowCase;

using System;
using System.Diagnostics;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Services;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Telemetry.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Entry point for demonstrating Multiple Distributed CRDTs orchestrated via a global registry.
/// </summary>
public static class Program
{
    private static readonly object ConsoleLock = new();
    private static int currentPort;
    private static int currentAdminPort;

    public static async Task Main(string[] args)
    {
        if (args.Length == 0 || !int.TryParse(args[0], out currentPort))
        {
            currentPort = GetNextAvailablePort(8100);
        }

        var currentHandshakePort = GetNextAvailablePort(8037);
        var currentAdminHandshakePort = GetNextAvailablePort(currentHandshakePort + 1);
        currentAdminPort = GetNextAvailablePort(currentPort + 1);
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
        
        // Add core CRDT distributed services and resolve the orchestrator
        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.PeerEvictionTtlSeconds = 60;
            options.CheckpointIntervalSeconds = 60;
            options.AntiEntropyInitialDelaySeconds = 1;
            options.AntiEntropyIntervalSeconds = 5;
        });

        services.AddDistributedCrdtReplica(replicaId);

        // Register the showcase JSON AOT and CRDT AOT contexts directly into the generic CRDT pipeline
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(ShowCaseJsonContext.Default)
                .AddCrdtAotContext(new ShowCaseCrdtAotContext())
                .AddCrdtSerializableType<TaskItem>("task-item")
                .AddCrdtSerializableType<DeviceStatus>("device-status")
                .AddCrdtSystemTextJson(useBrotliCompression: true);

        // Register document types into the orchestrator and expose generic interfaces via explicit transparent forwarders
        services.AddDistributedDocumentType<TaskListState>("task-list");
        services.AddDistributedCrdtService<ITaskManager, TaskManager>();

        services.AddDistributedDocumentType<FleetState>("fleet-list");
        services.AddDistributedCrdtService<IFleetManager, FleetManager>();

        // Register Showcase file-based CRDT storage overriding memory fallbacks for all documents
        services.AddDistributedCrdtStorage<ShowCaseCrdtStorage>();

        // Register the background multi-document orchestration and route inbound intents from the network
        services.AddDistributedCrdtP2p("internal", replicaId);

        // Network Layer bindings
        services
            .AddP2pMesh("internal")
            .AddGossipNetwork(options =>
            {
                options.GossipInterval = TimeSpan.FromMilliseconds(500);
            })
            .AddTcpTransport(options =>
            {
                options.ListenPort = currentPort;
                options.ListenHost = "127.0.0.1";
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = "239.255.0.2";
                options.MulticastPort = 8036;
                options.DiscoveryInterval = TimeSpan.FromSeconds(1);
                options.DiscoveryTimeout = TimeSpan.FromSeconds(1);
            })
            .AddUdpPeerHandshake(options =>
            {
                options.ListenPort = currentHandshakePort;
            })
            .ConfigureFailureDetector(options =>
            {
                options.HeartbeatInterval = TimeSpan.FromSeconds(5);
            });

        services
            .AddP2pTelemetryForwarder(options =>
            {
                options.TargetMeshId = "admin";
                options.FlushInterval = TimeSpan.FromSeconds(1);
            })
            .AddP2pMesh("admin")
            .AddTcpTransport(options =>
            {
                options.ListenPort = currentAdminPort;
                options.ListenHost = "127.0.0.1";
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = "239.255.0.3";
                options.MulticastPort = 8036;
                options.DiscoveryInterval = TimeSpan.FromSeconds(1);
                options.DiscoveryTimeout = TimeSpan.FromSeconds(1);
            })
            .AddUdpPeerHandshake(options =>
            {
                options.ListenPort = currentAdminHandshakePort;
            })
            .ConfigureFailureDetector(options =>
            {
                options.HeartbeatInterval = TimeSpan.FromSeconds(5);
            });

        await using var provider = services.BuildServiceProvider();
        var logger = provider.GetRequiredService<ILoggerFactory>().CreateLogger("ShowCase");
        
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            logger.LogInformation("Starting Dynamic Multi-CRDT orchestrated node on HTTP port {Port} and UDP Handshake port {HandshakePort}...", currentPort, currentHandshakePort);

            foreach (var service in hostedServices)
            {
                await service.StartAsync(cts.Token).ConfigureAwait(false);
            }

            var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);

            var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            var taskManager = scope.ServiceProvider.GetRequiredService<ITaskManager>();
            var fleetManager = scope.ServiceProvider.GetRequiredService<IFleetManager>();

            // UI refresh bindings
            taskManager.StateChanged += (sender, eventArgs) => DrawState(orchestrator, taskManager, fleetManager);
            fleetManager.StateChanged += (sender, eventArgs) => DrawState(orchestrator, taskManager, fleetManager);

#if DEBUG
            if (Debugger.IsAttached)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        // Ensure the document exists first to avoid invalid mutations
                        await orchestrator.CreateDocumentAsync("bbb", "task-list", cts.Token).ConfigureAwait(false);
                        logger.LogInformation("Debug mode detected. Pumping 150 changes/sec to task_001...");

                        // Batching 15 requests every 100ms yields 150 requests/sec reliably.
                        using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
                        while (await timer.WaitForNextTickAsync(cts.Token).ConfigureAwait(false))
                        {
                            for (var i = 0; i < 15; i++)
                            {
                                await taskManager.SetTaskAsync("bbb", "task_001", "TaskBased", false, cts.Token).ConfigureAwait(false);
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        // Expected during graceful shutdown
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Load generator failed.");
                    }
                }, cts.Token);
            }
#endif

            DrawMenu();
            DrawState(orchestrator, taskManager, fleetManager);

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
                        case "new-list":
                            if (parts.Length >= 2)
                                await orchestrator.CreateDocumentAsync(parts[1], "task-list", cts.Token).ConfigureAwait(false);
                            else
                                WriteLineLocked("Usage: new-list <docId>");
                            break;

                        case "new-fleet":
                            if (parts.Length >= 2)
                                await orchestrator.CreateDocumentAsync(parts[1], "fleet-list", cts.Token).ConfigureAwait(false);
                            else
                                WriteLineLocked("Usage: new-fleet <docId>");
                            break;

                        case "del-doc":
                            if (parts.Length >= 2)
                                await orchestrator.DeleteDocumentAsync(parts[1], cts.Token).ConfigureAwait(false);
                            else
                                WriteLineLocked("Usage: del-doc <docId>");
                            break;

                        case "tset":
                            if (parts.Length >= 5 && bool.TryParse(parts[4], out var isDone))
                                await taskManager.SetTaskAsync(parts[1], parts[2], parts[3], isDone, cts.Token).ConfigureAwait(false);
                            else
                                WriteLineLocked("Usage: tset <docId> <taskId> <desc_without_spaces> <true|false>");
                            break;

                        case "tdel":
                            if (parts.Length >= 3)
                                await taskManager.RemoveTaskAsync(parts[1], parts[2], cts.Token).ConfigureAwait(false);
                            else
                                WriteLineLocked("Usage: tdel <docId> <taskId>");
                            break;

                        case "fset":
                            if (parts.Length >= 5 && bool.TryParse(parts[3], out var isOnline) && int.TryParse(parts[4], out var battery))
                                await fleetManager.SetDeviceAsync(parts[1], parts[2], isOnline, battery, cts.Token).ConfigureAwait(false);
                            else
                                WriteLineLocked("Usage: fset <docId> <deviceId> <true|false> <battery_int>");
                            break;

                        case "fdel":
                            if (parts.Length >= 3)
                                await fleetManager.RemoveDeviceAsync(parts[1], parts[2], cts.Token).ConfigureAwait(false);
                            else
                                WriteLineLocked("Usage: fdel <docId> <deviceId>");
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
            logger.LogInformation("Shutting down orchestrated services...");
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private static void CloneProcess(ILogger logger)
    {
        var nextPort = GetNextAvailablePort(currentPort + 1);
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

        logger.LogInformation("Cloned new cluster node on port {NextPort}.", nextPort);
    }

    private static void DrawMenu()
    {
        lock (ConsoleLock)
        {
            Console.WriteLine("=================================================");
            Console.WriteLine($" Multi-CRDT Peer Node - Listening on Port {currentPort}");
            Console.WriteLine("=================================================");
            Console.WriteLine("Commands:");
            Console.WriteLine(" new-list <docId>                               - Creates a new Task List document");
            Console.WriteLine(" new-fleet <docId>                              - Creates a new Fleet List document");
            Console.WriteLine(" del-doc <docId>                                - Tombstones and removes an active document");
            Console.WriteLine(" tset <docId> <taskId> <desc> <true|false>      - Adds/Updates a task item");
            Console.WriteLine(" tdel <docId> <taskId>                          - Removes a task item");
            Console.WriteLine(" fset <docId> <deviceId> <true|false> <batt>    - Adds/Updates a fleet device");
            Console.WriteLine(" fdel <docId> <deviceId>                        - Removes a fleet device");
            Console.WriteLine(" clone                                          - Spawns a new node process");
            Console.WriteLine(" exit                                           - Shuts down the node");
            Console.WriteLine("=================================================\n");
        }
    }

    private static void DrawState(ICrdtDocumentOrchestrator orchestrator, ITaskManager taskManager, IFleetManager fleetManager)
    {
        lock (ConsoleLock)
        {
            Console.WriteLine("\n--- [Cluster Registry Synchronized] ---");
            
            Console.WriteLine(" [Tasks CRDT State]");
            var taskDocs = orchestrator.GetActiveDocuments().OfType<IDistributedCrdtDocument<TaskListState>>().ToList();
            if (taskDocs.Count == 0)
            {
                Console.WriteLine("   (No task lists currently defined)");
            }
            else
            {
                foreach (var doc in taskDocs)
                {
                    Console.WriteLine($"   => List [{doc.DocumentId}]:");
                    var tasks = taskManager.GetTasks(doc.DocumentId);
                    if (tasks.Count == 0)
                    {
                        Console.WriteLine("      (Empty)");
                    }
                    else
                    {
                        foreach (var task in tasks.OrderBy(t => t.Key))
                        {
                            Console.WriteLine($"      [{task.Key}]: {task.Value.Description} (Done: {task.Value.IsDone})");
                        }
                    }
                }
            }

            Console.WriteLine("\n [Fleet CRDT State]");
            var fleetDocs = orchestrator.GetActiveDocuments().OfType<IDistributedCrdtDocument<FleetState>>().ToList();
            if (fleetDocs.Count == 0)
            {
                Console.WriteLine("   (No fleets currently defined)");
            }
            else
            {
                foreach (var doc in fleetDocs)
                {
                    Console.WriteLine($"   => Fleet [{doc.DocumentId}]:");
                    var devices = fleetManager.GetDevices(doc.DocumentId);
                    if (devices.Count == 0)
                    {
                        Console.WriteLine("      (Empty)");
                    }
                    else
                    {
                        foreach (var device in devices.OrderBy(d => d.Key))
                        {
                            Console.WriteLine($"      [{device.Key}]: Online: {device.Value.IsOnline}, Battery: {device.Value.BatteryLevel}%");
                        }
                    }
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

    private static int GetNextAvailablePort(int startingPort)
    {
        var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
        
        var activeTcpPorts = ipGlobalProperties.GetActiveTcpListeners().Select(l => l.Port);
        var activeUdpPorts = ipGlobalProperties.GetActiveUdpListeners().Select(l => l.Port);

        var activePorts = activeTcpPorts.Concat(activeUdpPorts).ToHashSet();

        var port = startingPort;
        while (activePorts.Contains(port))
        {
            port++;
        }

        return port;
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