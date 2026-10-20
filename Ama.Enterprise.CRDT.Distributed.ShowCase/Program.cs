namespace Ama.Enterprise.CRDT.Distributed.ShowCase;

using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Services;
using Ama.Enterprise.CRDT.MessagePack.Extensions;
using Ama.Enterprise.Licensing.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Telemetry.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.NetworkInformation;
using System.Runtime;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Entry point for demonstrating Multiple Distributed CRDTs orchestrated via a global registry.
/// </summary>
public static class Program
{
    private static readonly object ConsoleLock = new();
    private static readonly string TableSeparatorLine = new('-', 154);
    private static readonly List<DocumentMemoryStatsDto> ReusableMemoryStatsList = new(32);
    private static readonly Dictionary<string, long> ReusableShardDvvBytesCache = new(StringComparer.Ordinal);
    private static readonly Comparison<DocumentMemoryStatsDto> MemoryStatsComparison = CompareMemoryStats;

    private static int currentPort;
    private static int currentAdminPort;
    private static volatile bool _needsRedraw;

    // DTO for representing the state output instead of using tuples
    private readonly record struct DocumentStateDto<T>(string Id, IReadOnlyDictionary<string, T> Items);

    private readonly record struct DocumentMemoryStatsDto(
        string Id,
        string ShardId,
        string DocumentType,
        long DataSizeBytes,
        long MetadataSizeBytes,
        long StateSizeBytes,
        long WholeObjectSizeBytes,
        long GlobalStateSizeBytes);

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

        // Register licensing
        services.ConfigureAmaCommunityLicense();

        // Map central UI generic redraw observer tracking natively distributed states explicitly
        services.AddSingleton<ConsoleRedrawObserver>();
        services.AddSingleton<IDocumentStateObserver<TaskListState>>(sp => sp.GetRequiredService<ConsoleRedrawObserver>());
        services.AddSingleton<IDocumentStateObserver<FleetState>>(sp => sp.GetRequiredService<ConsoleRedrawObserver>());
        services.AddSingleton<ICrdtOrchestratorObserver>(sp => sp.GetRequiredService<ConsoleRedrawObserver>());

        // Add core CRDT distributed services and resolve the orchestrator
        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.PeerEvictionTtlSeconds = 0;
            options.AntiEntropyInitialDelaySeconds = 0;
            options.AntiEntropyIntervalSeconds = 5;
            options.CheckpointIntervalSeconds = 3;
            options.MaintenanceIntervalSeconds = 5 * 60;
            options.CompactionTtlSeconds = 5 * 60;
            options.ShardReplicationFactor = 2;
            options.EnableSharding = true;
        });

        services.AddDistributedCrdtReplica(replicaId);

        // Register the showcase JSON AOT and CRDT AOT contexts directly into the generic CRDT pipeline
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(ShowCaseJsonContext.Default)
                .AddCrdtAotContext(new ShowCaseCrdtAotContext())
                .AddCrdtSerializableType<TaskItem>("task-item")
                .AddCrdtSerializableType<DeviceStatus>("device-status")
                .AddCrdtSystemTextJson(useBrotliCompression: true)
                .AddCrdtShardIdProvider<ShowCaseShardIdProvider>();

        services.AddCrdtMessagePack(
            MessagePack.Resolvers.Ama_Enterprise_CRDT_MessagePack_MessagePackResolver.Instance,
            MessagePack.Resolvers.Ama_Enterprise_CRDT_Distributed_ShowCase_MessagePackResolver.Instance
        );

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
            .AddP2pMesh("internal", options =>
            {
                options.MinActivePeers = 10;
            })
            .AddGossipNetwork(options =>
            {
                options.GossipInterval = TimeSpan.FromMilliseconds(500);
            })
            .AddTcpTransport(options =>
            {
                options.ListenPort = currentPort;
                options.ListenHost = "127.0.0.1";
                options.MaxMessageSize = int.MaxValue;
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
                options.HeartbeatInterval = TimeSpan.FromSeconds(30);
            });

        services
            .AddP2pTelemetryForwarder(options =>
            {
                options.TargetMeshId = "admin";
                options.FlushInterval = TimeSpan.FromSeconds(1);
                options.IncludedMeterNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                {
                    string.Empty // Get ALL
                };
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
                options.HeartbeatInterval = TimeSpan.FromSeconds(30);
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

            // Background UI rendering task to decouple Console I/O from hot CRDT operations
            _ = Task.Run(async () =>
            {
                try
                {
                    using var uiTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
                    while (await uiTimer.WaitForNextTickAsync(cts.Token).ConfigureAwait(false))
                    {
                        if (_needsRedraw)
                        {
                            _needsRedraw = false;
                            //await DrawStateAsync(replicaId, orchestrator, taskManager, fleetManager, cts.Token).ConfigureAwait(false);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Clean shutdown
                }
            }, cts.Token);

            CancellationTokenSource? hammerCts = null;
            Task? hammerTask = null;

            DrawMenu();
            await DrawStateAsync(replicaId, orchestrator, taskManager, fleetManager, cts.Token).ConfigureAwait(false);

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

                        case "gen-fleets":
                            if (parts.Length >= 3 && int.TryParse(parts[1], out var fleetCount) && int.TryParse(parts[2], out var devicesPerFleet))
                            {
                                logger.LogInformation("Generating {FleetCount} fleets with {DevicesPerFleet} devices each...", fleetCount, devicesPerFleet);
                                for (int i = 1; i <= fleetCount; i++)
                                {
                                    if (cts.Token.IsCancellationRequested) break;
                                    var docId = $"auto-fleet-{i}";
                                    await orchestrator.CreateDocumentAsync(docId, "fleet-list", cts.Token).ConfigureAwait(false);
                                    
                                    for (int j = 1; j <= devicesPerFleet; j++)
                                    {
                                        if (cts.Token.IsCancellationRequested) break;
                                        var deviceId = $"dev-{j}";
                                        var isDevOnline = Random.Shared.NextDouble() > 0.5;
                                        var deviceBattery = Random.Shared.Next(0, 101);
                                        await fleetManager.SetDeviceAsync(docId, deviceId, isDevOnline, deviceBattery, IntentDeliveryMode.FireAndForget, cts.Token).ConfigureAwait(false);
                                    }
                                }
                                logger.LogInformation("Finished generating fleets.");
                            }
                            else
                            {
                                WriteLineLocked("Usage: gen-fleets <numberOfFleets> <devicesPerFleet>");
                            }
                            break;

                        case "hammer":
                            if (parts.Length >= 2 && int.TryParse(parts[1], out var cps))
                            {
                                if (hammerCts is not null)
                                {
                                    logger.LogInformation("Stopping hammer mode... Please wait, draining pending operations (this may take a moment)...");
                                    await hammerCts.CancelAsync().ConfigureAwait(false);
                                    
                                    if (hammerTask is not null)
                                    {
                                        try
                                        {
                                            await hammerTask.ConfigureAwait(false);
                                        }
                                        catch
                                        {
                                            // Ignore expected task cancellation exceptions during unwinding
                                        }
                                    }
                                    
                                    hammerCts.Dispose();
                                    hammerCts = null;
                                    hammerTask = null;
                                    logger.LogInformation("Hammer mode stopped successfully.");
                                }

                                if (cps > 0)
                                {
                                    hammerCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                                    var token = hammerCts.Token;

                                    hammerTask = Task.Run(async () =>
                                    {
                                        try
                                        {
                                            // Distribute traffic across 5 distinct documents ensuring parallel load on multiple distributed shards
                                            var targetDocs = Enumerable.Range(1, 5).Select(i => $"nail-{i}").ToArray();
                                            foreach (var doc in targetDocs)
                                            {
                                                await orchestrator.CreateDocumentAsync(doc, "task-list", token).ConfigureAwait(false);
                                            }

                                            logger.LogInformation("Hammer mode activated. Target: {Cps} ops/sec sharded across {DocCount} nail documents...", cps, targetDocs.Length);

                                            var sw = Stopwatch.StartNew();
                                            var reportSw = Stopwatch.StartNew();
                                            long opsCompleted = 0;
                                            long lastReportedOps = 0;

                                            // Bounded pool of 20 keys: keeping ~80% alive ensures roughly 16 active items, satisfying the 10-20 active items boundary.
                                            var taskIds = Enumerable.Range(1, 20).Select(i => $"nail-task-{i}").ToArray();

                                            // Helper for concurrent dispatch avoiding closure captures or ValueTask casting ambiguities
                                            async Task FirePayloadAsync(string dId, string tId, long index, bool done, bool delete, CancellationToken ct)
                                            {
                                                if (ct.IsCancellationRequested) return;

                                                try
                                                {
                                                    // Do not use cancellation tokens here, because we will break the process in memory/journal
                                                    if (delete)
                                                    {
                                                        await taskManager.RemoveTaskAsync(dId, tId).ConfigureAwait(false);
                                                    }
                                                    else
                                                    {
                                                        await taskManager.SetTaskAsync(dId, tId, $"Hammered payload {index}", done).ConfigureAwait(false);
                                                    }
                                                }
                                                // Suppress cancellation exceptions to avoid massive unwinding delays when stopping heavy loads
                                                catch (TaskCanceledException) { }
                                                catch (OperationCanceledException) { }
                                            }

                                            while (!token.IsCancellationRequested)
                                            {
                                                var targetOps = (long)(sw.Elapsed.TotalSeconds * cps);
                                                var batch = targetOps - opsCompleted;

                                                if (batch > 0)
                                                {
                                                    // Increased cap explicitly permitting high-throughput bursts natively catching up safely
                                                    if (batch > 5000)
                                                    {
                                                        batch = 5000;
                                                    }

                                                    var pendingTasks = new List<Task>((int)batch);

                                                    for (var i = 0; i < batch; i++)
                                                    {
                                                        if (token.IsCancellationRequested) break;

                                                        var docId = targetDocs[Random.Shared.Next(targetDocs.Length)];
                                                        var taskId = taskIds[Random.Shared.Next(taskIds.Length)];
                                                        var isDelete = Random.Shared.NextDouble() < 0.2; // 20% chance to delete
                                                        var isDone = (opsCompleted + i) % 2 == 0;
                                                        
                                                        // Enqueue operation without awaiting immediately
                                                        pendingTasks.Add(FirePayloadAsync(docId, taskId, opsCompleted + i, isDone, isDelete, token));
                                                    }
                                                    
                                                    // Await the entire batch concurrently, maximizing thread pool utilization and breaking the serial bottleneck
                                                    await Task.WhenAll(pendingTasks).ConfigureAwait(false);
                                                    opsCompleted += batch;
                                                }
                                                else
                                                {
                                                    // Yield using standard timer tick avoiding harsh CPU burn when caught up
                                                    await Task.Delay(1, token).ConfigureAwait(false);
                                                }

                                                // Output actual telemetry every second to track bottlenecks natively
                                                if (reportSw.Elapsed.TotalSeconds >= 1.0)
                                                {
                                                    var currentCps = opsCompleted - lastReportedOps;
                                                    lastReportedOps = opsCompleted;
                                                    reportSw.Restart();
                                                    logger.LogInformation("[Hammer Telemetry] Target: {TargetCps}/s | Actual: {ActualCps}/s | Total Ops: {Total}", cps, currentCps, opsCompleted);
                                                }
                                            }
                                        }
                                        catch (OperationCanceledException)
                                        {
                                            // Expected during cancellation
                                        }
                                        catch (Exception ex)
                                        {
                                            logger.LogError(ex, "Hammer generator failed.");
                                        }
                                    }, token);
                                }
                            }
                            else
                            {
                                WriteLineLocked("Usage: hammer <changes_per_second> (use 0 to stop)");
                            }
                            break;
                            
                        case "print":
                            await DrawStateAsync(replicaId, orchestrator, taskManager, fleetManager, cts.Token).ConfigureAwait(false);
                            break;

                        case "mem":
                            await DrawMemoryStatsAsync(replicaId, orchestrator, cts.Token).ConfigureAwait(false);
                            break;

                        case "clone":
                            int cloneCount = 1;
                            if (parts.Length >= 2 && int.TryParse(parts[1], out var parsedCount) && parsedCount > 0)
                            {
                                cloneCount = parsedCount;
                            }
                            CloneProcess(logger, cloneCount);
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

    private static void CloneProcess(ILogger logger, int count)
    {
        var processPath = Environment.ProcessPath;

        if (string.IsNullOrEmpty(processPath))
        {
            logger.LogWarning("Unable to determine process path for cloning.");
            return;
        }

        var startPort = currentPort + 2;
        for (var i = 0; i < count; i++)
        {
            var nextPort = GetNextAvailablePort(startPort);

            Process.Start(new ProcessStartInfo
            {
                FileName = processPath,
                Arguments = nextPort.ToString(),
                UseShellExecute = true
            });

            logger.LogInformation("Cloned new cluster node on port {NextPort}.", nextPort);
            startPort = nextPort + 2;
        }
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
            Console.WriteLine(" gen-fleets <count> <devices>                   - Generates <count> fleets with <devices> devices each");
            Console.WriteLine(" hammer <cps>                                   - Pumps <cps> changes/sec into 5 sharded documents (0 to stop)");
            Console.WriteLine(" print                                          - Prints the current state of documents hosted on this node");
            Console.WriteLine(" mem                                            - Prints the serialized memory footprint of active and registry documents");
            Console.WriteLine(" clone [count]                                  - Spawns new node process(es) (default 1)");
            Console.WriteLine(" exit                                           - Shuts down the node");
            Console.WriteLine("=================================================\n");
        }
    }

    private static async Task DrawMemoryStatsAsync(string replicaId, ICrdtDocumentOrchestrator orchestrator, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(replicaId);
        ArgumentNullException.ThrowIfNull(orchestrator);

        var gcAllocatedBytes = GC.GetTotalMemory(forceFullCollection: false);
        var workingSetBytes = Environment.WorkingSet;

        var docs = orchestrator.GetActiveDocuments();

        ReusableMemoryStatsList.Clear();
        ReusableShardDvvBytesCache.Clear();

        long totalDataBytes = 0;
        long totalMetadataBytes = 0;
        long totalStateBytes = 0;
        long totalWholeObjectBytes = 0;
        long totalGlobalStateBytes = 0;

        foreach (var doc in docs)
        {
            var footprint = await doc.EstimateMemoryFootprintAsync(cancellationToken).ConfigureAwait(false);
            var dataBytes = footprint.DataBytes;
            var metadataBytes = footprint.MetadataBytes;
            var stateBytes = footprint.DataAndMetadataBytes;
            var wholeObjectBytes = footprint.WholeDocumentBytes;

            if (!ReusableShardDvvBytesCache.TryGetValue(doc.ShardId, out var globalStateBytes))
            {
                globalStateBytes = footprint.ShardVersionVectorBytes;
                ReusableShardDvvBytesCache[doc.ShardId] = globalStateBytes;
            }

            var docType = string.Equals(doc.DocumentId, orchestrator.Registry.DocumentId, StringComparison.Ordinal)
                ? "Registry"
                : doc switch
                {
                    IDistributedCrdtDocument<TaskListState> => "TaskList",
                    IDistributedCrdtDocument<FleetState> => "FleetList",
                    _ => "Document"
                };

            totalDataBytes += dataBytes;
            totalMetadataBytes += metadataBytes;
            totalStateBytes += stateBytes;
            totalWholeObjectBytes += wholeObjectBytes;
            totalGlobalStateBytes += globalStateBytes;

            ReusableMemoryStatsList.Add(new DocumentMemoryStatsDto(doc.DocumentId, doc.ShardId, docType, dataBytes, metadataBytes, stateBytes, wholeObjectBytes, globalStateBytes));
        }

        ReusableMemoryStatsList.Sort(MemoryStatsComparison);

        lock (ConsoleLock)
        {
            Console.WriteLine($"\n--- [Document Memory Footprint: {replicaId}] ---");
            Console.WriteLine($" {"Document ID",-25} | {"Type",-10} | {"Shard ID",-26} | {"Data Est.",14} | {"CRDT Meta Est.",14} | {"Data + Meta",14} | {"Whole Doc Est.",15} | {"Shard DVV Est.",14}");
            Console.WriteLine(TableSeparatorLine);

            for (int i = 0; i < ReusableMemoryStatsList.Count; i++)
            {
                var stat = ReusableMemoryStatsList[i];
                Console.WriteLine($" {stat.Id,-25} | {stat.DocumentType,-10} | {stat.ShardId,-26} | {FormatBytes(stat.DataSizeBytes),14} | {FormatBytes(stat.MetadataSizeBytes),14} | {FormatBytes(stat.StateSizeBytes),14} | {FormatBytes(stat.WholeObjectSizeBytes),15} | {FormatBytes(stat.GlobalStateSizeBytes),14}");
            }

            Console.WriteLine(TableSeparatorLine);
            Console.WriteLine($" Total Active Documents : {ReusableMemoryStatsList.Count} (1 Registry + {Math.Max(0, ReusableMemoryStatsList.Count - 1)} Active)");
            Console.WriteLine($" Total Actual Data Est. : {FormatBytes(totalDataBytes)} ({totalDataBytes:N0} B)");
            Console.WriteLine($" Total CRDT Meta Est.   : {FormatBytes(totalMetadataBytes)} ({totalMetadataBytes:N0} B)");
            Console.WriteLine($" Total Data + Meta Est. : {FormatBytes(totalStateBytes)} ({totalStateBytes:N0} B)");
            Console.WriteLine($" Total Whole Doc Objs   : {FormatBytes(totalWholeObjectBytes)} ({totalWholeObjectBytes:N0} B)");
            Console.WriteLine($" Total Shard DVV Est.   : {FormatBytes(totalGlobalStateBytes)} ({totalGlobalStateBytes:N0} B)");
            Console.WriteLine($" Total Managed GC Heap  : {FormatBytes(gcAllocatedBytes)} ({gcAllocatedBytes:N0} B)");
            Console.WriteLine($" Process Working Set    : {FormatBytes(workingSetBytes)} ({workingSetBytes:N0} B)");
            Console.WriteLine("------------------------------------\n> ");
        }
    }

    private static int CompareMemoryStats(DocumentMemoryStatsDto x, DocumentMemoryStatsDto y)
    {
        var xOrder = string.Equals(x.DocumentType, "Registry", StringComparison.Ordinal) ? 0 : 1;
        var yOrder = string.Equals(y.DocumentType, "Registry", StringComparison.Ordinal) ? 0 : 1;
        var cmp = xOrder.CompareTo(yOrder);
        return cmp != 0 ? cmp : string.CompareOrdinal(x.Id, y.Id);
    }

    private static string FormatBytes(long bytes)
    {
        if (bytes < 1024)
        {
            return $"{bytes} B";
        }

        var kb = bytes / 1024.0;
        if (kb < 1024.0)
        {
            return $"{kb:F2} KB";
        }

        var mb = kb / 1024.0;
        return $"{mb:F2} MB";
    }

    private static async Task DrawStateAsync(string replicaId, ICrdtDocumentOrchestrator orchestrator, ITaskManager taskManager, IFleetManager fleetManager, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(replicaId);
        ArgumentNullException.ThrowIfNull(orchestrator);
        ArgumentNullException.ThrowIfNull(taskManager);
        ArgumentNullException.ThrowIfNull(fleetManager);

        var taskDocsInfo = new List<DocumentStateDto<TaskItem>>();
        foreach (var doc in orchestrator.GetActiveDocuments().OfType<IDistributedCrdtDocument<TaskListState>>())
        {
            var tasks = await taskManager.GetTasksAsync(doc.DocumentId, cancellationToken).ConfigureAwait(false);
            taskDocsInfo.Add(new DocumentStateDto<TaskItem>(doc.DocumentId, tasks));
        }

        var fleetDocsInfo = new List<DocumentStateDto<DeviceStatus>>();
        foreach (var doc in orchestrator.GetActiveDocuments().OfType<IDistributedCrdtDocument<FleetState>>())
        {
            var devices = await fleetManager.GetDevicesAsync(doc.DocumentId, cancellationToken).ConfigureAwait(false);
            fleetDocsInfo.Add(new DocumentStateDto<DeviceStatus>(doc.DocumentId, devices));
        }

        lock (ConsoleLock)
        {
            Console.WriteLine($"\n--- [Local Shard State: {replicaId}] ---");
            Console.WriteLine("   (Only documents assigned to this specific node are printed)");
            
            Console.WriteLine("\n [Tasks CRDT State]");
            if (taskDocsInfo.Count == 0)
            {
                Console.WriteLine("   (No task lists hosted locally)");
            }
            else
            {
                foreach (var info in taskDocsInfo)
                {
                    Console.WriteLine($"   => List [{info.Id}]:");
                    if (info.Items.Count == 0)
                    {
                        Console.WriteLine("      (Empty)");
                    }
                    else
                    {
                        foreach (var task in info.Items.OrderBy(t => t.Key))
                        {
                            Console.WriteLine($"      [{task.Key}]: {task.Value.Description} (Done: {task.Value.IsDone})");
                        }
                    }
                }
            }

            Console.WriteLine("\n [Fleet CRDT State]");
            if (fleetDocsInfo.Count == 0)
            {
                Console.WriteLine("   (No fleets hosted locally)");
            }
            else
            {
                foreach (var info in fleetDocsInfo)
                {
                    Console.WriteLine($"   => Fleet [{info.Id}]:");
                    if (info.Items.Count == 0)
                    {
                        Console.WriteLine("      (Empty)");
                    }
                    else
                    {
                        foreach (var device in info.Items.OrderBy(d => d.Key))
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

    private sealed class ConsoleRedrawObserver : IDocumentStateObserver<TaskListState>, IDocumentStateObserver<FleetState>, ICrdtOrchestratorObserver
    {
        public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
        {
            _needsRedraw = true;
            return Task.CompletedTask;
        }

        public Task OnTopologyChangedAsync(CancellationToken cancellationToken = default)
        {
            _needsRedraw = true;
            return Task.CompletedTask;
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