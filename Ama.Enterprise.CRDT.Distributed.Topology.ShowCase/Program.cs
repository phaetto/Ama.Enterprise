namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase;

using System;
using System.Diagnostics;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;
using Ama.Enterprise.CRDT.MessagePack.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Ama.Enterprise.Licensing.Extensions;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Extensions;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;
using Ama.Enterprise.P2p.AspNetCore.Models;

/// <summary>
/// Entry point for demonstrating Multiple Distributed CRDTs orchestrated via a global registry.
/// </summary>
public static class Program
{
    private static readonly object ConsoleLock = new();
    private static string currentMode = string.Empty;
    private static int currentPort;
    private static int currentTcpPort;
    private static int currentHandshakePort;
    private static volatile bool needsRedraw;
    private static string currentRole = string.Empty;
    private static string currentRegion = string.Empty;

    private readonly record struct DocumentStateDto<T>(string Id, IReadOnlyDictionary<string, T> Items);

    private readonly record struct PeerHealthDisplayDto(
        string MeshId,
        string PeerId,
        string ReplicaId,
        PeerStatus Status,
        string Endpoint,
        int TrackedShards);

    private readonly record struct ShardTopologyDisplayDto(
        string ShardId,
        int DocumentCount,
        bool IsLocal,
        IReadOnlyList<string> TargetReplicas);

    public static async Task Main(string[] args)
    {
        var parentPort = 0;

        currentMode = args.Length > 0 ? args[0].ToLowerInvariant() : "server";
        if (currentMode != "server" && currentMode != "user") currentMode = "server";

        if (args.Length > 1) int.TryParse(args[1], out currentPort);
        if (currentPort <= 0) currentPort = GetNextAvailablePort(8100);

        if (currentMode == "server")
        {
            currentRole = string.Empty; // Server has no role, it can host both admin and user data
            currentRegion = args.Length > 2 ? args[2] : "EU";
            if (args.Length > 3) int.TryParse(args[3], out parentPort);
        }
        else
        {
            currentRole = args.Length > 2 ? args[2] : "user";
            currentRegion = args.Length > 3 ? args[3] : "EU";
            if (args.Length > 4) int.TryParse(args[4], out parentPort);
        }

        currentTcpPort = 0;
        currentHandshakePort = 0;

        if (currentMode == "server")
        {
            currentTcpPort = GetNextAvailablePort(currentPort + 1);
            currentHandshakePort = GetNextAvailablePort(currentTcpPort + 1);
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

        // Register licensing
        services.ConfigureAmaCommunityLicense();

        // Implement transparent console redraw triggering decoupled directly executing decoupled safely
        services.AddSingleton<ConsoleRedrawObserver>();
        services.AddSingleton<IDocumentStateObserver<TaskListState>>(sp => sp.GetRequiredService<ConsoleRedrawObserver>());
        services.AddSingleton<IDocumentStateObserver<FleetState>>(sp => sp.GetRequiredService<ConsoleRedrawObserver>());
        services.AddSingleton<ICrdtOrchestratorObserver>(sp => sp.GetRequiredService<ConsoleRedrawObserver>());

        // Add core CRDT distributed services and resolve the orchestrator
        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.PeerEvictionTtlSeconds = 0;
            options.AntiEntropyInitialDelaySeconds = 2;
            options.AntiEntropyIntervalSeconds = 15;
            options.CheckpointIntervalSeconds = 120;
            options.JournalSoftTrimThreshold = 15000;
        });

        // Register custom topology provider tracking multi-mesh active session generic explicit partitions gracefully natively
        services.Replace(ServiceDescriptor.Scoped<IScopeTopologyProvider, RbacScopeTopologyProvider>());

        // Register the showcase JSON AOT and CRDT AOT contexts directly into the generic CRDT pipeline
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(ShowCaseJsonContext.Default)
                .AddCrdtAotContext(new ShowCaseCrdtAotContext())
                .AddCrdtSerializableType<TaskItem>("task-item")
                .AddCrdtSerializableType<DeviceStatus>("device-status")
                .AddCrdtSystemTextJson(useBrotliCompression: true);

        services.AddCrdtMessagePack(
            MessagePack.Resolvers.Ama_Enterprise_CRDT_MessagePack_MessagePackResolver.Instance,
            MessagePack.Resolvers.Ama_Enterprise_CRDT_Distributed_Topology_ShowCase_MessagePackResolver.Instance
        );

        // Register document types into the orchestrator and expose generic interfaces via explicit transparent forwarders
        services.AddDistributedDocumentType<TaskListState>("task-list");
        services.AddDistributedCrdtService<ITaskManager, TaskManager>();

        services.AddDistributedDocumentType<FleetState>("fleet-list");
        services.AddDistributedCrdtService<IFleetManager, FleetManager>();

        // Register Showcase file-based CRDT storage overriding memory fallbacks for all documents
        services.AddDistributedCrdtStorage<ShowCaseCrdtStorage>();

        // Register custom token context explicitly providing RBAC configurations dynamically
        services.AddSingleton(new ShowCaseNodeContext(currentRole, currentRegion));

        // Network Layer bindings

        if (currentMode == "server")
        {
            // Server-to-Server Mesh (TCP/UDP)
            services.AddDistributedCrdtReplica(replicaId, "server");

            services.AddP2pMesh("server")
                .AddSessionAuthentication<ShowCaseTokenValidator>(options =>
                {
                    options.RequireSession = true;
                })
                .EnableZeroTrustRouting()
                .AddRoutingPolicy<RbacMeshRoutingPolicy>()
                .AddGossipNetwork(options =>
                {
                    options.GossipInterval = TimeSpan.FromMilliseconds(500);
                })
                .ConfigureFailureDetector(options =>
                {
                    options.HeartbeatInterval = TimeSpan.FromSeconds(5);
                })
                .AddTcpTransport(options =>
                {
                    options.ListenPort = currentTcpPort;
                    options.ListenHost = "127.0.0.1";
                    options.MaxMessageSize = 100 * 1024 * 1024;
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
                });
        }

        // Both Server and User expose WebRTC out-of-band HTTP signaling.
        // Server awaits Users. User awaits other downstream Users or acts as edge node.
        
        services.AddDistributedCrdtReplica(replicaId, "user");

        services.AddP2pMesh("user")
            .AddSessionAuthentication<ShowCaseTokenValidator>(options =>
            {
                options.RequireSession = true;
            })
            .EnableZeroTrustRouting()
            .AddRoutingPolicy<RbacMeshRoutingPolicy>()
            .AddGossipNetwork(options =>
            {
                options.GossipInterval = TimeSpan.FromMilliseconds(500);
            })
            .ConfigureFailureDetector(options =>
            {
                options.HeartbeatInterval = TimeSpan.FromSeconds(5);
            })
            .AddWebRtcTransport(options =>
            {
                options.IceServers = Array.Empty<string>();
                options.IceGatheringTimeout = TimeSpan.FromSeconds(2);
            })
            .AddAspNetCoreWebRtcSignaling(options =>
            {
                options.HostingMode = AspNetCoreHostingMode.Standalone;
                options.StandaloneListenHost = "127.0.0.1";
                options.StandaloneListenPort = currentPort;
                options.IgnoreOutboundSslErrors = true;
            })
            .AddWebRtcHttpPeerDiscovery();

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
            if (currentMode == "server")
            {
                logger.LogInformation("Starting Dynamic Multi-CRDT node in SERVER mode on WS: {Port}, TCP: {TcpPort}, UDP Handshake: {HandshakePort}...", currentPort, currentTcpPort, currentHandshakePort);
            }
            else
            {
                logger.LogInformation("Starting Dynamic Multi-CRDT node in USER mode on WS: {Port}...", currentPort);
            }

            foreach (var service in hostedServices)
            {
                await service.StartAsync(cts.Token).ConfigureAwait(false);
            }

            // In User mode, actively connect to the designated parent's WebRTC signaling endpoint out-of-band
            if (currentMode == "user" && parentPort > 0)
            {
                logger.LogInformation("Connecting to parent node via WebRTC signaling at http://127.0.0.1:{ParentPort}...", parentPort);
                var discovery = provider.GetRequiredKeyedService<IWebRtcHttpPeerDiscovery>("user");
                var connected = await discovery.DiscoverPeerAsync(new Uri($"http://127.0.0.1:{parentPort}"), null, cts.Token).ConfigureAwait(false);
                
                if (connected)
                {
                    logger.LogInformation("Successfully connected to parent node via WebRTC!");
                }
                else
                {
                    logger.LogWarning("Failed to connect to parent node via WebRTC.");
                }
            }

            var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);

            var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
            var taskManager = scope.ServiceProvider.GetRequiredService<ITaskManager>();
            var fleetManager = scope.ServiceProvider.GetRequiredService<IFleetManager>();
            var clusterTracker = scope.ServiceProvider.GetRequiredService<IClusterStateTracker>();
            var shardDistributionManager = scope.ServiceProvider.GetRequiredService<IShardDistributionManager>();
            var shardIdProvider = scope.ServiceProvider.GetRequiredService<IShardIdProvider>();
            var peerRegistry = provider.GetRequiredService<IPeerRegistry>();

            // Background UI rendering task to decouple Console I/O from hot CRDT operations
            _ = Task.Run(async () =>
            {
                try
                {
                    using var uiTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(500));
                    while (await uiTimer.WaitForNextTickAsync(cts.Token).ConfigureAwait(false))
                    {
                        if (needsRedraw)
                        {
                            needsRedraw = false;
                            await DrawStateAsync(orchestrator, taskManager, fleetManager, cts.Token).ConfigureAwait(false);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // Clean shutdown
                }
            }, cts.Token);

            CancellationTokenSource? hammerCts = null;

            DrawMenu();
            await DrawStateAsync(orchestrator, taskManager, fleetManager, cts.Token).ConfigureAwait(false);

            var shutdownCoordinator = provider.GetRequiredService<ICrdtShutdownCoordinator>();
            using var loopCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token, shutdownCoordinator.ShutdownToken);

            while (!loopCts.Token.IsCancellationRequested)
            {
                var input = await ReadConsoleLineAsync(loopCts.Token).ConfigureAwait(false);
                if (loopCts.Token.IsCancellationRequested)
                {
                    break;
                }

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

                        case "hammer":
                            if (parts.Length >= 2 && int.TryParse(parts[1], out var cps))
                            {
                                if (hammerCts is not null)
                                {
                                    await hammerCts.CancelAsync().ConfigureAwait(false);
                                    hammerCts.Dispose();
                                    hammerCts = null;
                                    logger.LogInformation("Hammer mode stopped.");
                                }

                                if (cps > 0)
                                {
                                    hammerCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);
                                    var token = hammerCts.Token;

                                    _ = Task.Run(async () =>
                                    {
                                        try
                                        {
                                            await orchestrator.CreateDocumentAsync("nail", "task-list", token).ConfigureAwait(false);
                                            logger.LogInformation("Hammer mode activated. Target: {Cps} ops/sec against doc 'nail'...", cps);

                                            var sw = Stopwatch.StartNew();
                                            var reportSw = Stopwatch.StartNew();
                                            long opsCompleted = 0;
                                            long lastReportedOps = 0;

                                            // Bounded pool of 10 keys: keeps document state size small
                                            // but exercises CRDT map updates visibly in the console UI
                                            var taskIds = Enumerable.Range(1, 10).Select(i => $"nail-task-{i}").ToArray();

                                            // Helper for concurrent dispatch avoiding closure captures or ValueTask casting ambiguities
                                            async Task FirePayloadAsync(string tId, long index, bool done, CancellationToken ct)
                                            {
                                                await taskManager.SetTaskAsync("nail", tId, $"Hammered payload {index}", done, ct).ConfigureAwait(false);
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
                                                        var taskId = taskIds[Random.Shared.Next(taskIds.Length)];
                                                        var isDone = (opsCompleted + i) % 2 == 0;
                                                        
                                                        // Enqueue operation without awaiting immediately
                                                        pendingTasks.Add(FirePayloadAsync(taskId, opsCompleted + i, isDone, token));
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

                        case "peers":
                        case "topology":
                            {
                                var showDetails = parts.Length >= 2 &&
                                    (string.Equals(parts[1], "details", StringComparison.OrdinalIgnoreCase) ||
                                     (bool.TryParse(parts[1], out var parsedDetails) && parsedDetails));
                                await DrawTopologyAndPeersAsync(replicaId, orchestrator, clusterTracker, shardDistributionManager, shardIdProvider, peerRegistry, showDetails, cts.Token).ConfigureAwait(false);
                            }
                            break;

                        case "clone":
                            var cMode = parts.Length > 1 ? parts[1].ToLowerInvariant() : "user";
                            if (cMode != "server" && cMode != "user")
                            {
                                WriteLineLocked("Usage: clone <server|user> ...");
                                break;
                            }

                            string cRole = string.Empty;
                            string cRegion = currentRegion;
                            
                            if (cMode == "server")
                            {
                                cRegion = parts.Length > 2 ? parts[2] : currentRegion;
                            }
                            else
                            {
                                cRole = parts.Length > 2 ? parts[2] : "user";
                                cRegion = parts.Length > 3 ? parts[3] : currentRegion;
                            }
                            
                            CloneProcess(logger, cMode, cRole, cRegion);
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

    private static void CloneProcess(ILogger logger, string mode, string role, string region)
    {
        var nextPort = GetNextAvailablePort(currentPort + 1);
        var processPath = Environment.ProcessPath;

        if (string.IsNullOrEmpty(processPath))
        {
            logger.LogWarning("Unable to determine process path for cloning.");
            return;
        }

        var arguments = mode == "user"
            ? $"{mode} {nextPort} {role} {region} {currentPort}"
            : $"{mode} {nextPort} {region}";

        Process.Start(new ProcessStartInfo
        {
            FileName = processPath,
            Arguments = arguments,
            UseShellExecute = true
        });

        var displayRole = string.IsNullOrEmpty(role) ? "None" : role;
        logger.LogInformation("Cloned new cluster node on port {NextPort} in {Mode} mode with Role '{Role}' and Region '{Region}'.", nextPort, mode.ToUpperInvariant(), displayRole, region);
    }

    private static void DrawMenu()
    {
        lock (ConsoleLock)
        {
            var displayRole = string.IsNullOrEmpty(currentRole) ? "None" : currentRole;

            Console.WriteLine("=================================================");
            Console.WriteLine($" Multi-CRDT Peer Node - Listening on WS Port {currentPort}");
            Console.WriteLine($" Session: Mode = {currentMode.ToUpperInvariant()}, Role = {displayRole}, Region = {currentRegion}");
            Console.WriteLine("=================================================");
            Console.WriteLine("Commands:");
            Console.WriteLine(" new-list <docId>                               - Creates a new Task List document");
            Console.WriteLine(" new-fleet <docId>                              - Creates a new Fleet List document");
            Console.WriteLine(" del-doc <docId>                                - Tombstones and removes an active document");
            Console.WriteLine(" tset <docId> <taskId> <desc> <true|false>      - Adds/Updates a task item");
            Console.WriteLine(" tdel <docId> <taskId>                          - Removes a task item");
            Console.WriteLine(" fset <docId> <deviceId> <true|false> <batt>    - Adds/Updates a fleet device");
            Console.WriteLine(" fdel <docId> <deviceId>                        - Removes a fleet device");
            Console.WriteLine(" hammer <cps>                                   - Pumps <cps> changes/sec into doc 'nail' (0 to stop)");
            Console.WriteLine(" topology [details] / peers [details]           - Prints connected peers health and shard topology");
            Console.WriteLine(" clone server [region]                          - Spawns a new server node (e.g. clone server EU)");
            Console.WriteLine(" clone user [role] [region]                     - Spawns a new user node connecting via WebRTC (e.g. clone user user EU)");
            Console.WriteLine(" exit                                           - Shuts down the node");
            Console.WriteLine("=================================================\n");
        }
    }

    private static async Task DrawTopologyAndPeersAsync(
        string replicaId,
        ICrdtDocumentOrchestrator orchestrator,
        IClusterStateTracker clusterTracker,
        IShardDistributionManager shardDistributionManager,
        IShardIdProvider shardIdProvider,
        IPeerRegistry peerRegistry,
        bool showDetails,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(replicaId);
        ArgumentNullException.ThrowIfNull(orchestrator);
        ArgumentNullException.ThrowIfNull(clusterTracker);
        ArgumentNullException.ThrowIfNull(shardDistributionManager);
        ArgumentNullException.ThrowIfNull(shardIdProvider);
        ArgumentNullException.ThrowIfNull(peerRegistry);

        var peerList = new List<PeerHealthDisplayDto>();
        var meshes = currentMode == "server" ? new[] { "server", "user" } : new[] { "user" };
        var statuses = new[] { PeerStatus.Active, PeerStatus.Suspect, PeerStatus.Dead };

        foreach (var meshId in meshes)
        {
            foreach (var status in statuses)
            {
                var peers = await peerRegistry.GetPeersByStatusAsync(meshId, status, cancellationToken).ConfigureAwait(false);
                foreach (var peer in peers)
                {
                    var peerIdStr = peer.Id.Value.ToString();
                    var mappedReplicaId = clusterTracker.GetReplicaIdByPeerId(peerIdStr);
                    var trackedShards = 0;

                    if (!string.IsNullOrEmpty(mappedReplicaId))
                    {
                        var peerState = clusterTracker.TryGetPeerState(mappedReplicaId);
                        trackedShards = peerState?.Keys.Count ?? 0;
                    }

                    peerList.Add(new PeerHealthDisplayDto(
                        meshId,
                        peerIdStr,
                        mappedReplicaId ?? "(Unmapped)",
                        status,
                        peer.Endpoint.ToString() ?? "Unknown",
                        trackedShards));
                }
            }
        }

        var activeReplicaIds = clusterTracker.GetActiveReplicaIds();
        var exportedTrackerState = clusterTracker.ExportState();
        var tombstonedReplicas = exportedTrackerState.TombstonedReplicas;

        var regDoc = await orchestrator.Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        var regState = regDoc.Data ?? new CrdtRegistryState();

        var shardDocCounts = new Dictionary<string, int>(StringComparer.Ordinal)
        {
            [orchestrator.Registry.ShardId] = 1
        };

        var activeDocumentCount = 0;
        var deletedDocumentCount = 0;

        foreach (var kvp in regState.Documents)
        {
            var entry = kvp.Value;
            if (entry.IsDeleted)
            {
                deletedDocumentCount++;
                continue;
            }

            activeDocumentCount++;
            var shardId = ResolveShardIdForEntry(shardIdProvider, entry.DocumentId, entry.TypeAlias);
            if (shardDocCounts.TryGetValue(shardId, out var currentCount))
            {
                shardDocCounts[shardId] = currentCount + 1;
            }
            else
            {
                shardDocCounts[shardId] = 1;
            }
        }

        var shardTopologies = new List<ShardTopologyDisplayDto>(shardDocCounts.Count);
        var localShardCount = 0;
        var remoteShardCount = 0;

        foreach (var kvp in shardDocCounts.OrderBy(s => s.Key, StringComparer.Ordinal))
        {
            var isLocal = await shardDistributionManager.IsShardLocalAsync(kvp.Key, cancellationToken).ConfigureAwait(false);
            var targetReplicas = await shardDistributionManager.GetTargetNodesForShardAsync(kvp.Key, cancellationToken).ConfigureAwait(false);

            if (isLocal)
            {
                localShardCount++;
            }
            else
            {
                remoteShardCount++;
            }

            shardTopologies.Add(new ShardTopologyDisplayDto(kvp.Key, kvp.Value, isLocal, targetReplicas));
        }

        var localActiveDocsCount = orchestrator.GetActiveDocuments().Count;
        var displayRole = string.IsNullOrEmpty(currentRole) ? "None" : currentRole;
        var portSummary = currentMode == "server"
            ? $"WS Port: {currentPort}, TCP Port: {currentTcpPort}, Handshake Port: {currentHandshakePort}"
            : $"WS Port: {currentPort}";

        lock (ConsoleLock)
        {
            Console.WriteLine($"\n--- [Cluster Peers & Topology: {replicaId}] ---");
            Console.WriteLine($" Local Replica ID       : {replicaId} ({portSummary})");
            Console.WriteLine($" Local Session Context  : Mode={currentMode.ToUpperInvariant()}, Role={displayRole}, Region={currentRegion}");
            Console.WriteLine($" Active CRDT Replicas   : {activeReplicaIds.Count + 1} (Local: {replicaId}{(activeReplicaIds.Count > 0 ? ", Remote: " + string.Join(", ", activeReplicaIds.OrderBy(r => r, StringComparer.Ordinal)) : string.Empty)})");
            Console.WriteLine($" Tombstoned Replicas    : {tombstonedReplicas.Count}{(tombstonedReplicas.Count > 0 ? " (" + string.Join(", ", tombstonedReplicas.Keys.OrderBy(k => k, StringComparer.Ordinal)) + ")" : string.Empty)}");

            Console.WriteLine("\n [P2P Network Peers & Health]");
            if (peerList.Count == 0)
            {
                Console.WriteLine("   (No peers discovered across meshes)");
            }
            else
            {
                var aliveCount = peerList.Count(p => p.Status == PeerStatus.Active);
                var suspectedCount = peerList.Count(p => p.Status == PeerStatus.Suspect);
                var deadCount = peerList.Count(p => p.Status == PeerStatus.Dead);
                Console.WriteLine($"   Summary: Total={peerList.Count} | Alive={aliveCount} | Suspected={suspectedCount} | Dead={deadCount}");
                Console.WriteLine($"   {"Mesh",-10} | {"Status",-10} | {"Replica ID",-16} | {"Tracked Shards",14} | {"Peer ID",-36} | {"Endpoint"}");
                Console.WriteLine($"   {new string('-', 120)}");

                foreach (var peer in peerList.OrderBy(p => p.MeshId, StringComparer.Ordinal).ThenBy(p => p.Status).ThenBy(p => p.ReplicaId, StringComparer.Ordinal))
                {
                    Console.WriteLine($"   {peer.MeshId,-10} | {peer.Status,-10} | {peer.ReplicaId,-16} | {peer.TrackedShards,14} | {peer.PeerId,-36} | {peer.Endpoint}");
                }
            }

            Console.WriteLine("\n [CRDT Shard Topology Overview]");
            Console.WriteLine($"   Cluster Registry Docs  : {activeDocumentCount} Active, {deletedDocumentCount} Tombstoned (+1 System Registry)");
            Console.WriteLine($"   Local Hosted Documents : {localActiveDocsCount} in-memory");
            Console.WriteLine($"   Total Cluster Shards   : {shardTopologies.Count} (Hosted Locally: {localShardCount}, Remote Only: {remoteShardCount})");

            if (showDetails && shardTopologies.Count > 0)
            {
                Console.WriteLine($"\n   {"Shard ID",-28} | {"Docs",6} | {"Locality",-10} | {"Assigned Replicas"}");
                Console.WriteLine($"   {new string('-', 95)}");

                foreach (var shard in shardTopologies)
                {
                    var locality = shard.IsLocal ? "Local" : "Remote";
                    var replicasText = shard.TargetReplicas.Count > 0 ? string.Join(", ", shard.TargetReplicas) : "(None)";
                    Console.WriteLine($"   {shard.ShardId,-28} | {shard.DocumentCount,6} | {locality,-10} | {replicasText}");
                }
            }
            else if (!showDetails && shardTopologies.Count > 0)
            {
                Console.WriteLine("   (Run 'topology details' or 'peers details' to list individual shard assignments)");
            }

            Console.WriteLine("------------------------------------\n> ");
        }
    }

    private static string ResolveShardIdForEntry(IShardIdProvider shardIdProvider, string documentId, string typeAlias)
    {
        return typeAlias switch
        {
            "task-list" => shardIdProvider.GetShardId<TaskListState>(documentId),
            "fleet-list" => shardIdProvider.GetShardId<FleetState>(documentId),
            _ => documentId
        };
    }

    private static async Task DrawStateAsync(ICrdtDocumentOrchestrator orchestrator, ITaskManager taskManager, IFleetManager fleetManager, CancellationToken cancellationToken)
    {
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
            Console.WriteLine("\n--- [Cluster Registry Synchronized] ---");
            
            Console.WriteLine(" [Tasks CRDT State]");
            if (taskDocsInfo.Count == 0)
            {
                Console.WriteLine("   (No task lists currently defined)");
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
                Console.WriteLine("   (No fleets currently defined)");
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

    private static async Task<string?> ReadConsoleLineAsync(CancellationToken cancellationToken)
    {
        try
        {
            return await Task.Run(Console.ReadLine, cancellationToken).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return null;
        }
    }

    private sealed class ConsoleRedrawObserver : IDocumentStateObserver<TaskListState>, IDocumentStateObserver<FleetState>, ICrdtOrchestratorObserver
    {
        public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
        {
            needsRedraw = true;
            return Task.CompletedTask;
        }

        public Task OnTopologyChangedAsync(CancellationToken cancellationToken = default)
        {
            needsRedraw = true;
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