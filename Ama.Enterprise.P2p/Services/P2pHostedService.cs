namespace Ama.Enterprise.P2p.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// An orchestrating background service that boots all configured Keyed P2P meshes across the application lifecycle.
/// Orchestrates discovery loops and standard health checks natively explicitly decoupling logic from the protocol implementations cleanly.
/// </summary>
public sealed class P2pHostedService(
    IServiceProvider serviceProvider,
    IEnumerable<P2pMeshMetadata> meshes,
    IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
    ILogger<P2pHostedService> logger,
    IP2pAlgorithm? p2pProtocol = null) : IHostedService
{
    private readonly IServiceProvider serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IEnumerable<P2pMeshMetadata> meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
    private readonly ILogger<P2pHostedService> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    private readonly IP2pAlgorithm? p2pProtocol = p2pProtocol;

    private readonly ConcurrentDictionary<string, Task> inboundProcessors = new();
    private readonly ConcurrentDictionary<string, MeshState> activeMeshes = new();

    private const int CacheSize = 131072;
    private const int CacheMask = CacheSize - 1;
    private readonly long[] seenMessagesCache = new long[CacheSize];

    private CancellationTokenSource? processCts;

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        processCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        foreach (var mesh in meshes)
        {
            logger.LogInformation("Orchestrating startup for P2P mesh network: {MeshId}", mesh.MeshId);

            var state = new MeshState(
                mesh.MeshId,
                serviceProvider.GetRequiredKeyedService<IApplicationPayloadDispatcher>(mesh.MeshId),
                serviceProvider.GetRequiredKeyedService<IFailureDetector>(mesh.MeshId),
                serviceProvider.GetRequiredService<IPeerRegistry>()
            );

            state.LoopCts = CancellationTokenSource.CreateLinkedTokenSource(processCts.Token);
            state.HealthCheckLoopTask = Task.Run(() => HealthCheckLoopAsync(mesh.MeshId, state, state.LoopCts.Token), state.LoopCts.Token);
            
            activeMeshes.TryAdd(mesh.MeshId, state);

            var targetQueue = serviceProvider.GetKeyedService<IInboundMessageQueue<IMeshMessage>>(mesh.MeshId);
            if (targetQueue is not null)
            {
                var processorTask = Task.Run(() => ProcessInboundQueueAsync(mesh.MeshId, state, targetQueue, processCts.Token), processCts.Token);
                inboundProcessors.TryAdd(mesh.MeshId, processorTask);
            }

            var listeners = serviceProvider.GetKeyedServices<ITransportListener>(mesh.MeshId);
            foreach (var listener in listeners)
            {
                await listener.StartListeningAsync(async msg => 
                {
                    var incomingVersion = new Version(0, 0, 0);
                    if (!string.IsNullOrWhiteSpace(msg.ProtocolVersion) && Version.TryParse(msg.ProtocolVersion, out var parsedVersion))
                    {
                        incomingVersion = parsedVersion;
                    }

                    var localVersion = Version.Parse(Constants.ProtocolVersion);
                    if (incomingVersion.Major != localVersion.Major)
                    {
                        logger.LogWarning("[{MeshId}] Rejected incoming protocol message due to major version mismatch. Local: {LocalVersion}, Incoming: {IncomingVersion}", mesh.MeshId, localVersion, incomingVersion);
                        throw new NotSupportedException($"Protocol major version mismatch. Local: {localVersion.Major}, Incoming: {incomingVersion.Major}");
                    }

                    if (!string.Equals(msg.MeshId, mesh.MeshId, StringComparison.Ordinal))
                    {
                        logger.LogWarning("[{ExpectedMeshId}] Listener received message isolated for a different mesh {ActualMeshId}.", mesh.MeshId, msg.MeshId);
                        return;
                    }

                    if (targetQueue is not null)
                    {
                        await targetQueue.WriteAsync(msg, default).ConfigureAwait(false);
                    }
                    else
                    {
                        logger.LogWarning("[{MeshId}] Received generic message but no inbound queue is configured.", mesh.MeshId);
                    }
                }, cancellationToken).ConfigureAwait(false);
            }

            var handshaker = serviceProvider.GetKeyedService<IPeerHandshaker>(mesh.MeshId);
            if (handshaker is not null)
            {
                await handshaker.StartListeningAsync(cancellationToken).ConfigureAwait(false);
            }

            var discovery = serviceProvider.GetKeyedService<IPeerDiscovery>(mesh.MeshId);
            if (discovery is not null)
            {
                await discovery.StartListeningAsync(cancellationToken).ConfigureAwait(false);
                state.DiscoveryLoopTask = Task.Run(() => DiscoveryLoopAsync(mesh.MeshId, state, discovery, state.LoopCts.Token), state.LoopCts.Token);
            }
        }

        if (p2pProtocol is null)
        {
            logger.LogWarning("No IP2pProtocol is registered. The overarching P2P protocol will not be started.");
        }
        else
        {
            await p2pProtocol.StartAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (processCts is not null)
        {
            await processCts.CancelAsync().ConfigureAwait(false);
        }

        if (p2pProtocol is not null)
        {
            await p2pProtocol.StopAsync(cancellationToken).ConfigureAwait(false);
        }

        var tasksToAwait = new List<Task>(inboundProcessors.Values);
        foreach (var state in activeMeshes.Values)
        {
            if (state.HealthCheckLoopTask is not null)
            {
                tasksToAwait.Add(state.HealthCheckLoopTask);
            }
            if (state.DiscoveryLoopTask is not null)
            {
                tasksToAwait.Add(state.DiscoveryLoopTask);
            }
        }

        foreach (var task in tasksToAwait)
        {
            try
            {
                await task.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred while awaiting generic background processor shutdown.");
            }
        }

        foreach (var state in activeMeshes.Values)
        {
            state.Dispose();
        }
        activeMeshes.Clear();

        foreach (var mesh in meshes)
        {
            logger.LogInformation("Orchestrating shutdown for P2P mesh network: {MeshId}", mesh.MeshId);

            var discovery = serviceProvider.GetKeyedService<IPeerDiscovery>(mesh.MeshId);
            if (discovery is not null)
            {
                await discovery.StopListeningAsync(cancellationToken).ConfigureAwait(false);
            }

            var handshaker = serviceProvider.GetKeyedService<IPeerHandshaker>(mesh.MeshId);
            if (handshaker is not null)
            {
                await handshaker.StopListeningAsync(cancellationToken).ConfigureAwait(false);
            }

            var listeners = serviceProvider.GetKeyedServices<ITransportListener>(mesh.MeshId);
            foreach (var listener in listeners)
            {
                await listener.StopListeningAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        if (processCts is not null)
        {
            processCts.Dispose();
            processCts = null;
        }
    }

    private async Task ProcessInboundQueueAsync(string meshId, MeshState state, IInboundMessageQueue<IMeshMessage> targetQueue, CancellationToken cancellationToken)
    {
        try
        {
            await foreach (var message in targetQueue.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                await HandleIncomingMessageAsync(meshId, state, message, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] An error occurred while orchestrating the generic inbound message queue.", meshId);
        }
    }

    private async Task HandleIncomingMessageAsync(string meshId, MeshState state, IMeshMessage message, CancellationToken cancellationToken)
    {
        var messageId = message.MessageId;
        var hash = Unsafe.As<Guid, long>(ref messageId);
        var index = (int)(hash & CacheMask);
        var existingHash = Volatile.Read(ref seenMessagesCache[index]);
        if (existingHash == hash)
        {
            logger.LogDebug("[{MeshId}] Suppressed duplicate generic message {MessageId}.", meshId, messageId);
            return;
        }

        Volatile.Write(ref seenMessagesCache[index], hash);

        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        if (message.SenderId.Value == nodeOptions.LocalPeerId)
        {
            logger.LogDebug("[{MeshId}] Received our own message {MessageId} echoed back. Dropping.", meshId, message.MessageId);
            return;
        }

        try
        {
            await state.FailureDetector.RecordHeartbeatAsync(message.SenderId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to record heartbeat for peer {PeerId}.", meshId, message.SenderId.Value);
        }

        if (p2pProtocol is not null)
        {
            await p2pProtocol.ProcessMessageAsync(message, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await state.Dispatcher.DispatchAsync(meshId, message.SenderId, message.Payload, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HealthCheckLoopAsync(string meshId, MeshState state, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var options = nodeOptionsMonitor.Get(meshId);
                await Task.Delay(options.HealthCheckInterval, cancellationToken).ConfigureAwait(false);
                
                var peers = await state.PeerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
                foreach (var peer in peers)
                {
                    var health = await state.FailureDetector.EvaluatePeerHealthAsync(peer.Id, cancellationToken).ConfigureAwait(false);
                    
                    if (health == PeerStatus.Dead)
                    {
                        logger.LogInformation("[{MeshId}] Peer {PeerId} marked as Dead by failure detector. Removing from registry.", meshId, peer.Id.Value);
                        await state.PeerRegistry.RemovePeerAsync(meshId, peer.Id, cancellationToken).ConfigureAwait(false);
                    }
                    else if (health == PeerStatus.Suspect)
                    {
                        await state.PeerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Suspect, cancellationToken).ConfigureAwait(false);
                    }
                    else 
                    {
                        await state.PeerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] An error occurred during the algorithm health check tick.", meshId);
            }
        }
    }

    private async Task DiscoveryLoopAsync(string meshId, MeshState state, IPeerDiscovery discovery, CancellationToken cancellationToken)
    {
        try
        {
            var options = nodeOptionsMonitor.Get(meshId);
            await Task.Delay(options.InitialDiscoveryDelay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        var currentInterval = discovery.DiscoveryInterval;

        while (!cancellationToken.IsCancellationRequested)
        {
            var baseInterval = discovery.DiscoveryInterval;

            try
            {
                var discoveredPeers = await discovery.DiscoverPeersAsync(cancellationToken).ConfigureAwait(false);

                foreach (var peer in discoveredPeers)
                {
                    await state.PeerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                }

                var options = nodeOptionsMonitor.Get(meshId);
                var activePeers = await state.PeerRegistry.GetPeersByStatusAsync(meshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                var activeCount = activePeers.Count();

                if (activeCount < options.MinActivePeers)
                {
                    currentInterval = baseInterval;
                }
                else if (activeCount >= options.MaxActivePeers)
                {
                    currentInterval = TimeSpan.FromMilliseconds(Math.Min(currentInterval.TotalMilliseconds * 2, options.MaxDiscoveryInterval.TotalMilliseconds));
                }
                else
                {
                    currentInterval = TimeSpan.FromMilliseconds(Math.Min(currentInterval.TotalMilliseconds * 1.5, options.MaxDiscoveryInterval.TotalMilliseconds));
                }

                if (currentInterval < baseInterval)
                {
                    currentInterval = baseInterval;
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Unexpected error in central peer discovery polling loop.", meshId);
            }

            if (cancellationToken.IsCancellationRequested) break;

            try
            {
                await Task.Delay(currentInterval, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }

    private sealed class MeshState : IDisposable
    {
        public string MeshId { get; }
        public IApplicationPayloadDispatcher Dispatcher { get; }
        public IFailureDetector FailureDetector { get; }
        public IPeerRegistry PeerRegistry { get; }

        public Task? HealthCheckLoopTask { get; set; }
        public Task? DiscoveryLoopTask { get; set; }
        public CancellationTokenSource? LoopCts { get; set; }

        public MeshState(string meshId, IApplicationPayloadDispatcher dispatcher, IFailureDetector failureDetector, IPeerRegistry peerRegistry)
        {
            MeshId = meshId;
            Dispatcher = dispatcher;
            FailureDetector = failureDetector;
            PeerRegistry = peerRegistry;
        }

        public void Dispose()
        {
            if (LoopCts is not null)
            {
                if (!LoopCts.IsCancellationRequested)
                {
                    LoopCts.Cancel();
                }

                LoopCts.Dispose();
            }
        }
    }
}