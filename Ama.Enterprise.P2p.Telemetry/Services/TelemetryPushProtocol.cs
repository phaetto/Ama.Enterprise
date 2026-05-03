namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Telemetry.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Protocol implementation dedicated to pushing telemetry messages across the configured telemetry mesh natively without extraneous forwarding overhead.
/// </summary>
public sealed class TelemetryPushProtocol : IP2pProtocol, IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IOptionsMonitor<TelemetryOptions> telemetryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly ILogger<TelemetryPushProtocol> logger;

    private CancellationTokenSource? loopCts;
    private Task? inboundLoopTask;
    private Task? healthCheckLoopTask;

    private ITransportRouter? transportRouter;
    private IInboundMessageQueue<GossipMessage>? inboundQueue;
    private IApplicationPayloadDispatcher? dispatcher;
    private IFailureDetector? failureDetector;
    private IPeerRegistry? peerRegistry;

    private bool isStarted;

    /// <summary>
    /// Initializes a new instance of the <see cref="TelemetryPushProtocol"/> class.
    /// </summary>
    public TelemetryPushProtocol(
        IServiceProvider serviceProvider,
        IOptionsMonitor<TelemetryOptions> telemetryOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        ILogger<TelemetryPushProtocol> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.telemetryOptionsMonitor = telemetryOptionsMonitor ?? throw new ArgumentNullException(nameof(telemetryOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        if (isStarted)
        {
            return Task.CompletedTask;
        }

        var options = telemetryOptionsMonitor.CurrentValue;
        var meshId = options.TargetMeshId;

        if (!options.IsEnabled)
        {
            return Task.CompletedTask;
        }

        logger.LogInformation("[{MeshId}] Starting Telemetry Push Protocol...", meshId);

        transportRouter = serviceProvider.GetRequiredKeyedService<ITransportRouter>(meshId);
        inboundQueue = serviceProvider.GetRequiredKeyedService<IInboundMessageQueue<GossipMessage>>(meshId);
        dispatcher = serviceProvider.GetRequiredKeyedService<IApplicationPayloadDispatcher>(meshId);
        failureDetector = serviceProvider.GetRequiredKeyedService<IFailureDetector>(meshId);
        peerRegistry = serviceProvider.GetRequiredService<IPeerRegistry>();

        loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        inboundLoopTask = Task.Run(() => ProcessInboundQueueAsync(meshId, loopCts.Token), loopCts.Token);
        healthCheckLoopTask = Task.Run(() => HealthCheckLoopAsync(meshId, loopCts.Token), loopCts.Token);

        isStarted = true;
        logger.LogInformation("[{MeshId}] Telemetry Push Protocol started.", meshId);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (!isStarted)
        {
            return;
        }

        var options = telemetryOptionsMonitor.CurrentValue;
        logger.LogInformation("[{MeshId}] Stopping Telemetry Push Protocol...", options.TargetMeshId);

        if (loopCts is not null)
        {
            await loopCts.CancelAsync().ConfigureAwait(false);
        }

        if (inboundLoopTask is not null)
        {
            try
            {
                await inboundLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        if (healthCheckLoopTask is not null)
        {
            try
            {
                await healthCheckLoopTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }

        Dispose();
        isStarted = false;
    }

    /// <inheritdoc />
    public async Task BroadcastAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (!isStarted || payload.IsEmpty || transportRouter is null || peerRegistry is null)
        {
            return;
        }

        var options = telemetryOptionsMonitor.CurrentValue;
        var meshId = options.TargetMeshId;
        var nodeOptions = nodeOptionsMonitor.Get(meshId);

        if (nodeOptions is null || nodeOptions.LocalPeerId == Guid.Empty)
        {
            return;
        }

        var message = new GossipMessage(
            meshId,
            Ama.Enterprise.P2p.Constants.ProtocolVersion,
            Guid.NewGuid(),
            new PeerId(nodeOptions.LocalPeerId),
            1,
            payload);

        var peers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
        var peerList = peers.ToList();
        if (peerList.Count == 0)
        {
            return;
        }

        var sendTasks = new List<Task>(peerList.Count);
        foreach (var peer in peerList)
        {
            sendTasks.Add(SendToPeerAsync(peer, message, cancellationToken));
        }

        await Task.WhenAll(sendTasks).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (loopCts is not null)
        {
            if (!loopCts.IsCancellationRequested)
            {
                loopCts.Cancel();
            }

            loopCts.Dispose();
            loopCts = null;
        }
    }

    private async Task SendToPeerAsync(PeerNode peer, GossipMessage message, CancellationToken cancellationToken)
    {
        try
        {
            if (transportRouter is not null)
            {
                await transportRouter.SendAsync(peer.Endpoint, message, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to push telemetry payload to peer {PeerId}.", peer.Id.Value);
        }
    }

    private async Task ProcessInboundQueueAsync(string meshId, CancellationToken cancellationToken)
    {
        if (inboundQueue is null || dispatcher is null || failureDetector is null)
        {
            return;
        }

        try
        {
            await foreach (var message in inboundQueue.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    await failureDetector.RecordHeartbeatAsync(message.SenderId, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogWarning(ex, "[{MeshId}] Failed to record heartbeat for peer {PeerId}.", meshId, message.SenderId.Value);
                }

                try
                {
                    await dispatcher.DispatchAsync(message.MeshId, message.SenderId, message.Payload, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[{MeshId}] Error pushing unwrapped telemetry payload into local domain {MessageId}.", meshId, message.MessageId);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] An error occurred while processing the inbound message queue.", meshId);
        }
    }

    private async Task HealthCheckLoopAsync(string meshId, CancellationToken cancellationToken)
    {
        if (peerRegistry is null || failureDetector is null)
        {
            return;
        }

        var checkInterval = TimeSpan.FromSeconds(5);
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(checkInterval, cancellationToken).ConfigureAwait(false);

                var peers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
                foreach (var peer in peers)
                {
                    var health = await failureDetector.EvaluatePeerHealthAsync(peer.Id, cancellationToken).ConfigureAwait(false);

                    if (health == PeerStatus.Dead)
                    {
                        logger.LogInformation("[{MeshId}] Peer {PeerId} marked as Dead. Removing from registry.", meshId, peer.Id.Value);
                        await peerRegistry.RemovePeerAsync(meshId, peer.Id, cancellationToken).ConfigureAwait(false);
                    }
                    else if (health == PeerStatus.Suspect)
                    {
                        await peerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Suspect, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await peerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] An error occurred during health check tick.", meshId);
            }
        }
    }
}