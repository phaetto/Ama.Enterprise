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
public sealed class TelemetryPushProtocol : IDisposable
{
    private readonly IServiceProvider serviceProvider;
    private readonly IOptionsMonitor<TelemetryOptions> telemetryOptionsMonitor;
    private readonly IDirectMessageSender directMessageSender;
    private readonly ILogger<TelemetryPushProtocol> logger;

    private CancellationTokenSource? loopCts;
    private Task? healthCheckLoopTask;

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
        IDirectMessageSender directMessageSender,
        ILogger<TelemetryPushProtocol> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.telemetryOptionsMonitor = telemetryOptionsMonitor ?? throw new ArgumentNullException(nameof(telemetryOptionsMonitor));
        this.directMessageSender = directMessageSender ?? throw new ArgumentNullException(nameof(directMessageSender));
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

        dispatcher = serviceProvider.GetRequiredKeyedService<IApplicationPayloadDispatcher>(meshId);
        failureDetector = serviceProvider.GetRequiredKeyedService<IFailureDetector>(meshId);
        peerRegistry = serviceProvider.GetRequiredService<IPeerRegistry>();

        loopCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

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
        if (!isStarted || payload.IsEmpty || peerRegistry is null)
        {
            return;
        }

        var options = telemetryOptionsMonitor.CurrentValue;
        var meshId = options.TargetMeshId;

        var peers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
        var peerList = peers.ToList();
        if (peerList.Count == 0)
        {
            return;
        }

        var sendTasks = new List<Task>(peerList.Count);
        foreach (var peer in peerList)
        {
            sendTasks.Add(SendToPeerAsync(meshId, peer.Id, payload, cancellationToken));
        }

        await Task.WhenAll(sendTasks).ConfigureAwait(false);
    }

    /// <summary>
    /// Processes an incoming generic mesh message delegating it to the underlying concrete protocol algorithm.
    /// </summary>
    /// <param name="message">The incoming mapped protocol message.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous processing operation.</returns>
    public Task ProcessMessageAsync(IMeshMessage message, CancellationToken cancellationToken)
    {
        if (!isStarted || dispatcher is null || failureDetector is null)
        {
            return Task.CompletedTask;
        }

        if (message is GossipMessage gossipMsg)
        {
            return ProcessInboundMessageAsync(gossipMsg, cancellationToken);
        }

        return Task.CompletedTask;
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

    private async Task SendToPeerAsync(string meshId, PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        try
        {
            await directMessageSender.SendDirectAsync(meshId, targetPeerId, payload, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to push telemetry payload to peer {PeerId} on explicit mesh {MeshId}.", targetPeerId.Value, meshId);
        }
    }

    private async Task ProcessInboundMessageAsync(GossipMessage message, CancellationToken cancellationToken)
    {
        try
        {
            await failureDetector!.RecordHeartbeatAsync(message.SenderId, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to record heartbeat for peer {PeerId}.", message.MeshId, message.SenderId.Value);
        }

        try
        {
            await dispatcher!.DispatchAsync(message.MeshId, message.SenderId, message.Payload, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error pushing unwrapped telemetry payload into local domain {MessageId}.", message.MeshId, message.MessageId);
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