namespace Ama.Enterprise.FeatureFlags.Services.P2p;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Models.P2p;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Message handler that bridges generic gossip messages into the feature flags CRDT domain.
/// </summary>
public sealed class FeatureFlagGossipHandler : IMessageHandler<GossipMessage>
{
    private readonly ICrdtScopeFactory crdtScopeFactory;
    private readonly FeatureFlagOptions options;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<FeatureFlagGossipHandler> logger;

    public FeatureFlagGossipHandler(
        ICrdtScopeFactory crdtScopeFactory,
        IOptions<FeatureFlagOptions> options,
        ICrdtSerializer serializer,
        ILogger<FeatureFlagGossipHandler> logger)
    {
        this.crdtScopeFactory = crdtScopeFactory ?? throw new ArgumentNullException(nameof(crdtScopeFactory));
        this.options = options?.Value ?? throw new ArgumentNullException(nameof(options));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task HandleAsync(GossipMessage message, CancellationToken cancellationToken)
    {
        if (message.Payload.IsEmpty)
        {
            return;
        }

        FeatureFlagMessageWrapper wrapper;
        try
        {
            // Converting to Array to ensure compatibility with ICrdtSerializer interface which takes byte[]
            wrapper = serializer.DeserializeFromBytes<FeatureFlagMessageWrapper>(message.Payload.ToArray());
        }
        catch (Exception)
        {
            // Ignored safely. The message belongs to another system utilizing the shared Gossip protocol.
            // Catching generic Exception since ICrdtSerializer abstracts away the underlying serialization exceptions (e.g. JsonException).
            return;
        }

        if (string.IsNullOrEmpty(wrapper.MessageType) || wrapper.Payload == null)
        {
            return;
        }

        if (wrapper.MessageType == "FeatureFlagSync")
        {
            await ProcessStateSyncAsync(wrapper.Payload, cancellationToken).ConfigureAwait(false);
        }
        else if (wrapper.MessageType == "FeatureFlagOps")
        {
            await ProcessOperationsAsync(wrapper.Payload, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task ProcessStateSyncAsync(byte[] payload, CancellationToken cancellationToken)
    {
        try
        {
            var syncMsg = serializer.DeserializeFromBytes<FeatureFlagStateSyncMessage>(payload);
            
            // Generate a valid CRDT scope matching the current node ReplicaId
            using var scope = crdtScopeFactory.CreateScope(options.ReplicaId);
            var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
            var clusterManager = scope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();
            
            // Resolve IGossipProtocol from the scope here instead of constructor to break the circular dependency.
            var gossipProtocol = scope.ServiceProvider.GetRequiredService<IGossipProtocol>();

            if (string.IsNullOrEmpty(syncMsg.ReplicaId) || syncMsg.ReplicaId == replicaContext.ReplicaId || syncMsg.State == null)
            {
                return;
            }

            var missingOps = await clusterManager.GetMissingOperationsAsync(syncMsg.ReplicaId, syncMsg.State, cancellationToken).ConfigureAwait(false);
            
            if (missingOps.Count > 0)
            {
                logger.LogDebug("Sending {Count} missing operations to replica {ReplicaId}", missingOps.Count, syncMsg.ReplicaId);
                
                var opsMsg = new FeatureFlagOperationsMessage(replicaContext.ReplicaId, missingOps.ToArray());
                var opsPayload = serializer.SerializeToBytes(opsMsg);
                
                var replyWrapper = new FeatureFlagMessageWrapper("FeatureFlagOps", opsPayload);
                var replyBytes = serializer.SerializeToBytes(replyWrapper);

                await gossipProtocol.BroadcastAsync(replyBytes, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process incoming FeatureFlagSync message.");
        }
    }

    private async Task ProcessOperationsAsync(byte[] payload, CancellationToken cancellationToken)
    {
        try
        {
            var opsMsg = serializer.DeserializeFromBytes<FeatureFlagOperationsMessage>(payload);
            
            if (opsMsg.Operations == null || opsMsg.Operations.Length == 0)
            {
                return;
            }

            using var scope = crdtScopeFactory.CreateScope(options.ReplicaId);
            var clusterManager = scope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();

            logger.LogDebug("Applying {Count} incoming operations from replica {ReplicaId}", opsMsg.Operations.Length, opsMsg.ReplicaId);
            
            await clusterManager.ApplyOperationsAsync(opsMsg.Operations, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process incoming FeatureFlagOps message.");
        }
    }
}