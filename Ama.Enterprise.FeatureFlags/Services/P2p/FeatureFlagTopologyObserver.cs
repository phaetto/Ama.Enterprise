namespace Ama.Enterprise.FeatureFlags.Services.P2p;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.FeatureFlags.Models.P2p;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observes P2P network topology changes and triggers a DVV synchronization when connecting to the first peer.
/// </summary>
public sealed class FeatureFlagTopologyObserver : IPeerTopologyObserver
{
    private readonly FeatureFlagCrdtScopeProvider scopeProvider;
    private readonly IP2pProtocol p2pProtocol;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<FeatureFlagTopologyObserver> logger;
    private int hasConnected;

    public FeatureFlagTopologyObserver(
        FeatureFlagCrdtScopeProvider scopeProvider,
        IP2pProtocol p2pProtocol,
        ICrdtSerializer serializer,
        ILogger<FeatureFlagTopologyObserver> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.p2pProtocol = p2pProtocol ?? throw new ArgumentNullException(nameof(p2pProtocol));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task OnPeerJoinedAsync(PeerNode node, CancellationToken cancellationToken)
    {
        // Only trigger on the very first successful connection
        if (Interlocked.CompareExchange(ref hasConnected, 1, 0) == 0)
        {
            logger.LogInformation("Connected to first peer {PeerId}. Triggering immediate DVV state sync.", node.Id);

            try
            {
                var clusterManager = scopeProvider.Scope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();
                var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();

                var state = clusterManager.GetLocalState();
                var syncMsg = new FeatureFlagStateSyncMessage(replicaContext.ReplicaId, state);
                
                var payload = serializer.SerializeToBytes(syncMsg);
                var wrapper = new FeatureFlagMessageWrapper("FeatureFlagSync", payload);
                var finalBytes = serializer.SerializeToBytes(wrapper);

                await p2pProtocol.BroadcastAsync(finalBytes, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to broadcast initial DVV state sync upon connecting to first peer.");
            }
        }
    }

    /// <inheritdoc />
    public Task OnPeerDepartedAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task OnPeerStatusChangedAsync(PeerId peerId, PeerStatus newStatus, CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}