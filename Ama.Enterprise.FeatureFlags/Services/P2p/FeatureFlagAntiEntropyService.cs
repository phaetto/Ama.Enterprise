namespace Ama.Enterprise.FeatureFlags.Services.P2p;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.FeatureFlags.Models.P2p;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Background service responsible for broadcasting local state vectors periodically to synchronize feature flags across the P2P cluster.
/// </summary>
public sealed class FeatureFlagAntiEntropyService : BackgroundService
{
    private readonly IServiceScopeFactory serviceScopeFactory;
    private readonly IGossipProtocol gossipProtocol;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<FeatureFlagAntiEntropyService> logger;

    public FeatureFlagAntiEntropyService(
        IServiceScopeFactory serviceScopeFactory,
        IGossipProtocol gossipProtocol,
        ICrdtSerializer serializer,
        ILogger<FeatureFlagAntiEntropyService> logger)
    {
        this.serviceScopeFactory = serviceScopeFactory ?? throw new ArgumentNullException(nameof(serviceScopeFactory));
        this.gossipProtocol = gossipProtocol ?? throw new ArgumentNullException(nameof(gossipProtocol));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Initial delay allowing node discovery and initialization to settle before generating network traffic
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                using var scope = serviceScopeFactory.CreateScope();
                var clusterManager = scope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();
                var replicaContext = scope.ServiceProvider.GetRequiredService<ReplicaContext>();
                
                var state = clusterManager.GetLocalState();
                var syncMsg = new FeatureFlagStateSyncMessage(replicaContext.ReplicaId, state);
                
                // Use the standardized ICrdtSerializer rather than explicit System.Text.Json implementations
                var payload = serializer.SerializeToBytes(syncMsg);
                
                var wrapper = new FeatureFlagMessageWrapper("FeatureFlagSync", payload);
                var finalBytes = serializer.SerializeToBytes(wrapper);

                await gossipProtocol.BroadcastAsync(finalBytes, stoppingToken).ConfigureAwait(false);
                
                logger.LogTrace("Broadcasted local feature flag synchronization state.");
            }
            catch (OperationCanceledException)
            {
                // Graceful cancellation triggered.
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred during feature flag anti-entropy broadcast.");
            }

            await Task.Delay(TimeSpan.FromSeconds(15), stoppingToken).ConfigureAwait(false);
        }
    }
}