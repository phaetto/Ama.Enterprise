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
    private readonly FeatureFlagCrdtScopeProvider scopeProvider;
    private readonly IGossipProtocol gossipProtocol;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<FeatureFlagAntiEntropyService> logger;

    public FeatureFlagAntiEntropyService(
        FeatureFlagCrdtScopeProvider scopeProvider,
        IGossipProtocol gossipProtocol,
        ICrdtSerializer serializer,
        ILogger<FeatureFlagAntiEntropyService> logger)
    {
        this.scopeProvider = scopeProvider ?? throw new ArgumentNullException(nameof(scopeProvider));
        this.gossipProtocol = gossipProtocol ?? throw new ArgumentNullException(nameof(gossipProtocol));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Retrieve cluster manager from the centralized long-lived scope
        var clusterManager = scopeProvider.Scope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();
        var replicaContext = scopeProvider.Scope.ServiceProvider.GetRequiredService<ReplicaContext>();

        // Using SemaphoreSlim to safely signal and bypass the regular anti-entropy delay
        using var syncSignal = new SemaphoreSlim(0, 1);

        void OnStateChanged(object? sender, EventArgs e)
        {
            try
            {
                if (syncSignal.CurrentCount == 0)
                {
                    syncSignal.Release();
                }
            }
            catch (ObjectDisposedException)
            {
                // Ignore if firing concurrently during graceful shutdown disposal
            }
        }

        clusterManager.LocalStateChanged += OnStateChanged;

        try
        {
            // Initial delay allowing node discovery and initialization to settle before generating network traffic
            await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken).ConfigureAwait(false);

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
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

                try
                {
                    // Wait 15 seconds, or wake up immediately if a local state change occurs while Active Sync is enabled
                    await syncSignal.WaitAsync(TimeSpan.FromSeconds(15), stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    // Graceful cancellation triggered during waiting.
                    break;
                }
            }
        }
        finally
        {
            clusterManager.LocalStateChanged -= OnStateChanged;
        }
    }
}