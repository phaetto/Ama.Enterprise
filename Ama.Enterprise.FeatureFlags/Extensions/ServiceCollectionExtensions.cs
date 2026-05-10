namespace Ama.Enterprise.FeatureFlags.Extensions;

using System;
using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extensions for registering feature flags CRDT components leveraging the distributed CRDT core.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the complete, plug-and-play feature flags system to the service collection.
    /// P2P network transports and configurations must be registered by the application independently.
    /// </summary>
    public static IServiceCollection AddFeatureFlags(this IServiceCollection services, string replicaId, Action<FeatureFlagOptions>? configure = null)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        var ffOpts = new FeatureFlagOptions();
        configure?.Invoke(ffOpts);

        // Apply configuration mappings natively maintaining backwards compatibility
        services.Configure(configure ?? (_ => { }));

        Action<DistributedCrdtOptions> distConfig = dist =>
        {
            dist.ActiveSyncEnabled = ffOpts.Crdt.ActiveSyncEnabled;
            dist.CheckpointIntervalSeconds = ffOpts.Crdt.CheckpointIntervalSeconds;
            dist.AntiEntropyInitialDelaySeconds = ffOpts.Crdt.AntiEntropyInitialDelaySeconds;
            dist.AntiEntropyIntervalSeconds = ffOpts.Crdt.AntiEntropyIntervalSeconds;
            dist.PeerEvictionTtlSeconds = ffOpts.Crdt.PeerEvictionTtlSeconds;
        };

        // Explicitly register the replica scope bounds
        services.AddDistributedCrdtReplica(replicaId);

        // Bootstrap generic core dependencies
        services.AddDistributedCrdtCore(distConfig);

        // Register CRDT models specifically for feature flags domain
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(FeatureFlagsJsonContext.Default)
                .AddCrdtAotContext<FeatureFlagsCrdtAotContext>();

        services.AddCrdtSerializableType<FeatureFlag>("feature-flag");

        // Map domain generic types inside the centralized document pool securely via explicitly inherited constraints
        services.AddDistributedDocumentType<FeatureFlagState>("feature-flag");

        // Register the business logic domain wrapper scoped exactly to the CRDT hierarchy via transparent forwarder natively
        services.AddDistributedCrdtService<IFeatureFlagClusterManager, FeatureFlagClusterManager>();

        // Register the background initialization service to securely build the global scope natively
        services.AddHostedService<FeatureFlagBootstrapper>();

        // Delegate routing orchestrations and background services completely to the distributed core explicitly bounded
        services.AddDistributedCrdtP2p(ffOpts.InternalMeshId, replicaId);

        return services;
    }
}