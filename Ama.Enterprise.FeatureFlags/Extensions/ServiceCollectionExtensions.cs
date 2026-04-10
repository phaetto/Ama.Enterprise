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
    /// Adds the feature flags system to the service collection.
    /// </summary>
    public static IServiceCollection AddFeatureFlags(this IServiceCollection services, Action<FeatureFlagOptions>? configure = null)
    {
        if (services == null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        // Apply configuration mappings natively maintaining backwards compatibility
        services.Configure<FeatureFlagOptions>(configure ?? (_ => { }));

        Action<DistributedCrdtOptions> distConfig = dist =>
        {
            if (configure != null)
            {
                var ffOpts = new FeatureFlagOptions();
                configure(ffOpts);
                dist.ReplicaId = ffOpts.ReplicaId;
                dist.ActiveSyncEnabled = ffOpts.ActiveSyncEnabled;
            }
        };

        // Bootstrap generic core dependencies
        services.AddDistributedCrdtCore(distConfig);

        // Register CRDT models specifically for feature flags domain
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(FeatureFlagsJsonContext.Default)
                .AddCrdtAotContext<FeatureFlagsCrdtAotContext>();

        services.AddCrdtSerializableType<FeatureFlag>("feature-flag");

        // Map domain generic types inside the centralized document pool securely via explicitly inherited constraints
        services.AddDistributedDocument<FeatureFlagState>();

        // Register the business logic domain wrapper scoped exactly to the CRDT hierarchy
        services.AddScoped<IFeatureFlagClusterManager, FeatureFlagClusterManager>();

        return services;
    }

    /// <summary>
    /// Adds P2P networking support for the feature flags system to synchronize across nodes.
    /// </summary>
    public static IServiceCollection AddFeatureFlagsP2p(this IServiceCollection services, string meshId)
    {
        if (services == null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        // Delegate routing orchestrations and background services completely to the distributed core
        services.AddDistributedCrdtP2p(meshId);

        return services;
    }
}