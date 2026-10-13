namespace Ama.Enterprise.FeatureFlags.Extensions;

using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.CRDT.Distributed.Services.P2p;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

/// <summary>
/// Extensions for registering feature flags components.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the feature flags system to the service collection.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddFeatureFlags(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Register CRDT models specifically for feature flags domain
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(FeatureFlagsJsonContext.Default)
                .AddCrdtAotContext<FeatureFlagsCrdtAotContext>();

        services.AddCrdtSerializableType<FeatureFlag>(Constants.FeatureFlagDocumentType);

        // Map domain types inside the centralized document pool
        services.AddDistributedDocumentType<FeatureFlagState>(Constants.FeatureFlagDocumentType);

        // Register the business logic domain wrapper scoped to the CRDT hierarchy
        services.AddDistributedCrdtService<IFeatureFlagClusterManager, FeatureFlagClusterManager>();

        services.AddSingleton<IDocumentStateObserver<FeatureFlagState>, FeatureFlagStateObserver>();

        // Register the background initialization service to build the global scope
        services.AddHostedService<FeatureFlagBootstrapper>();

        return services;
    }
}