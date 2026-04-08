namespace Ama.Enterprise.FeatureFlags.Extensions;

using System;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Decorators;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Models.P2p;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.FeatureFlags.Services.P2p;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extensions for registering feature flags CRDT components.
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

        if (configure != null)
        {
            services.Configure(configure);
        }
        else
        {
            services.Configure<FeatureFlagOptions>(_ => { });
        }

        // Register CRDT models specifically for feature flags
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(FeatureFlagsJsonContext.Default)
                .AddCrdtAotContext<FeatureFlagsCrdtAotContext>();

        services.AddCrdtSerializableType<FeatureFlag>("feature-flag");

        // Register the shared MemoryJournal as a singleton simulation for V1
        services.AddCrdtJournaling<MemoryJournal>();

        // Attach decorators for automatic journaling and compaction
        services.AddCrdtApplicatorDecorator<JournalingApplicatorDecorator>(DecoratorBehavior.After);
        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);
        services.AddCrdtApplicatorDecorator<CompactingApplicatorDecorator>(DecoratorBehavior.After);

        // Register the singleton scope provider to hold the ReplicaContext alive
        services.AddSingleton<FeatureFlagCrdtScopeProvider>();

        // Register the cluster manager
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

        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(FeatureFlagP2pJsonContext.Default);
        
        // Register the gossip handler using Keyed DI restricted to the specified mesh network
        services.AddKeyedSingleton<IMessageHandler<GossipMessage>, FeatureFlagGossipHandler>(meshId);
        
        // Register the topology observer to trigger immediate sync on first peer connection
        services.AddSingleton<IPeerTopologyObserver, FeatureFlagTopologyObserver>();
        
        services.AddHostedService<FeatureFlagAntiEntropyService>();

        return services;
    }
}