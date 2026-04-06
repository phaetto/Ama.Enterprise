namespace Ama.Enterprise.FeatureFlags.Extensions;

using System;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Decorators;
using Ama.CRDT.Services.Journaling;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extensions for registering feature flags CRDT components.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the feature flags system to the service collection.
    /// </summary>
    public static IServiceCollection AddFeatureFlags(this IServiceCollection services)
    {
        if (services == null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        // Register CRDT models specifically for feature flags
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(FeatureFlagsJsonContext.Default)
                .AddCrdtAotContext<FeatureFlagsCrdtAotContext>();

        // Register the shared MemoryJournal as a singleton simulation for V1
        services.AddSingleton<MemoryJournal>();
        services.AddSingleton<ICrdtOperationJournal>(sp => sp.GetRequiredService<MemoryJournal>());

        // Attach decorators for automatic journaling and compaction
        services.AddCrdtApplicatorDecorator<JournalingApplicatorDecorator>(DecoratorBehavior.After);
        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);
        services.AddCrdtApplicatorDecorator<CompactingApplicatorDecorator>(DecoratorBehavior.After);

        // Register the cluster manager
        services.AddScoped<IFeatureFlagClusterManager, FeatureFlagClusterManager>();

        return services;
    }
}