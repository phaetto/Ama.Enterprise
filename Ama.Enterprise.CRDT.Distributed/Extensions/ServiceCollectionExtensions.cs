namespace Ama.Enterprise.CRDT.Distributed.Extensions;

using System;
using System.Diagnostics.CodeAnalysis;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Decorators;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.P2p;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Extension methods for registering completely generic distributed CRDT state logic gracefully elegantly cleanly intelligently.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Bootstraps the baseline services, scope providers, and journaling decorators required by distributed CRDT modules.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtCore(this IServiceCollection services, Action<DistributedCrdtOptions>? configure = null)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        var optionsBuilder = services.AddOptions<DistributedCrdtOptions>();

        if (configure != null)
        {
            optionsBuilder.Configure(configure);
        }

        optionsBuilder
            .Validate(options => options.CheckpointIntervalSeconds > 0, 
                "CheckpointIntervalSeconds must be greater than zero.")
            .Validate(options => options.AntiEntropyIntervalSeconds > 0, 
                "AntiEntropyIntervalSeconds must be greater than zero.")
            .Validate(options => options.AntiEntropyInitialDelaySeconds >= 0, 
                "AntiEntropyInitialDelaySeconds cannot be negative.")
            .Validate(options => options.PeerEvictionTtlSeconds == 0 || options.PeerEvictionTtlSeconds >= options.AntiEntropyIntervalSeconds * 3, 
                "PeerEvictionTtlSeconds must be at least 3 times the AntiEntropyIntervalSeconds to prevent peer topology oscillation.")
            .Validate(options => options.PeerEvictionTtlSeconds == 0 || options.PeerEvictionTtlSeconds >= options.CheckpointIntervalSeconds, 
                "PeerEvictionTtlSeconds must be greater than or equal to the CheckpointIntervalSeconds as evictions are processed during checkpoint cycles.")
            .ValidateOnStart();

        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtP2pJsonContext.Default)
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtSystemJsonContext.Default)
                .AddCrdtAotContext(new DistributedCrdtSystemAotContext())
                .AddCrdtSerializableType<CrdtRegistryEntry>("crdt-registry-entry");

        services.TryAddSingleton<IDistributedCrdtStorage, MemoryCrdtStorage>();

        services.AddCrdtJournaling<StorageJournalForwarder>();

        services.AddCrdtApplicatorDecorator<JournalingApplicatorDecorator>(DecoratorBehavior.After);
        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);
        services.AddCrdtApplicatorDecorator<CompactingApplicatorDecorator>(DecoratorBehavior.After);

        services.AddSingleton<DistributedCrdtScopeProvider>();
        
        services.AddDistributedCrdtService<ICrdtDocumentOrchestrator, CrdtDocumentOrchestrator>();
        
        services.AddSingleton<IClusterStateTracker, ClusterStateTracker>();
        services.AddSingleton<ICrdtEvictionService, CrdtEvictionService>();
        
        services.AddHostedService<CrdtInitializationService>();

        services.AddHostedService<CrdtCheckpointService>();

        return services;
    }

    /// <summary>
    /// Registers a specific AOT-compliant CRDT document type.
    /// </summary>
    public static IServiceCollection AddDistributedDocumentType<TState>(this IServiceCollection services, string typeAlias) where TState : class, IDistributedCrdtState, new()
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));

        services.AddKeyedSingleton<IDocumentFactory>(typeAlias, new DocumentFactory<TState>());
        return services;
    }

    /// <summary>
    /// Registers a generic domain service scoped securely within the dynamic CRDT lifecycle natively.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtService<TService, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TImplementation>(this IServiceCollection services) 
        where TService : class 
        where TImplementation : class, TService
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        services.AddScoped<TImplementation>();
        services.AddTransient<TService>(sp => 
            sp.GetRequiredService<DistributedCrdtScopeProvider>().Scope.ServiceProvider.GetRequiredService<TImplementation>());

        return services;
    }

    /// <summary>
    /// Injects the generic CRDT routing dispatchers and anti-entropy background processors directly into the targeted P2P Mesh pipeline securely decoupled from underlying algorithm gracefully.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtP2p(this IServiceCollection services, string meshId)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(meshId)) throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));

        services.AddKeyedSingleton<IApplicationPayloadHandler, CrdtP2pPayloadHandler>(meshId);
        services.AddSingleton<IPeerTopologyObserver, CrdtTopologyObserver>();
        services.AddHostedService<CrdtAntiEntropyService>();

        return services;
    }
}