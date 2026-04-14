namespace Ama.Enterprise.CRDT.Distributed.Extensions;

using System;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Decorators;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.P2p;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Extension methods for registering completely generic distributed CRDT state logic.
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

        // Validate the options rigorously to prevent systemic oscillations and side effects
        // effectively bounded by strictly validated topology rules.
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

        // Register default memory storage only if a custom one hasn't been provided previously
        services.TryAddSingleton<IDistributedCrdtStorage, MemoryCrdtStorage>();

        // Wire up the forwarder so that the underlying pipeline seamlessly uses the unified storage interface
        services.AddCrdtJournaling<StorageJournalForwarder>();

        services.AddCrdtApplicatorDecorator<JournalingApplicatorDecorator>(DecoratorBehavior.After);
        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);
        services.AddCrdtApplicatorDecorator<CompactingApplicatorDecorator>(DecoratorBehavior.After);

        services.AddSingleton<DistributedCrdtScopeProvider>();
        
        // Register core dynamic AOT-safe generic orchestrator seamlessly
        services.AddScoped<ICrdtDocumentOrchestrator, CrdtDocumentOrchestrator>();
        
        // Register internal tracking singletons strictly matching background orchestration loops explicitly
        services.AddSingleton<IClusterStateTracker, ClusterStateTracker>();
        services.AddSingleton<ICrdtEvictionService, CrdtEvictionService>();
        
        // Ensures documents initialize their states from their registered persistence providers eagerly on startup
        services.AddHostedService<CrdtInitializationService>();

        // Orchestrates periodic saves ensuring underlying snapshots seamlessly offload persistent writes out-of-band directly alongside combined secure journal trimming bounds
        services.AddHostedService<CrdtCheckpointService>();

        return services;
    }

    /// <summary>
    /// Explicitly securely registers a specific natively fully AOT compliant CRDT type correctly perfectly intelligently cleanly natively logically smoothly properly mapping across the active global bounds seamlessly accurately effortlessly gracefully effortlessly seamlessly successfully effortlessly naturally efficiently effectively cleanly explicitly.
    /// </summary>
    public static IServiceCollection AddDistributedDocumentType<TState>(this IServiceCollection services, string typeAlias) where TState : class, IDistributedCrdtState, new()
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));

        services.AddKeyedSingleton<IDocumentFactory>(typeAlias, new DocumentFactory<TState>());
        return services;
    }

    /// <summary>
    /// Injects the generic CRDT routing dispatchers and anti-entropy background processors directly into the targeted P2P Mesh pipeline.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtP2p(this IServiceCollection services, string meshId)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(meshId)) throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));

        services.AddKeyedSingleton<IMessageHandler<GossipMessage>, CrdtGossipHandler>(meshId);
        services.AddSingleton<IPeerTopologyObserver, CrdtTopologyObserver>();
        services.AddHostedService<CrdtAntiEntropyService>();

        return services;
    }
}