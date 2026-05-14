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
/// Extension methods for registering distributed CRDT state logic supporting multi-replica environments.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Bootstraps the baseline services, scope managers, and journaling decorators.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtCore(this IServiceCollection services, Action<DistributedCrdtOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

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

        // Provide memory storage as the default fallback actively mapped as scoped per replica instance.
        services.TryAddScoped<IDistributedCrdtStorage, MemoryCrdtStorage>();

        services.AddCrdtJournaling<StorageJournalForwarder>();

        services.AddCrdtApplicatorDecorator<JournalingApplicatorDecorator>(DecoratorBehavior.After);
        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);
        services.AddCrdtApplicatorDecorator<CompactingApplicatorDecorator>(DecoratorBehavior.After);

        services.TryAddSingleton<IDistributedCrdtScopeFactory, DistributedCrdtScopeFactory>();
        services.TryAddSingleton<DistributedCrdtScopeManager>();

        services.AddScoped<ICrdtDocumentOrchestrator, CrdtDocumentOrchestrator>();
        services.AddScoped<IClusterStateTracker, ClusterStateTracker>();
        services.AddScoped<ICrdtEvictionService, CrdtEvictionService>();

        services.AddHostedService<CrdtInitializationService>();
        services.AddHostedService<CrdtCheckpointService>();

        return services;
    }

    /// <summary>
    /// Explicitly declares a distinct CRDT Replica registering bounding constraints generating persistent decoupled structures.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtReplica(this IServiceCollection services, string replicaId)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        services.AddSingleton(new DistributedCrdtReplicaRegistration(replicaId));
        return services;
    }

    /// <summary>
    /// Registers a specific AOT-compliant CRDT document type avoiding reflection.
    /// </summary>
    public static IServiceCollection AddDistributedDocumentType<TState>(this IServiceCollection services, string typeAlias) where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));

        services.AddKeyedSingleton<IDocumentFactory>(typeAlias, new DocumentFactory<TState>());
        return services;
    }

    /// <summary>
    /// Registers a scoped distributed CRDT application service operating within the isolated CRDT multi-mesh scope explicitly.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtService<TService, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TImplementation>(this IServiceCollection services) 
        where TService : class
        where TImplementation : class, TService
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddScoped<TService, TImplementation>();

        return services;
    }

    /// <summary>
    /// Injects generic anti-entropy multi-mesh payloads mapped tightly enforcing pure identity limits directly isolating routing domains.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtP2p(this IServiceCollection services, string meshId, string replicaId)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(meshId)) throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        services.AddKeyedSingleton<IApplicationPayloadHandler>(meshId, (sp, key) => 
            ActivatorUtilities.CreateInstance<CrdtP2pPayloadHandler>(sp, replicaId));
            
        services.AddKeyedSingleton<IPeerTopologyObserver>(meshId, (sp, key) => 
            ActivatorUtilities.CreateInstance<CrdtTopologyObserver>(sp, replicaId));
            
        services.AddHostedService<CrdtAntiEntropyService>();

        return services;
    }

    /// <summary>
    /// Registers a single unified distributed CRDT storage mechanism mapped for the entire scope, overriding the default memory fallback.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtStorage<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TStorage>(this IServiceCollection services) 
        where TStorage : class, IDistributedCrdtStorage
    {
        ArgumentNullException.ThrowIfNull(services);

        services.RemoveAll(typeof(IDistributedCrdtStorage));
        services.AddScoped<IDistributedCrdtStorage, TStorage>();
        
        return services;
    }
}