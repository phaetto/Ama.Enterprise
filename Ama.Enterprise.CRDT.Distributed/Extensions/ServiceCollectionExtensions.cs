namespace Ama.Enterprise.CRDT.Distributed.Extensions;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Models.Intents.Decorators;
using Ama.CRDT.Services.Decorators;
using Ama.CRDT.Services.GarbageCollection;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.CRDT.Distributed.Services.P2p;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

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
            .Validate(options => options.MaintenanceIntervalSeconds > 0, 
                "MaintenanceIntervalSeconds must be greater than zero.")
            .Validate(options => options.AntiEntropyIntervalSeconds > 0, 
                "AntiEntropyIntervalSeconds must be greater than zero.")
            .Validate(options => options.AntiEntropyInitialDelaySeconds >= 0, 
                "AntiEntropyInitialDelaySeconds cannot be negative.")
            .Validate(options => options.AntiEntropyInitialMaxJitterMilliseconds >= 0, 
                "AntiEntropyInitialMaxJitterMilliseconds cannot be negative.")
            .Validate(options => options.AntiEntropyStartupDurationSeconds >= 0, 
                "AntiEntropyStartupDurationSeconds cannot be negative.")
            .Validate(options => options.AntiEntropyStartupIntervalSeconds > 0, 
                "AntiEntropyStartupIntervalSeconds must be greater than zero.")
            .Validate(options => options.PeerEvictionTtlSeconds == 0 || options.PeerEvictionTtlSeconds >= options.AntiEntropyIntervalSeconds * 3, 
                "PeerEvictionTtlSeconds must be at least 3 times the AntiEntropyIntervalSeconds to prevent peer topology oscillation.")
            .Validate(options => options.PeerEvictionTtlSeconds == 0 || options.PeerEvictionTtlSeconds >= options.MaintenanceIntervalSeconds, 
                "PeerEvictionTtlSeconds must be greater than or equal to the MaintenanceIntervalSeconds as evictions are processed during maintenance cycles.")
            .Validate(options => options.JournalSoftTrimThreshold >= 0,
                "JournalSoftTrimThreshold cannot be negative.")
            .Validate(options => options.JournalHardTrimThreshold >= 0,
                "JournalHardTrimThreshold cannot be negative.")
            .Validate(options => options.CompactionTtlSeconds >= 0, 
                "CompactionTtlSeconds cannot be negative.")
            .Validate(options => options.ShardReplicationFactor > 0,
                "ShardReplicationFactor must be greater than zero.")
            .Validate(options => options.VirtualNodesPerReplica > 0,
                "VirtualNodesPerReplica must be greater than zero.")
            .Validate(options => options.RpcTimeoutMilliseconds > 0,
                "RpcTimeoutMilliseconds must be greater than zero.")
            .Validate(options => options.RpcMaxRetries > 0,
                "RpcMaxRetries must be greater than zero.")
            .Validate(options => options.RpcRetryDelayMilliseconds >= 0,
                "RpcRetryDelayMilliseconds cannot be negative.")
            .ValidateOnStart();

        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtP2pJsonContext.Default)
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtSystemJsonContext.Default)
                .AddCrdtAotContext(new DistributedCrdtSystemAotContext())
                .AddCrdtSerializableType<CrdtRegistryEntry>("crdt-registry-entry")
                .AddCrdtJsonModifier(ConfigureOperationIntentPolymorphism);

        // Provide memory storage as the default fallback mapped as scoped per replica instance.
        services.TryAddScoped<IDistributedCrdtStorage, MemoryCrdtStorage>();

        services.AddCrdtJournaling<StorageJournalForwarder>();

        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);
        services.AddCrdtApplicatorDecorator<CompactingApplicatorDecorator>(DecoratorBehavior.After);
        services.AddCrdtMergerDecorator<CompactingMergerDecorator>(DecoratorBehavior.After);

        services.AddCrdtCompactionPolicyFactory(sp =>
        {
            var options = sp.GetRequiredService<IOptions<DistributedCrdtOptions>>().Value;
            if (options.CompactionTtlSeconds > 0)
            {
                return new ThresholdCompactionPolicyFactory(
                    TimeSpan.FromSeconds(options.CompactionTtlSeconds),
                    sp.GetRequiredService<ICrdtTimestampProvider>());
            }

            // Disabled compaction (threshold is minimal possible timestamp, never compacts)
            return new ThresholdCompactionPolicyFactory(() => new CrdtTimestamp(long.MinValue));
        });

        services.TryAddSingleton<IDistributedCrdtTelemetry, DistributedCrdtTelemetry>();
        services.TryAddSingleton<IDistributedCrdtScopeFactory, DistributedCrdtScopeFactory>();
        services.TryAddSingleton<DistributedCrdtScopeManager>();

        services.TryAddScoped<IScopeTopologyProvider, DefaultScopeTopologyProvider>();
        services.TryAddScoped<IDocumentCommandPipeline, DocumentCommandPipeline>();
        
        // Register Consistent Hashing Distribution mappings
        services.TryAddScoped<IShardDistributionManager, ConsistentHashShardDistributionManager>();
        services.TryAddScoped<IShardVersionVectorManager, ShardVersionVectorManager>();

        services.AddScoped<ICrdtDocumentOrchestrator, CrdtDocumentOrchestrator>();
        services.AddScoped<IClusterStateTracker, ClusterStateTracker>();
        services.AddScoped<ICrdtEvictionService, CrdtEvictionService>();

        DecorateShardIdProviderResolutions(services);
        
        // Register topology observer
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IClusterStateObserver, OrchestratorClusterStateObserver>());
        
        // Register decoupled broadcast evaluations
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IDocumentPatchObserver, CrdtPatchBroadcaster>());

        services.AddHostedService<CrdtInitializationService>();
        services.AddHostedService<CrdtCheckpointService>();
        services.AddHostedService<CrdtMaintenanceService>();

        return services;
    }

    /// <summary>
    /// Declares a distinct CRDT Replica registering bounding constraints.
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

        var factory = new DocumentFactory<TState>(typeAlias);
        services.AddKeyedSingleton<IDocumentFactory>(typeAlias, factory);
        services.AddSingleton<IDocumentFactory>(factory);
        return services;
    }

    /// <summary>
    /// Registers a scoped distributed CRDT application service operating within the isolated CRDT multi-mesh scope.
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
    /// Injects generic anti-entropy multi-mesh payloads mapped to a specific replica and mesh.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtP2p(this IServiceCollection services, string meshId, string replicaId)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(meshId)) throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));

        services.AddSingleton(new DistributedCrdtP2pRegistration(meshId, replicaId));

        services.AddKeyedSingleton<IApplicationPayloadHandler>(meshId, (sp, key) => 
            ActivatorUtilities.CreateInstance<CrdtP2pPayloadHandler>(sp, replicaId));
            
        services.AddSingleton<IPeerTopologyObserver>(sp => 
            ActivatorUtilities.CreateInstance<CrdtTopologyObserver>(sp, meshId, replicaId));
            
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

    private static void ConfigureOperationIntentPolymorphism(JsonTypeInfo typeInfo)
    {
        if (typeInfo.Type != typeof(IOperationIntent))
        {
            return;
        }

        typeInfo.PolymorphismOptions ??= new JsonPolymorphismOptions
        {
            TypeDiscriminatorPropertyName = "$type",
            IgnoreUnrecognizedTypeDiscriminators = true,
            UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FailSerialization
        };

        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(AddIntent), "intent-add");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(AddVertexIntent), "intent-add-vertex");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(AddEdgeIntent), "intent-add-edge");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(ClearIntent), "intent-clear");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(EpochClearIntent), "intent-epoch-clear");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(IncrementIntent), "intent-increment");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(InsertIntent), "intent-insert");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(MapIncrementIntent), "intent-map-increment");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(MapRemoveIntent), "intent-map-remove");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(MapSetIntent), "intent-map-set");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(RemoveEdgeIntent), "intent-remove-edge");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(RemoveIntent), "intent-remove");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(RemoveValueIntent), "intent-remove-value");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(RemoveVertexIntent), "intent-remove-vertex");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(SetIndexIntent), "intent-set-index");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(SetIntent), "intent-set");
        AddDerivedIntentType(typeInfo.PolymorphismOptions, typeof(VoteIntent), "intent-vote");
    }

    private static void AddDerivedIntentType(JsonPolymorphismOptions options, Type derivedType, string discriminator)
    {
        if (!options.DerivedTypes.Any(dt => dt.DerivedType == derivedType))
        {
            options.DerivedTypes.Add(new JsonDerivedType(derivedType, discriminator));
        }
    }

    [UnconditionalSuppressMessage("Trimming", "IL2067", Justification = "Target types are already registered in the DI container with preserved constructors.")]
    private static void DecorateShardIdProviderResolutions(IServiceCollection services)
    {
        var shardProviderDescriptor = services.LastOrDefault(d => d.ServiceType == typeof(IShardIdProvider));
        if (shardProviderDescriptor != null)
        {
            services.Remove(shardProviderDescriptor);
            services.Add(ServiceDescriptor.Describe(
                typeof(IShardIdProvider),
                sp =>
                {
                    var inner = shardProviderDescriptor.ImplementationInstance as IShardIdProvider
                        ?? (shardProviderDescriptor.ImplementationFactory != null
                            ? (IShardIdProvider)shardProviderDescriptor.ImplementationFactory(sp)
                            : (IShardIdProvider)ActivatorUtilities.CreateInstance(sp, shardProviderDescriptor.ImplementationType!));
                    return inner as SystemRegistryShardIdProviderDecorator
                        ?? new SystemRegistryShardIdProviderDecorator(inner);
                },
                shardProviderDescriptor.Lifetime));
        }

        for (int i = 0; i < services.Count; i++)
        {
            var descriptor = services[i];
            if (descriptor.IsKeyedService ||
                descriptor.ServiceType == typeof(IShardIdProvider) ||
                descriptor.ServiceType.IsGenericTypeDefinition ||
                descriptor.ImplementationInstance != null ||
                descriptor.ServiceType.Namespace == null ||
                !descriptor.ServiceType.Namespace.StartsWith("Ama.", StringComparison.Ordinal))
            {
                continue;
            }

            if (descriptor.ImplementationFactory != null)
            {
                var originalFactory = descriptor.ImplementationFactory;
                services[i] = ServiceDescriptor.Describe(
                    descriptor.ServiceType,
                    sp => originalFactory(new SystemRegistryServiceProvider(sp)),
                    descriptor.Lifetime);
            }
            else if (descriptor.ImplementationType != null)
            {
                var implementationType = descriptor.ImplementationType;
                services[i] = ServiceDescriptor.Describe(
                    descriptor.ServiceType,
                    sp => ActivatorUtilities.CreateInstance(new SystemRegistryServiceProvider(sp), implementationType),
                    descriptor.Lifetime);
            }
        }
    }
}