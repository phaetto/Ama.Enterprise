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
using Ama.CRDT.Services.Pooling;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.CRDT.Distributed.Services.P2p;
using Ama.Enterprise.P2p.Extensions;
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
            .Validate(options => options.TopologyStabilizationDelayMilliseconds >= 0,
                "TopologyStabilizationDelayMilliseconds cannot be negative.")
            .Validate(options => options.AntiEntropyBusyQueueThreshold >= 0,
                "AntiEntropyBusyQueueThreshold cannot be negative.")
            .Validate(options => options.AntiEntropyAdaptiveDelayPerThousandItemsMilliseconds >= 0,
                "AntiEntropyAdaptiveDelayPerThousandItemsMilliseconds cannot be negative.")
            .Validate(options => options.AntiEntropyMaxAdaptiveIntervalSeconds >= options.AntiEntropyIntervalSeconds,
                "AntiEntropyMaxAdaptiveIntervalSeconds must be greater than or equal to AntiEntropyIntervalSeconds.")
            .Validate(options => options.PeerEvictionTtlSeconds == 0 || options.PeerEvictionTtlSeconds >= options.AntiEntropyIntervalSeconds * 3, 
                "PeerEvictionTtlSeconds must be at least 3 times the AntiEntropyIntervalSeconds to prevent peer topology oscillation.")
            .Validate(options => options.PeerEvictionTtlSeconds == 0 || options.PeerEvictionTtlSeconds >= options.MaintenanceIntervalSeconds, 
                "PeerEvictionTtlSeconds must be greater than or equal to the MaintenanceIntervalSeconds as evictions are processed during maintenance cycles.")
            .Validate(options => options.DynamicScopeIdleTtlSeconds >= 0,
                "DynamicScopeIdleTtlSeconds cannot be negative.")
            .Validate(options => options.JournalSoftTrimThreshold >= 0,
                "JournalSoftTrimThreshold cannot be negative.")
            .Validate(options => options.JournalHardTrimThreshold >= 0,
                "JournalHardTrimThreshold cannot be negative.")
            .Validate(options => options.CompactionTtlSeconds >= 0, 
                "CompactionTtlSeconds cannot be negative.")
            .Validate(options => options.CompactionIntervalSeconds > 0,
                "CompactionIntervalSeconds must be greater than zero.")
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
            .Validate(options => options.WalChannelCapacity > 0,
                "WalChannelCapacity must be greater than zero.")
            .Validate(options => options.WalMaxBatchSize > 0,
                "WalMaxBatchSize must be greater than zero.")
            .Validate(options => options.WalFlushIntervalMilliseconds > 0,
                "WalFlushIntervalMilliseconds must be greater than zero.")
            .Validate(options => options.ShutdownTimeoutMilliseconds > 0,
                "ShutdownTimeoutMilliseconds must be greater than zero.")
            .ValidateOnStart();

        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtP2pJsonContext.Default)
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtSystemJsonContext.Default)
                .AddCrdtAotContext(new DistributedCrdtSystemAotContext())
                .AddCrdtSerializableType<CrdtRegistryEntry>("crdt-registry-entry")
                .AddCrdtJsonModifier(ConfigureOperationIntentPolymorphism);

        // Provide memory storage as the default fallback mapped as scoped per replica instance.
        services.TryAddScoped<MemoryCrdtStorage>();
        services.TryAddScoped(sp => ResolveDecoratedStorage(sp, sp.GetRequiredService<MemoryCrdtStorage>()));

        services.AddCrdtJournaling<StorageJournalForwarder>();

        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);

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
        services.TryAddSingleton<IDocumentFactoryRegistry, DocumentFactoryRegistry>();
        services.TryAddSingleton<IDocumentCommandStripePool, DocumentCommandStripePool>();
        services.TryAddSingleton<IOrchestratorCommandStripePool, OrchestratorCommandStripePool>();
        services.TryAddSingleton<IDistributedCrdtScopeFactory, DistributedCrdtScopeFactory>();
        services.TryAddSingleton<DistributedCrdtScopeManager>();
        services.TryAddSingleton<ICrdtShutdownCoordinator, CrdtShutdownCoordinator>();
        services.TryAddSingleton<ICrdtMemoryEstimator, CrdtMemoryEstimator>();
        services.TryAddSingleton<ICrdtRpcPayloadProcessor, CrdtRpcPayloadProcessor>();
        services.TryAddSingleton<ICrdtStateSyncPayloadProcessor, CrdtStateSyncPayloadProcessor>();
        services.TryAddSingleton<CrdtTrimCoordinator>();

        services.TryAddScoped<CrdtScopeContext>();
        services.TryAddScoped<IScopeTopologyProvider, DefaultScopeTopologyProvider>();
        services.TryAddScoped<IDocumentCommandPipeline, DocumentCommandPipeline>();
        services.TryAddScoped<InMemoryWriteAheadBuffer>();
        services.TryAddScoped<DiskWriteAheadBuffer>();
        
        // Register Consistent Hashing Distribution mappings
        services.TryAddScoped<IShardDistributionManager, ConsistentHashShardDistributionManager>();
        services.TryAddScoped<IShardVersionVectorManager, ShardVersionVectorManager>();

        services.TryAddScoped<ICrdtRemoteProxyClient, CrdtRemoteProxyClient>();
        services.TryAddScoped<ICrdtMapReduceCoordinator, CrdtMapReduceCoordinator>();
        services.TryAddScoped<ICrdtAntiEntropyCoordinator, CrdtAntiEntropyCoordinator>();

        services.AddScoped<ICrdtDocumentOrchestrator, CrdtDocumentOrchestrator>();
        services.AddScoped<IClusterStateTracker, ClusterStateTracker>();
        services.AddScoped<ICrdtEvictionService, CrdtEvictionService>();

        DecorateSystemRegistryProviders(services);
        
        // Register topology and registry state observers
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IClusterStateObserver, OrchestratorClusterStateObserver>());
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IDocumentStateObserver<CrdtRegistryState>, OrchestratorRegistryStateObserver>());
        
        // Register decoupled broadcast evaluations
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IDocumentPatchObserver, CrdtPatchBroadcaster>());

        services.AddHostedService<CrdtInitializationService>();
        services.AddHostedService<CrdtJournalFlushService>();
        services.AddHostedService<CrdtCheckpointService>();
        services.AddHostedService<CrdtMaintenanceService>();
        services.AddHostedService<CrdtCompactionService>();
        services.AddHostedService<CrdtShutdownService>();

        return services;
    }

    /// <summary>
    /// Registers a CRDT replica in the global inventory and optionally binds it to one or more P2P meshes.
    /// Registration is idempotent for the same <paramref name="replicaId"/> and mesh bindings.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="replicaId">The unique identifier for the local CRDT replica.</param>
    /// <param name="meshIds">Optional P2P mesh identifiers to bind to this replica.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddDistributedCrdtReplica(this IServiceCollection services, string replicaId, params string[] meshIds)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(replicaId)) throw new ArgumentException("Replica ID cannot be null or empty.", nameof(replicaId));
        ArgumentNullException.ThrowIfNull(meshIds);

        DecorateSystemRegistryProviders(services);

        replicaId = CrdtStringPool.Shared.GetOrAdd(replicaId);

        var alreadyRegistered = services.Any(d =>
            d.ServiceType == typeof(DistributedCrdtReplicaRegistration) &&
            d.ImplementationInstance is DistributedCrdtReplicaRegistration existing &&
            string.Equals(existing.ReplicaId, replicaId, StringComparison.Ordinal));

        if (!alreadyRegistered)
        {
            services.AddSingleton(new DistributedCrdtReplicaRegistration(replicaId));
        }

        for (int i = 0; i < meshIds.Length; i++)
        {
            var meshId = meshIds[i];
            if (string.IsNullOrWhiteSpace(meshId))
            {
                throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshIds));
            }

            BindMeshToReplicaInternal(services, meshId, replicaId);
        }

        return services;
    }

    /// <summary>
    /// Registers a specific AOT-compliant CRDT document type avoiding reflection.
    /// </summary>
    /// <typeparam name="TState">The CRDT document state type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="typeAlias">The unique string alias identifying this document type.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddDistributedDocumentType<TState>(this IServiceCollection services, string typeAlias) where TState : class, new()
    {
        return services.AddDistributedDocumentType<TState>(typeAlias, residencyMode: null);
    }

    /// <summary>
    /// Registers a specific AOT-compliant CRDT document type with an explicit in-memory residency mode override.
    /// </summary>
    /// <typeparam name="TState">The CRDT document state type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="typeAlias">The unique string alias identifying this document type.</param>
    /// <param name="residencyMode">The explicit residency mode override for this document type, or <see langword="null"/> to use <see cref="DistributedCrdtOptions.DefaultDocumentResidencyMode"/>.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddDistributedDocumentType<TState>(
        this IServiceCollection services,
        string typeAlias,
        CrdtDocumentResidencyMode? residencyMode)
        where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));

        DecorateSystemRegistryProviders(services);

        typeAlias = CrdtStringPool.Shared.GetOrAdd(typeAlias);
        var factory = new DocumentFactory<TState>(typeAlias, residencyMode);
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

        DecorateSystemRegistryProviders(services);

        services.AddScoped<TService, TImplementation>();

        return services;
    }

    /// <summary>
    /// Registers an AOT-compiled distributed Map/Reduce handler that can execute in-memory
    /// computations, aggregations, and bulk mutations across local and remote CRDT shards.
    /// </summary>
    /// <typeparam name="TState">The CRDT document state type.</typeparam>
    /// <typeparam name="TInput">The serializable input DTO type.</typeparam>
    /// <typeparam name="TAccumulate">The serializable intermediate accumulator DTO type.</typeparam>
    /// <typeparam name="TResult">The final reduced result type.</typeparam>
    /// <typeparam name="THandler">The concrete Map/Reduce handler implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="jobAlias">The unique wire alias identifying this Map/Reduce job across peers.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddDistributedCrdtMapReduce<
        TState,
        TInput,
        TAccumulate,
        TResult,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IServiceCollection services,
        string jobAlias)
        where TState : class, new()
        where THandler : class, ICrdtMapReduce<TState, TInput, TAccumulate, TResult>
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(jobAlias))
        {
            throw new ArgumentException("Job alias cannot be null or empty.", nameof(jobAlias));
        }

        jobAlias = CrdtStringPool.Shared.GetOrAdd(jobAlias);

        services.TryAddScoped<THandler>();
        services.TryAddScoped<ICrdtMapReduce<TState, TInput, TAccumulate, TResult>>(
            sp => sp.GetRequiredService<THandler>());

        var executor = new CrdtMapReduceExecutor<TState, TInput, TAccumulate, TResult>(jobAlias);
        services.AddSingleton<ICrdtMapReduceExecutor>(executor);
        services.AddKeyedSingleton<ICrdtMapReduceExecutor>(jobAlias, executor);

        return services;
    }

    /// <summary>
    /// Registers an AOT-compiled distributed cluster query handler (where the intermediate accumulator
    /// and final result are the same type <typeparamref name="TResult"/>) across local and remote CRDT shards.
    /// </summary>
    /// <typeparam name="TState">The CRDT document state type.</typeparam>
    /// <typeparam name="TQuery">The serializable query request DTO type.</typeparam>
    /// <typeparam name="TResult">The serializable query result DTO type.</typeparam>
    /// <typeparam name="THandler">The concrete cluster query handler implementation type.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="queryAlias">The unique wire alias identifying this query across peers.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddDistributedCrdtQuery<
        TState,
        TQuery,
        TResult,
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] THandler>(
        this IServiceCollection services,
        string queryAlias)
        where TState : class, new()
        where THandler : class, ICrdtClusterQuery<TState, TQuery, TResult>
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(queryAlias))
        {
            throw new ArgumentException("Query alias cannot be null or empty.", nameof(queryAlias));
        }

        queryAlias = CrdtStringPool.Shared.GetOrAdd(queryAlias);

        services.TryAddScoped<THandler>();
        services.TryAddScoped<ICrdtClusterQuery<TState, TQuery, TResult>>(
            sp => sp.GetRequiredService<THandler>());
        services.TryAddScoped<ICrdtMapReduce<TState, TQuery, TResult, TResult>>(
            sp => sp.GetRequiredService<THandler>());

        var executor = new CrdtMapReduceExecutor<TState, TQuery, TResult, TResult>(queryAlias);
        services.AddSingleton<ICrdtMapReduceExecutor>(executor);
        services.AddKeyedSingleton<ICrdtMapReduceExecutor>(queryAlias, executor);

        return services;
    }

    /// <summary>
    /// Binds the current P2P mesh builder to a local CRDT replica and registers the replica in the global inventory.
    /// If <paramref name="replicaId"/> is omitted or null, <see cref="IP2pMeshBuilder.MeshId"/> is used as the replica ID.
    /// </summary>
    public static IP2pMeshBuilder BindCrdtReplica(this IP2pMeshBuilder builder, string? replicaId = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var resolvedReplicaId = string.IsNullOrWhiteSpace(replicaId) ? builder.MeshId : replicaId;
        builder.Services.AddDistributedCrdtReplica(resolvedReplicaId, builder.MeshId);

        return builder;
    }

    /// <summary>
    /// Registers a single unified distributed CRDT storage mechanism mapped for the entire scope, overriding the default memory fallback.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtStorage<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TStorage>(this IServiceCollection services) 
        where TStorage : class, IDistributedCrdtStorage
    {
        ArgumentNullException.ThrowIfNull(services);

        DecorateSystemRegistryProviders(services);

        services.RemoveAll(typeof(IDistributedCrdtStorage));
        services.TryAddScoped<TStorage>();
        services.AddScoped(sp => ResolveDecoratedStorage(sp, sp.GetRequiredService<TStorage>()));
        
        return services;
    }

    /// <summary>
    /// Registers a zero-downtime live CRDT migration bridge between a source replica/document type and a target replica/document type.
    /// Automatically wires up real-time source state observers, background backfill, and graceful drain coordination.
    /// </summary>
    /// <typeparam name="TSourceState">The source (legacy) CRDT document state type.</typeparam>
    /// <typeparam name="TTargetState">The target (new) CRDT document state type.</typeparam>
    /// <typeparam name="TMigrator">The user-implemented <see cref="ICrdtDocumentMigrator{TSourceState, TTargetState}"/>.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">Action to configure the <see cref="CrdtMigrationOptions"/>.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddDistributedCrdtMigration<TSourceState, TTargetState, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TMigrator>(
        this IServiceCollection services,
        Action<CrdtMigrationOptions> configure)
        where TSourceState : class, new()
        where TTargetState : class, new()
        where TMigrator : class, ICrdtDocumentMigrator<TSourceState, TTargetState>
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var migrationOptions = new CrdtMigrationOptions();
        configure(migrationOptions);

        if (string.IsNullOrWhiteSpace(migrationOptions.SourceReplicaId))
            throw new ArgumentException("SourceReplicaId must be configured on CrdtMigrationOptions.", nameof(configure));
        if (string.IsNullOrWhiteSpace(migrationOptions.SourceTypeAlias))
            throw new ArgumentException("SourceTypeAlias must be configured on CrdtMigrationOptions.", nameof(configure));
        if (string.IsNullOrWhiteSpace(migrationOptions.TargetReplicaId))
            throw new ArgumentException("TargetReplicaId must be configured on CrdtMigrationOptions.", nameof(configure));
        if (string.IsNullOrWhiteSpace(migrationOptions.TargetTypeAlias))
            throw new ArgumentException("TargetTypeAlias must be configured on CrdtMigrationOptions.", nameof(configure));
        if (migrationOptions.MaxConcurrency <= 0)
            throw new ArgumentOutOfRangeException(nameof(configure), "MaxConcurrency must be greater than zero.");

        var migrationId = CrdtStringPool.Shared.GetOrAdd(
            string.IsNullOrWhiteSpace(migrationOptions.MigrationId)
                ? Guid.NewGuid().ToString("N")
                : migrationOptions.MigrationId);
        migrationOptions.MigrationId = migrationId;

        services.TryAddSingleton<ICrdtDocumentMigrator<TSourceState, TTargetState>, TMigrator>();
        services.TryAddSingleton<ICrdtMigrationManager, CrdtMigrationManager>();

        // Register scoped source state and registry observers so any mutation on the source replica immediately notifies the bridge
        services.TryAddScoped<MigrationSourceStateObserver<TSourceState>>();
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IDocumentStateObserver<TSourceState>, MigrationSourceStateObserver<TSourceState>>(
            sp => sp.GetRequiredService<MigrationSourceStateObserver<TSourceState>>()));
        services.TryAddEnumerable(ServiceDescriptor.Scoped<IDocumentStateObserver<CrdtRegistryState>, MigrationSourceStateObserver<TSourceState>.RegistryStateObserverAdapter>(
            sp => sp.GetRequiredService<MigrationSourceStateObserver<TSourceState>>().RegistryObserver));
        services.TryAddEnumerable(ServiceDescriptor.Scoped<ICrdtOrchestratorObserver, MigrationSourceStateObserver<TSourceState>>(
            sp => sp.GetRequiredService<MigrationSourceStateObserver<TSourceState>>()));

        services.AddKeyedSingleton(
            migrationId,
            (sp, _) => ActivatorUtilities.CreateInstance<CrdtMigrationBridgeService<TSourceState, TTargetState>>(sp, migrationOptions));

        services.AddSingleton<ICrdtMigrationBridge>(sp =>
            sp.GetRequiredKeyedService<CrdtMigrationBridgeService<TSourceState, TTargetState>>(migrationId));

        services.AddSingleton< Microsoft.Extensions.Hosting.IHostedService>(sp =>
            sp.GetRequiredKeyedService<CrdtMigrationBridgeService<TSourceState, TTargetState>>(migrationId));

        return services;
    }

    private static void BindMeshToReplicaInternal(IServiceCollection services, string meshId, string replicaId)
    {
        meshId = CrdtStringPool.Shared.GetOrAdd(meshId);

        var alreadyBound = services.Any(d =>
            d.ServiceType == typeof(DistributedCrdtP2pRegistration) &&
            d.ImplementationInstance is DistributedCrdtP2pRegistration existing &&
            string.Equals(existing.MeshId, meshId, StringComparison.Ordinal) &&
            string.Equals(existing.ReplicaId, replicaId, StringComparison.Ordinal));

        if (alreadyBound)
        {
            return;
        }

        services.AddSingleton(new DistributedCrdtP2pRegistration(meshId, replicaId));

        services.AddKeyedSingleton<IApplicationPayloadHandler>(meshId, (sp, _) =>
            ActivatorUtilities.CreateInstance<CrdtP2pPayloadHandler>(sp, replicaId));

        services.AddSingleton<IPeerTopologyObserver>(sp =>
            ActivatorUtilities.CreateInstance<CrdtTopologyObserver>(sp, meshId, replicaId));

        services.AddHostedService<CrdtAntiEntropyService>();
    }

    private static IDistributedCrdtStorage ResolveDecoratedStorage(IServiceProvider sp, IDistributedCrdtStorage innerStorage)
    {
        if (innerStorage is BufferedDistributedCrdtStorageDecorator)
        {
            return innerStorage;
        }

        var options = sp.GetRequiredService<IOptions<DistributedCrdtOptions>>().Value;
        return options.JournalMode switch
        {
            CrdtJournalMode.Direct => innerStorage,
            CrdtJournalMode.InMemoryBuffer when innerStorage is MemoryCrdtStorage => innerStorage,
            CrdtJournalMode.InMemoryBuffer => new BufferedDistributedCrdtStorageDecorator(
                innerStorage,
                sp.GetRequiredService<InMemoryWriteAheadBuffer>()),
            CrdtJournalMode.DiskWal => new BufferedDistributedCrdtStorageDecorator(
                innerStorage,
                sp.GetRequiredService<DiskWriteAheadBuffer>()),
            _ => innerStorage
        };
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

    private static void DecorateSystemRegistryProviders(IServiceCollection services)
    {
        var shardProviderDescriptor = services.LastOrDefault(d => d.ServiceType == typeof(IShardIdProvider));
        if (shardProviderDescriptor != null && shardProviderDescriptor.ImplementationInstance is not SystemRegistryShardIdProviderDecorator)
        {
            services.Remove(shardProviderDescriptor);
            services.Add(ServiceDescriptor.Singleton<IShardIdProvider>(new SystemRegistryShardIdProviderDecorator(shardProviderDescriptor)));
        }

        var docIdProviderDescriptor = services.LastOrDefault(d => d.ServiceType == typeof(IDocumentIdProvider));
        if (docIdProviderDescriptor != null && docIdProviderDescriptor.ImplementationInstance is not SystemRegistryDocumentIdProviderDecorator)
        {
            services.Remove(docIdProviderDescriptor);
            services.Add(ServiceDescriptor.Singleton<IDocumentIdProvider>(new SystemRegistryDocumentIdProviderDecorator(docIdProviderDescriptor)));
        }
    }
}