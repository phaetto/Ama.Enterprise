namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background service responsible for systematically persisting memory states.
/// </summary>
public sealed class CrdtCheckpointService : BackgroundService
{
    private readonly DistributedCrdtScopeManager scopeManager;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtCheckpointService> logger;

    private readonly Meter meter;
    private readonly Counter<long> checkpointCyclesCounter;

    private readonly Dictionary<string, int> lastKnownMapDigestCache = new(StringComparer.Ordinal);
    private readonly Dictionary<string, int> lastKnownClusterStateDigestCache = new(StringComparer.Ordinal);

    public CrdtCheckpointService(
        DistributedCrdtScopeManager scopeManager,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtCheckpointService> logger,
        IMeterFactory? meterFactory = null)
    {
        this.scopeManager = scopeManager ?? throw new ArgumentNullException(nameof(scopeManager));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtCheckpointService") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtCheckpointService");
        this.checkpointCyclesCounter = this.meter.CreateCounter<long>("crdt.checkpoint.cycles", "cycles", "Total background checkpoint cycles executed");
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var intervalSeconds = options.Value.CheckpointIntervalSeconds;
        var delay = intervalSeconds > 0 ? TimeSpan.FromSeconds(intervalSeconds) : TimeSpan.FromSeconds(30);
        var avoidBlindWrites = options.Value.AvoidBlindCheckpointWrites;

        logger.LogInformation("CRDT background checkpoint service started with an interval of {Seconds} seconds. AvoidBlindWrites: {AvoidBlindWrites}", delay.TotalSeconds, avoidBlindWrites);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(delay, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            var scopes = scopeManager.GetActiveScopes();

            if (avoidBlindWrites && (lastKnownMapDigestCache.Count > 0 || lastKnownClusterStateDigestCache.Count > 0))
            {
                PruneEvictedScopeDigests(scopes);
            }

            foreach (var scope in scopes)
            {
                try
                {
                    var isMemoryStorage = scope.Storage is MemoryCrdtStorage
                        || (scope.Storage is BufferedDistributedCrdtStorageDecorator decorator && decorator.InnerStorage is MemoryCrdtStorage);

                    var versionVectorManager = scope.ServiceProvider.GetRequiredService<IShardVersionVectorManager>();
                    var orchestrator = scope.Orchestrator;

                    var tag = new KeyValuePair<string, object?>("replica_id", scope.ReplicaId);
                    checkpointCyclesCounter.Add(1, tag);

                    await CheckpointActiveDocumentsInBatchesAsync(scope.Storage, orchestrator, stoppingToken).ConfigureAwait(false);

                    // Ephemeral MemoryCrdtStorage lives inside the same RAM space as ShardVersionVectorManager and ClusterStateTracker.
                    // Deep-cloning 100K+ ShardVersionVectors and ClusterState snapshots every few seconds into MemoryCrdtStorage
                    // and local cache dictionaries only duplicates the entire version vector heap in memory with zero durability benefit.
                    if (isMemoryStorage)
                    {
                        continue;
                    }

                    var safelyPersistedMap = versionVectorManager.GetFullSnapshot();
                    var exportedClusterState = scope.ClusterTracker.ExportState();

                    var mapDigest = ComputeMapDigest(safelyPersistedMap);
                    var clusterDigest = ComputeClusterStateDigest(exportedClusterState);

                    var dvvChanged = true;
                    var clusterStateChanged = true;

                    if (avoidBlindWrites)
                    {
                        if (lastKnownMapDigestCache.TryGetValue(scope.ReplicaId, out var cachedMapDigest))
                        {
                            dvvChanged = cachedMapDigest != mapDigest;
                        }

                        if (lastKnownClusterStateDigestCache.TryGetValue(scope.ReplicaId, out var cachedStateDigest))
                        {
                            clusterStateChanged = cachedStateDigest != clusterDigest;
                        }
                    }

                    if (dvvChanged || !avoidBlindWrites)
                    {
                        await scope.Storage.SaveShardVersionVectorMapAsync(scope.ReplicaId, safelyPersistedMap, stoppingToken).ConfigureAwait(false);
                        if (avoidBlindWrites)
                        {
                            lastKnownMapDigestCache[scope.ReplicaId] = mapDigest;
                        }
                    }

                    if (clusterStateChanged || !avoidBlindWrites)
                    {
                        await scope.Storage.SaveClusterStateAsync(scope.ReplicaId, exportedClusterState, stoppingToken).ConfigureAwait(false);
                        if (avoidBlindWrites)
                        {
                            lastKnownClusterStateDigestCache[scope.ReplicaId] = clusterDigest;
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "[{ReplicaId}] An error occurred during periodic CRDT checkpointing. Cycle aborted.", scope.ReplicaId);
                }
            }
        }
    }

    public override void Dispose()
    {
        meter.Dispose();
        base.Dispose();
    }

    internal static async Task CheckpointActiveDocumentsInBatchesAsync(
        IDistributedCrdtStorage storage,
        ICrdtDocumentOrchestrator orchestrator,
        CancellationToken cancellationToken,
        ShardVersionVectorMap? safelyPersistedMapToMergeRegistry = null)
    {
        const int MaxBatchSize = 2048;
        List<SerializedDocumentCheckpointDto>? batchPayload = null;
        List<IDistributedCrdtDocument>? batchDocs = null;

        async Task FlushBatchAsync()
        {
            if (batchPayload == null || batchPayload.Count == 0)
            {
                return;
            }

            try
            {
                await storage.SaveSerializedDocumentBatchAsync(batchPayload, cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                if (batchDocs != null)
                {
                    for (int i = 0; i < batchDocs.Count; i++)
                    {
                        batchDocs[i].RestoreDirtyAfterFailedCheckpoint();
                    }
                }

                throw;
            }
            finally
            {
                batchPayload.Clear();
                batchDocs?.Clear();
            }
        }

        void CollectDocument(IDistributedCrdtDocument doc)
        {
            if (doc.TryPrepareCheckpoint(out var checkpoint))
            {
                batchPayload ??= new List<SerializedDocumentCheckpointDto>(256);
                batchDocs ??= new List<IDistributedCrdtDocument>(256);
                batchPayload.Add(checkpoint);
                batchDocs.Add(doc);
            }
        }

        if (orchestrator is CrdtDocumentOrchestrator concreteOrchestrator)
        {
            CollectDocument(concreteOrchestrator.Registry);
            if (safelyPersistedMapToMergeRegistry != null)
            {
                MergeRegistryVersionVector(concreteOrchestrator.Registry, safelyPersistedMapToMergeRegistry);
            }

            foreach (var document in concreteOrchestrator.EnumerateActiveDocuments())
            {
                CollectDocument(document);
                if (batchPayload is { Count: >= MaxBatchSize })
                {
                    await FlushBatchAsync().ConfigureAwait(false);
                }
            }

            await FlushBatchAsync().ConfigureAwait(false);
        }
        else
        {
            var documents = orchestrator.GetActiveDocuments();
            for (int i = 0; i < documents.Count; i++)
            {
                var document = documents[i];
                if (document is DistributedCrdtDocument<CrdtRegistryState>)
                {
                    CollectDocument(document);
                }
                else if (document.TryPrepareCheckpoint(out var checkpoint))
                {
                    batchPayload ??= new List<SerializedDocumentCheckpointDto>(256);
                    batchDocs ??= new List<IDistributedCrdtDocument>(256);
                    batchPayload.Add(checkpoint);
                    batchDocs.Add(document);
                }
                else
                {
                    await document.CheckpointAsync(cancellationToken).ConfigureAwait(false);
                }

                if (safelyPersistedMapToMergeRegistry != null && document is IDistributedCrdtDocument<CrdtRegistryState> registryDoc)
                {
                    MergeRegistryVersionVector(registryDoc, safelyPersistedMapToMergeRegistry);
                }

                if (batchPayload is { Count: >= MaxBatchSize })
                {
                    await FlushBatchAsync().ConfigureAwait(false);
                }
            }

            await FlushBatchAsync().ConfigureAwait(false);
        }
    }

    internal static void MergeRegistryVersionVector(
        IDistributedCrdtDocument<CrdtRegistryState> registryDoc,
        ShardVersionVectorMap safelyPersistedMap)
    {
        if (string.IsNullOrWhiteSpace(registryDoc.ShardId))
        {
            return;
        }

        var fullRegistryDoc = registryDoc.ReadFullDocument();
        if (fullRegistryDoc.Metadata?.VersionVector == null || fullRegistryDoc.Metadata.VersionVector.Count == 0)
        {
            return;
        }

        var regDvv = safelyPersistedMap.TryGetValue(registryDoc.ShardId, out var existingRegDvv)
            ? existingRegDvv
            : new DottedVersionVector();

        foreach (var kvp in fullRegistryDoc.Metadata.VersionVector)
        {
            if (kvp.Value > 0 &&
                (!regDvv.Versions.TryGetValue(kvp.Key, out var currentVer) || kvp.Value > currentVer))
            {
                regDvv.Versions[kvp.Key] = kvp.Value;
            }
        }

        if (regDvv.Versions.Count > 0)
        {
            safelyPersistedMap[registryDoc.ShardId] = regDvv;
        }
    }

    private static int ComputeMapDigest(ShardVersionVectorMap? map)
    {
        if (map is null)
        {
            return 0;
        }

        int hash = map.Count * 397;
        foreach (var kvp in map)
        {
            int shardHash = StringComparer.Ordinal.GetHashCode(kvp.Key);
            var dvv = kvp.Value;
            if (dvv.Versions != null)
            {
                shardHash ^= dvv.Versions.Count * 17;
                foreach (var vKvp in dvv.Versions)
                {
                    shardHash ^= HashCode.Combine(StringComparer.Ordinal.GetHashCode(vKvp.Key), vKvp.Value);
                }
            }

            if (dvv.Dots != null)
            {
                shardHash ^= dvv.Dots.Count * 31;
                foreach (var dKvp in dvv.Dots)
                {
                    int dotHash = StringComparer.Ordinal.GetHashCode(dKvp.Key) ^ (dKvp.Value.Count * 13);
                    foreach (var dot in dKvp.Value)
                    {
                        dotHash ^= dot.GetHashCode();
                    }
                    shardHash ^= dotHash;
                }
            }

            hash ^= shardHash;
        }

        return hash;
    }

    private static int ComputeClusterStateDigest(ClusterStateSnapshotDto? state)
    {
        if (state is null)
        {
            return 0;
        }

        int hash = (state.TombstonedReplicas.Count * 397) ^ state.PeerStates.Count;
        foreach (var tKvp in state.TombstonedReplicas)
        {
            hash ^= HashCode.Combine(StringComparer.Ordinal.GetHashCode(tKvp.Key), tKvp.Value);
        }

        foreach (var pKvp in state.PeerStates)
        {
            hash ^= HashCode.Combine(
                StringComparer.Ordinal.GetHashCode(pKvp.Key),
                pKvp.Value.LastSeen,
                ComputeMapDigest(pKvp.Value.State));
        }

        return hash;
    }

    private void PruneEvictedScopeDigests(IReadOnlyList<IDistributedCrdtScope> activeScopes)
    {
        var activeReplicaIds = new HashSet<string>(activeScopes.Count, StringComparer.Ordinal);
        for (int i = 0; i < activeScopes.Count; i++)
        {
            activeReplicaIds.Add(activeScopes[i].ReplicaId);
        }

        List<string>? staleKeys = null;
        foreach (var key in lastKnownMapDigestCache.Keys)
        {
            if (!activeReplicaIds.Contains(key))
            {
                staleKeys ??= new List<string>();
                staleKeys.Add(key);
            }
        }

        if (staleKeys != null)
        {
            for (int i = 0; i < staleKeys.Count; i++)
            {
                lastKnownMapDigestCache.Remove(staleKeys[i]);
            }
            staleKeys.Clear();
        }

        foreach (var key in lastKnownClusterStateDigestCache.Keys)
        {
            if (!activeReplicaIds.Contains(key))
            {
                staleKeys ??= new List<string>();
                staleKeys.Add(key);
            }
        }

        if (staleKeys != null)
        {
            for (int i = 0; i < staleKeys.Count; i++)
            {
                lastKnownClusterStateDigestCache.Remove(staleKeys[i]);
            }
        }
    }
}