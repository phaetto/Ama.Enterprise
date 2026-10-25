namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using Ama.CRDT.Services.Pooling;

/// <summary>
/// Configuration options for a live zero-downtime CRDT migration bridge between replicas or document types.
/// </summary>
public sealed class CrdtMigrationOptions : IEquatable<CrdtMigrationOptions>
{
    private string migrationId = CrdtStringPool.Shared.GetOrAdd("default-migration");
    private string sourceReplicaId = string.Empty;
    private string sourceTypeAlias = string.Empty;
    private string targetReplicaId = string.Empty;
    private string targetTypeAlias = string.Empty;

    /// <summary>
    /// Gets or sets the unique identifier for this migration registration.
    /// </summary>
    public string MigrationId
    {
        get => migrationId;
        set => migrationId = !string.IsNullOrWhiteSpace(value) ? CrdtStringPool.Shared.GetOrAdd(value) : string.Empty;
    }

    /// <summary>
    /// Gets or sets the source replica identifier from which documents and live changes are read.
    /// </summary>
    public string SourceReplicaId
    {
        get => sourceReplicaId;
        set => sourceReplicaId = !string.IsNullOrWhiteSpace(value) ? CrdtStringPool.Shared.GetOrAdd(value) : string.Empty;
    }

    /// <summary>
    /// Gets or sets the source document type alias registered in the source orchestrator.
    /// </summary>
    public string SourceTypeAlias
    {
        get => sourceTypeAlias;
        set => sourceTypeAlias = !string.IsNullOrWhiteSpace(value) ? CrdtStringPool.Shared.GetOrAdd(value) : string.Empty;
    }

    /// <summary>
    /// Gets or sets the target replica identifier to which migrated documents and intents are written.
    /// Can be the same as <see cref="SourceReplicaId"/> for in-place document type alias migrations, or a separate replica for cluster-to-cluster migrations.
    /// </summary>
    public string TargetReplicaId
    {
        get => targetReplicaId;
        set => targetReplicaId = !string.IsNullOrWhiteSpace(value) ? CrdtStringPool.Shared.GetOrAdd(value) : string.Empty;
    }

    /// <summary>
    /// Gets or sets the target document type alias registered in the target orchestrator.
    /// </summary>
    public string TargetTypeAlias
    {
        get => targetTypeAlias;
        set => targetTypeAlias = !string.IsNullOrWhiteSpace(value) ? CrdtStringPool.Shared.GetOrAdd(value) : string.Empty;
    }

    /// <summary>
    /// Gets or sets a value indicating whether only the primary shard owner in the source cluster should perform the background backfill for a shard.
    /// Defaults to <c>true</c> so multiple mediator nodes automatically partition the backfill workload without duplicating effort.
    /// </summary>
    public bool PartitionBackfillByPrimaryShardOwner { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether document tombstones (deletions) in the source cluster should be propagated to the target cluster.
    /// Defaults to <c>true</c>.
    /// </summary>
    public bool PropagateDocumentTombstones { get; set; } = true;

    /// <summary>
    /// Gets or sets the maximum number of documents to migrate concurrently during background backfill and live streaming.
    /// Defaults to <c>8</c>.
    /// </summary>
    public int MaxConcurrency { get; set; } = 8;

    /// <summary>
    /// Gets or sets the delay in milliseconds between background backfill batches to avoid saturating CPU and network resources on live systems.
    /// Defaults to <c>25</c> milliseconds.
    /// </summary>
    public int BackfillBatchDelayMilliseconds { get; set; } = 25;

    /// <summary>
    /// Gets or sets the interval in seconds for periodic reconciliation scans of the source registry to catch newly discovered documents or missed topological shifts.
    /// Defaults to <c>10</c> seconds.
    /// </summary>
    public int ReconciliationScanIntervalSeconds { get; set; } = 10;

    /// <summary>
    /// Gets or sets the delivery mode used when applying migrated intents or patches to the target orchestrator.
    /// Defaults to <see cref="IntentDeliveryMode.Acknowledged"/>.
    /// </summary>
    public IntentDeliveryMode TargetDeliveryMode { get; set; } = IntentDeliveryMode.Acknowledged;

    /// <inheritdoc />
    public bool Equals(CrdtMigrationOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(MigrationId, other.MigrationId, StringComparison.Ordinal) &&
               string.Equals(SourceReplicaId, other.SourceReplicaId, StringComparison.Ordinal) &&
               string.Equals(SourceTypeAlias, other.SourceTypeAlias, StringComparison.Ordinal) &&
               string.Equals(TargetReplicaId, other.TargetReplicaId, StringComparison.Ordinal) &&
               string.Equals(TargetTypeAlias, other.TargetTypeAlias, StringComparison.Ordinal) &&
               PartitionBackfillByPrimaryShardOwner == other.PartitionBackfillByPrimaryShardOwner &&
               PropagateDocumentTombstones == other.PropagateDocumentTombstones &&
               MaxConcurrency == other.MaxConcurrency &&
               BackfillBatchDelayMilliseconds == other.BackfillBatchDelayMilliseconds &&
               ReconciliationScanIntervalSeconds == other.ReconciliationScanIntervalSeconds &&
               TargetDeliveryMode == other.TargetDeliveryMode;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as CrdtMigrationOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(MigrationId, StringComparer.Ordinal);
        hash.Add(SourceReplicaId, StringComparer.Ordinal);
        hash.Add(SourceTypeAlias, StringComparer.Ordinal);
        hash.Add(TargetReplicaId, StringComparer.Ordinal);
        hash.Add(TargetTypeAlias, StringComparer.Ordinal);
        hash.Add(PartitionBackfillByPrimaryShardOwner);
        hash.Add(PropagateDocumentTombstones);
        hash.Add(MaxConcurrency);
        hash.Add(BackfillBatchDelayMilliseconds);
        hash.Add(ReconciliationScanIntervalSeconds);
        hash.Add(TargetDeliveryMode);
        return hash.ToHashCode();
    }
}