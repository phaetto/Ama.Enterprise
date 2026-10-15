namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Manages local shard dotted version vectors in a thread-safe manner.
/// Centralizes all state reads, mutations, and per-shard execution locks to prevent concurrent pollution and partial snapshot advances.
/// </summary>
/// <example>
/// <code>
/// public sealed class MySyncHandler
/// {
///     private readonly IShardVersionVectorManager versionVectorManager;
///     
///     public MySyncHandler(IShardVersionVectorManager versionVectorManager)
///     {
///         this.versionVectorManager = versionVectorManager;
///     }
///     
///     public async Task SyncDocumentSnapshotAsync(string shardId, string documentId, ShardVersionVectorMap remoteState, IReadOnlyList&lt;string&gt; activeShardDocs, CancellationToken cancellationToken)
///     {
///         await versionVectorManager.ExecuteShardLockedAsync(shardId, async () =>
///         {
///             versionVectorManager.RecordDocumentSnapshotMerge(shardId, documentId, remoteState, activeShardDocs);
///             await Task.CompletedTask;
///         }, cancellationToken);
///     }
/// }
/// </code>
/// </example>
public interface IShardVersionVectorManager
{
    /// <summary>
    /// Gets a thread-safe list of all currently tracked shard identifiers.
    /// </summary>
    /// <returns>A read-only list of shard identifiers.</returns>
    IReadOnlyList<string> GetShardIds();

    /// <summary>
    /// Creates a deep-copied snapshot of the version vector for a single shard.
    /// </summary>
    /// <param name="shardId">The identifier of the shard.</param>
    /// <returns>A map containing a copy of the requested shard's version vector if present.</returns>
    ShardVersionVectorMap GetShardSnapshot(string shardId);

    /// <summary>
    /// Creates a deep-copied snapshot of the version vectors for a specific set of shards.
    /// </summary>
    /// <param name="shardIds">The collection of shard identifiers to snapshot.</param>
    /// <returns>A map containing copies of the requested shards' version vectors.</returns>
    ShardVersionVectorMap GetShardsSnapshot(IEnumerable<string> shardIds);

    /// <summary>
    /// Creates a deep-copied snapshot of all tracked shard version vectors.
    /// </summary>
    /// <returns>A map containing copies of all tracked shard version vectors.</returns>
    ShardVersionVectorMap GetFullSnapshot();

    /// <summary>
    /// Asynchronously creates a deep-copied snapshot of the version vectors for a specific set of shards, waiting for any in-flight shard operations to complete.
    /// </summary>
    /// <param name="shardIds">The collection of shard identifiers to snapshot.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A map containing copies of the requested shards' version vectors.</returns>
    ValueTask<ShardVersionVectorMap> GetShardsSnapshotAsync(IEnumerable<string> shardIds, CancellationToken cancellationToken = default);

    /// <summary>
    /// Asynchronously creates a deep-copied snapshot of all tracked shard version vectors, waiting for any in-flight shard operations to complete.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A map containing copies of all tracked shard version vectors.</returns>
    ValueTask<ShardVersionVectorMap> GetFullSnapshotAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Ensures that a version vector entry exists for the specified shard.
    /// </summary>
    /// <param name="shardId">The identifier of the shard to register.</param>
    void EnsureShardRegistered(string shardId);

    /// <summary>
    /// Ensures that version vector entries exist for all specified shards and returns the complete set of known shard identifiers.
    /// </summary>
    /// <param name="shardIds">The collection of shard identifiers to register.</param>
    /// <returns>A read-only set of all known shard identifiers.</returns>
    IReadOnlySet<string> EnsureShardsRegisteredAndGetAll(IEnumerable<string> shardIds);

    /// <summary>
    /// Merges the incoming version vector for a specific shard into the local state.
    /// </summary>
    /// <param name="shardId">The identifier of the shard to update.</param>
    /// <param name="incomingState">The incoming shard version vector map.</param>
    void MergeShardState(string shardId, ShardVersionVectorMap incomingState);

    /// <summary>
    /// Records that a specific document within a shard has merged a snapshot, and only advances the shared shard version vector once all active documents in that shard have merged.
    /// </summary>
    /// <param name="shardId">The identifier of the shard.</param>
    /// <param name="documentId">The identifier of the document that merged its snapshot.</param>
    /// <param name="incomingState">The incoming shard version vector map attached to the snapshot.</param>
    /// <param name="activeShardDocumentIds">The list of all active document identifiers currently belonging to this shard.</param>
    void RecordDocumentSnapshotMerge(string shardId, string documentId, ShardVersionVectorMap incomingState, IReadOnlyList<string> activeShardDocumentIds);

    /// <summary>
    /// Replaces the entire local shard version vector map with a deep copy of the provided map.
    /// </summary>
    /// <param name="savedMap">The persisted shard version vector map to restore.</param>
    void RestoreFullState(ShardVersionVectorMap savedMap);

    /// <summary>
    /// Removes a shard's version vector from local tracking.
    /// </summary>
    /// <param name="shardId">The identifier of the shard to remove.</param>
    void RemoveShard(string shardId);

    /// <summary>
    /// Removes all evicted replicas from all tracked shard version vectors.
    /// </summary>
    /// <param name="replicaIds">The list of replica identifiers to evict.</param>
    void EvictReplicas(IReadOnlyList<string> replicaIds);

    /// <summary>
    /// Clears all tracked shard version vectors.
    /// </summary>
    void Clear();

    /// <summary>
    /// Executes an asynchronous action while holding the exclusive operation lock for the specified shard.
    /// Ensures that intent generation, DVV mutation, and journal persistence execute atomically per shard.
    /// </summary>
    /// <param name="shardId">The identifier of the shard to lock.</param>
    /// <param name="action">The asynchronous action to execute inside the shard lock.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ExecuteShardLockedAsync(string shardId, Func<Task> action, CancellationToken cancellationToken = default);
}