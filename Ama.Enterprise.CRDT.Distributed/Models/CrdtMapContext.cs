namespace Ama.Enterprise.CRDT.Distributed.Models;

using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Provides execution context for a single document during the Map phase of a distributed Map/Reduce operation.
/// </summary>
/// <typeparam name="TState">The CRDT document state type.</typeparam>
/// <param name="DocumentId">The unique document identifier.</param>
/// <param name="ShardId">The shard identifier hosting the document.</param>
/// <param name="ReplicaId">The local replica identifier evaluating the document.</param>
/// <param name="Document">The materialized CRDT document state and metadata.</param>
/// <param name="LiveDocumentHandle">The optional active document manager handle when the document is active in memory.</param>
public readonly record struct CrdtMapContext<TState>(
    string DocumentId,
    string ShardId,
    string ReplicaId,
    CrdtDocument<TState> Document,
    IDistributedCrdtDocument<TState>? LiveDocumentHandle = null)
    where TState : class, new()
{
    /// <summary>
    /// Gets a value indicating whether the document is currently active and mutable in live memory on this replica
    /// (as opposed to being read from a demoted storage snapshot fallback).
    /// </summary>
    public bool CanMutateInPlace => LiveDocumentHandle is not null;
}