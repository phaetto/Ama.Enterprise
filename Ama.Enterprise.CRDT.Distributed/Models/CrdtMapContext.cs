namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Provides execution context for a single document during the Map phase of a distributed Map/Reduce operation.
/// </summary>
/// <typeparam name="TState">The CRDT document state type.</typeparam>
public readonly record struct CrdtMapContext<TState> : IEquatable<CrdtMapContext<TState>>
    where TState : class, new()
{
    private readonly CrdtMetadata? fallbackMetadata;

    /// <summary>
    /// Gets the unique document identifier.
    /// </summary>
    public string DocumentId { get; }

    /// <summary>
    /// Gets the shard identifier hosting the document.
    /// </summary>
    public string ShardId { get; }

    /// <summary>
    /// Gets the local replica identifier evaluating the document.
    /// </summary>
    public string ReplicaId { get; }

    /// <summary>
    /// Gets the materialized state model for the document.
    /// In <see cref="CrdtDocumentResidencyMode.PackedMetadata"/> mode, accessing this property never unpacks CRDT metadata.
    /// </summary>
    public TState Data { get; }

    /// <summary>
    /// Gets the optional active document manager handle when the document is active in memory.
    /// </summary>
    public IDistributedCrdtDocument<TState>? LiveDocumentHandle { get; }

    /// <summary>
    /// Gets the full <see cref="CrdtDocument{TState}"/> including causal <see cref="CrdtMetadata"/>.
    /// When backed by a <see cref="LiveDocumentHandle"/> in <see cref="CrdtDocumentResidencyMode.PackedMetadata"/> mode,
    /// accessing this property unpacks the binary metadata on demand.
    /// </summary>
    public CrdtDocument<TState> Document => ReadFullDocument();

    /// <summary>
    /// Gets a value indicating whether the document is currently active and mutable in live memory on this replica
    /// (as opposed to being read from a demoted storage snapshot fallback).
    /// </summary>
    public bool CanMutateInPlace => LiveDocumentHandle is not null;

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtMapContext{TState}"/> struct using materialized domain data.
    /// </summary>
    /// <param name="documentId">The unique document identifier.</param>
    /// <param name="shardId">The shard identifier hosting the document.</param>
    /// <param name="replicaId">The local replica identifier evaluating the document.</param>
    /// <param name="data">The materialized CRDT document domain data.</param>
    /// <param name="liveDocumentHandle">The optional active document manager handle when the document is active in memory.</param>
    /// <param name="fallbackMetadata">Optional causal metadata when constructed from a stored snapshot fallback.</param>
    public CrdtMapContext(
        string documentId,
        string shardId,
        string replicaId,
        TState data,
        IDistributedCrdtDocument<TState>? liveDocumentHandle = null,
        CrdtMetadata? fallbackMetadata = null)
    {
        DocumentId = documentId;
        ShardId = shardId;
        ReplicaId = replicaId;
        Data = data;
        LiveDocumentHandle = liveDocumentHandle;
        this.fallbackMetadata = fallbackMetadata;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtMapContext{TState}"/> struct from a full <see cref="CrdtDocument{TState}"/>.
    /// </summary>
    /// <param name="documentId">The unique document identifier.</param>
    /// <param name="shardId">The shard identifier hosting the document.</param>
    /// <param name="replicaId">The local replica identifier evaluating the document.</param>
    /// <param name="document">The materialized CRDT document state and metadata.</param>
    /// <param name="liveDocumentHandle">The optional active document manager handle when the document is active in memory.</param>
    public CrdtMapContext(
        string documentId,
        string shardId,
        string replicaId,
        CrdtDocument<TState> document,
        IDistributedCrdtDocument<TState>? liveDocumentHandle = null)
    {
        DocumentId = documentId;
        ShardId = shardId;
        ReplicaId = replicaId;
        Data = document.Data!;
        LiveDocumentHandle = liveDocumentHandle;
        fallbackMetadata = document.Metadata;
    }

    /// <summary>
    /// Reads the full <see cref="CrdtDocument{TState}"/> including causal <see cref="CrdtMetadata"/>.
    /// </summary>
    /// <returns>The full CRDT document containing both data and metadata.</returns>
    public CrdtDocument<TState> ReadFullDocument()
    {
        if (LiveDocumentHandle is not null)
        {
            return LiveDocumentHandle.ReadFullDocument();
        }

        return new CrdtDocument<TState>(Data, fallbackMetadata ?? new CrdtMetadata());
    }

    /// <inheritdoc />
    public bool Equals(CrdtMapContext<TState> other)
    {
        return string.Equals(DocumentId, other.DocumentId, StringComparison.Ordinal) &&
               string.Equals(ShardId, other.ShardId, StringComparison.Ordinal) &&
               string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal) &&
               EqualityComparer<TState>.Default.Equals(Data, other.Data) &&
               ReferenceEquals(LiveDocumentHandle, other.LiveDocumentHandle);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(
            StringComparer.Ordinal.GetHashCode(DocumentId ?? string.Empty),
            StringComparer.Ordinal.GetHashCode(ShardId ?? string.Empty),
            StringComparer.Ordinal.GetHashCode(ReplicaId ?? string.Empty),
            Data,
            LiveDocumentHandle);
    }
}