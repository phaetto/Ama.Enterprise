namespace Ama.Enterprise.CRDT.Distributed.Services.Documents.Buffers;

using System;
using System.Collections.Generic;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines the pluggable in-memory representation strategy for a distributed CRDT document,
/// managing how the domain state and CRDT metadata are stored, unpacked, serialized, and disposed.
/// </summary>
/// <typeparam name="TState">The CRDT state model type.</typeparam>
/// <example>
/// <code>
/// using ICrdtDocumentBuffer&lt;MyState&gt; buffer = new PackedMetadataDocumentBuffer&lt;MyState&gt;(initialDoc, metadataManager, serializer);
/// var data = buffer.GetData();
/// var fullDoc = buffer.UnpackFullDocument();
/// </code>
/// </example>
public interface ICrdtDocumentBuffer<TState> : IDisposable where TState : class, new()
{
    /// <summary>
    /// Gets the active residency mode managed by this buffer implementation.
    /// </summary>
    CrdtDocumentResidencyMode Mode { get; }

    /// <summary>
    /// Retrieves the domain data model (<typeparamref name="TState"/>).
    /// In <see cref="CrdtDocumentResidencyMode.Unpacked"/> and <see cref="CrdtDocumentResidencyMode.PackedMetadata"/> modes, this returns the materialized reference in O(1) without unpacking metadata.
    /// In <see cref="CrdtDocumentResidencyMode.FullySerialized"/> mode, this deserializes the document on demand.
    /// </summary>
    /// <returns>The domain data model instance.</returns>
    TState GetData();

    /// <summary>
    /// Retrieves or unpacks the full <see cref="CrdtDocument{TState}"/> including both domain data and <see cref="CrdtMetadata"/>.
    /// </summary>
    /// <returns>The full materialized CRDT document.</returns>
    CrdtDocument<TState> UnpackFullDocument();

    /// <summary>
    /// Stores an updated full <see cref="CrdtDocument{TState}"/> into the buffer according to its residency mode.
    /// </summary>
    /// <param name="document">The full CRDT document to store.</param>
    void StoreFullDocument(CrdtDocument<TState> document);

    /// <summary>
    /// Populates the buffer directly from a raw serialized <see cref="CrdtDocument{TState}"/> byte payload.
    /// </summary>
    /// <param name="serializedDocumentBytes">The raw serialized document bytes.</param>
    void StoreSerializedBytes(ReadOnlySpan<byte> serializedDocumentBytes);

    /// <summary>
    /// Serializes or copies the full <see cref="CrdtDocument{TState}"/> as a byte array for checkpointing or wire snapshot transmission.
    /// </summary>
    /// <returns>A byte array containing the serialized CRDT document.</returns>
    byte[] GetSerializedBytes();

    /// <summary>
    /// Estimates the memory footprint of the buffered document and its local shard version vector.
    /// </summary>
    /// <param name="estimator">The memory estimator service.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="shardId">The shard identifier.</param>
    /// <param name="debounceBufferCapacity">The current capacity of the active-sync debounce buffer.</param>
    /// <param name="shardDvv">The local shard version vector map.</param>
    /// <param name="aotContexts">The registered AOT reflection contexts.</param>
    /// <returns>A <see cref="DocumentMemoryFootprintDto"/> detailing the memory usage.</returns>
    DocumentMemoryFootprintDto EstimateFootprint(
        ICrdtMemoryEstimator estimator,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap shardDvv,
        IReadOnlyList<CrdtAotContext> aotContexts);
}