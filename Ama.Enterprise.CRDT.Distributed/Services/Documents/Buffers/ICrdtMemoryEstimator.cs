namespace Ama.Enterprise.CRDT.Distributed.Services.Documents.Buffers;

using System.Collections.Generic;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Estimates the in-memory byte footprint of CRDT documents, metadata, and shard version vectors using AOT contexts.
/// </summary>
/// <example>
/// <code>
/// var estimator = serviceProvider.GetRequiredService&lt;ICrdtMemoryEstimator&gt;();
/// var footprint = estimator.EstimateDocumentFootprint(document, "doc-1", "shard-1", 0, globalState, aotContexts);
/// </code>
/// </example>
public interface ICrdtMemoryEstimator
{
    /// <summary>
    /// Computes the detailed memory footprint breakdown for a materialized CRDT document and its local shard version vector.
    /// </summary>
    /// <typeparam name="TState">The document data state type.</typeparam>
    /// <param name="document">The materialized CRDT document.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="shardId">The shard identifier.</param>
    /// <param name="debounceBufferCapacity">The current capacity of the document's active-sync debounce buffer.</param>
    /// <param name="globalState">The local shard version vector map.</param>
    /// <param name="aotContexts">The registered AOT reflection contexts used to inspect properties without runtime reflection.</param>
    /// <returns>A <see cref="DocumentMemoryFootprintDto"/> detailing data, metadata, whole document, and shard version vector byte sizes.</returns>
    DocumentMemoryFootprintDto EstimateDocumentFootprint<TState>(
        CrdtDocument<TState> document,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap globalState,
        IReadOnlyList<CrdtAotContext> aotContexts)
        where TState : class, new();

    /// <summary>
    /// Computes the detailed memory footprint breakdown for a packed CRDT document (where data is materialized and metadata is stored as bytes)
    /// and its local shard version vector.
    /// </summary>
    /// <typeparam name="TState">The document data state type.</typeparam>
    /// <param name="data">The materialized domain data model.</param>
    /// <param name="metadataByteLength">The logical byte length of the packed CRDT metadata.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="shardId">The shard identifier.</param>
    /// <param name="debounceBufferCapacity">The current capacity of the document's active-sync debounce buffer.</param>
    /// <param name="globalState">The local shard version vector map.</param>
    /// <param name="aotContexts">The registered AOT reflection contexts used to inspect properties without runtime reflection.</param>
    /// <param name="allocatedMetadataBufferCapacity">The actual allocated capacity of the underlying pooled metadata byte array.</param>
    /// <returns>A <see cref="DocumentMemoryFootprintDto"/> detailing data, metadata, whole document, and shard version vector byte sizes.</returns>
    DocumentMemoryFootprintDto EstimatePackedMetadataDocumentFootprint<TState>(
        TState data,
        int metadataByteLength,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap globalState,
        IReadOnlyList<CrdtAotContext> aotContexts,
        int allocatedMetadataBufferCapacity = 0)
        where TState : class, new();

    /// <summary>
    /// Computes the detailed memory footprint breakdown for a fully serialized CRDT document and its local shard version vector in O(1) time.
    /// </summary>
    /// <param name="serializedDocumentByteLength">The logical byte length of the serialized document.</param>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="shardId">The shard identifier.</param>
    /// <param name="debounceBufferCapacity">The current capacity of the document's active-sync debounce buffer.</param>
    /// <param name="globalState">The local shard version vector map.</param>
    /// <param name="allocatedBufferCapacity">The actual allocated capacity of the underlying pooled document byte array.</param>
    /// <returns>A <see cref="DocumentMemoryFootprintDto"/> detailing whole document and shard version vector byte sizes.</returns>
    DocumentMemoryFootprintDto EstimateFullySerializedDocumentFootprint(
        int serializedDocumentByteLength,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap globalState,
        int allocatedBufferCapacity = 0);
}