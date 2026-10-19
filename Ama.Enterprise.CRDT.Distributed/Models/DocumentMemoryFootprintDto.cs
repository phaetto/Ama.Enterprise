namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;

/// <summary>
/// Data structure representing the estimated in-memory footprint of a distributed CRDT document without serialization.
/// </summary>
/// <param name="DataAndMetadataBytes">The estimated in-memory size in bytes of the CRDT document data and metadata graphs.</param>
/// <param name="WholeDocumentBytes">The estimated in-memory size in bytes of the entire document manager instance, including channels, pools, and telemetry.</param>
/// <param name="ShardVersionVectorBytes">The estimated in-memory size in bytes of the shard version vector map associated with this document's shard.</param>
public readonly record struct DocumentMemoryFootprintDto(
    long DataAndMetadataBytes,
    long WholeDocumentBytes,
    long ShardVersionVectorBytes) : IEquatable<DocumentMemoryFootprintDto>;