namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;

/// <summary>
/// Data structure representing a pre-serialized CRDT document checkpoint payload ready for batch persistence.
/// </summary>
/// <param name="ShardId">The shard identifier isolating document bounds.</param>
/// <param name="DocumentId">The storage key of the document.</param>
/// <param name="SerializedDocument">The pre-serialized document payload bytes.</param>
public readonly record struct SerializedDocumentCheckpointDto(
    string ShardId,
    string DocumentId,
    byte[] SerializedDocument) : IEquatable<SerializedDocumentCheckpointDto>;