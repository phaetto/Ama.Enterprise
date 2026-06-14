namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.CRDT.Models;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Message payload containing a complete materialized CRDT document snapshot and its associated global bounds, used as a fallback synchronization mechanism when log truncation gaps are detected.
/// </summary>
public sealed record CrdtSnapshotMessage(string ReplicaId, byte[] SnapshotData, DottedVersionVector GlobalState) : IEquatable<CrdtSnapshotMessage>, IExtensibleDistributedPayload
{
    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }

    public bool Equals(CrdtSnapshotMessage? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return string.Equals(ReplicaId, other.ReplicaId, StringComparison.Ordinal) && 
               (SnapshotData == other.SnapshotData || (SnapshotData != null && other.SnapshotData != null && Enumerable.SequenceEqual(SnapshotData, other.SnapshotData))) &&
               Equals(GlobalState, other.GlobalState);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(ReplicaId, SnapshotData?.Length ?? 0, GlobalState);
    }
}