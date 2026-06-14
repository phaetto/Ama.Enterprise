namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.CRDT.Models;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Message containing the local state vector of a CRDT document to synchronize with remote peers.
/// </summary>
public sealed record CrdtStateSyncMessage(string? ReplicaId, DottedVersionVector? State) : IExtensibleDistributedPayload
{
    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }
}