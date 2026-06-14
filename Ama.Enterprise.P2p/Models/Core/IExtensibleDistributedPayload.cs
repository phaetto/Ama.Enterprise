namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// Defines a strict contract for distributed payloads to natively support the Tolerant Reader pattern.
/// Implementing this interface ensures that newer protocol versions (V2) can be safely
/// received, preserved, and re-transmitted by older nodes (V1) without destructive data loss across both JSON and Binary boundaries.
/// </summary>
public interface IExtensibleDistributedPayload
{
    /// <summary>
    /// Captures unknown Map-based JSON properties natively during System.Text.Json deserialization.
    /// The Native AOT engine automatically populates this property and reconstructs the payload cleanly upon serialization.
    /// </summary>
    [JsonExtensionData]
    IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <summary>
    /// Captures unknown structural Array-based trailing elements during MessagePack binary deserialization.
    /// The native AOT generator explicitly unpacks unmapped indices here gracefully protecting binary integrity.
    /// <remarks>
    /// Consumers must exclusively append new generic properties strictly to the end of their DTOs safely preventing index shifts natively.
    /// </remarks>
    /// </summary>
    [JsonIgnore]
    IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }
}