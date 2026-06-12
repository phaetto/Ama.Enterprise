namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;

using System.Collections.Generic;
using System.Text.Json.Serialization;

/// <summary>
/// AOT JSON context for the WebRTC CRDT distributed signaling state models.
/// </summary>
[JsonSourceGenerationOptions(
    WriteIndented = false,
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
    GenerationMode = JsonSourceGenerationMode.Default)]
[JsonSerializable(typeof(CrdtSignalingState))]
[JsonSerializable(typeof(Dictionary<string, long>))]
internal partial class DistributedSignalingAotContext : JsonSerializerContext
{
}