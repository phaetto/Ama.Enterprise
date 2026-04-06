namespace Ama.Enterprise.FeatureFlags.Models.P2p;

using System.Text.Json.Serialization;
using Ama.CRDT.Models;

/// <summary>
/// JSON serialization context for Feature Flags P2P integration models guaranteeing AOT compatibility.
/// </summary>
[JsonSerializable(typeof(FeatureFlagMessageWrapper))]
[JsonSerializable(typeof(FeatureFlagStateSyncMessage))]
[JsonSerializable(typeof(FeatureFlagOperationsMessage))]
[JsonSerializable(typeof(CrdtOperation[]))]
[JsonSerializable(typeof(CrdtOperation))]
[JsonSerializable(typeof(DottedVersionVector))]
public sealed partial class FeatureFlagP2pJsonContext : JsonSerializerContext
{
}