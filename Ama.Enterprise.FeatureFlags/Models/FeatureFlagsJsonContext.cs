namespace Ama.Enterprise.FeatureFlags.Models;

using System.Collections.Generic;
using System.Text.Json.Serialization;
using Ama.CRDT.Models;

/// <summary>
/// AOT-friendly JSON context for Feature Flags serialization.
/// </summary>
[JsonSerializable(typeof(FeatureFlag))]
[JsonSerializable(typeof(FeatureFlagMetadata))]
[JsonSerializable(typeof(FeatureFlagAudit))]
[JsonSerializable(typeof(FeatureFlagOwnership))]
[JsonSerializable(typeof(FeatureFlagState))]
[JsonSerializable(typeof(CrdtDocument<FeatureFlagState>))]
[JsonSerializable(typeof(Dictionary<string, FeatureFlag>))]
[JsonSerializable(typeof(List<JournaledOperation>))]
[JsonSerializable(typeof(DottedVersionVector))]
public sealed partial class FeatureFlagsJsonContext : JsonSerializerContext
{
}