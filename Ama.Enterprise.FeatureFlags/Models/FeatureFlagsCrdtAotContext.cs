namespace Ama.Enterprise.FeatureFlags.Models;

using System.Collections.Generic;
using Ama.CRDT.Attributes;
using Ama.CRDT.Models.Aot;

/// <summary>
/// AOT reflection context for the Feature Flags CRDT models.
/// </summary>
[CrdtAotType(typeof(FeatureFlag))]
[CrdtAotType(typeof(FeatureFlagState))]
[CrdtAotType(typeof(IDictionary<string, FeatureFlag>))]
[CrdtAotType(typeof(Dictionary<string, FeatureFlag>))]
public sealed partial class FeatureFlagsCrdtAotContext : CrdtAotContext
{
}