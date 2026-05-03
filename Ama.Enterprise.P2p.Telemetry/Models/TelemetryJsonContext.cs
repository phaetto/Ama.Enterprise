namespace Ama.Enterprise.P2p.Telemetry.Models;

using System.Text.Json.Serialization;

/// <summary>
/// Explicit AOT tracking boundaries uniquely mapping standard telemetry network interfaces safely isolated without reflections.
/// </summary>
[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(TelemetryPayloadDto))]
[JsonSerializable(typeof(MetricSnapshotDto))]
[JsonSerializable(typeof(MetricTagDto))]
public sealed partial class TelemetryJsonContext : JsonSerializerContext
{
}