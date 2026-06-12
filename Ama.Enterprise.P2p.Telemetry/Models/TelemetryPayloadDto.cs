namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;
using System.Collections.Generic;

/// <summary>
/// Top-level network payload transmission encapsulating uniquely active node configurations bounding periodic explicitly wrapped metric topologies.
/// </summary>
public sealed record TelemetryPayloadDto : IEquatable<TelemetryPayloadDto>
{
    /// <summary>
    /// Unique magic header prefix used to identify telemetry payloads over the network bypassing deserialization exceptions.
    /// Hexadecimal equivalent of ASCII "TELE" (0x454C4554).
    /// </summary>
    public const int MagicHeader = 0x454C4554;

    /// <summary>
    /// Globally distinct network identity originating generic metric clusters internally.
    /// </summary>
    public Guid NodeId { get; init; }

    /// <summary>
    /// Time sequence bounding exactly when explicit telemetry configurations captured runtime values natively.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Active measured snapshots capturing specific decoupled metrics natively.
    /// </summary>
    public IReadOnlyList<MetricSnapshotDto> Metrics { get; init; } = Array.Empty<MetricSnapshotDto>();

    /// <inheritdoc />
    public bool Equals(TelemetryPayloadDto? other)
    {
        if (other is null)
        {
            return false;
        }

        if (NodeId != other.NodeId || Timestamp != other.Timestamp)
        {
            return false;
        }

        if (Metrics.Count != other.Metrics.Count)
        {
            return false;
        }

        for (int i = 0; i < Metrics.Count; i++)
        {
            if (!Metrics[i].Equals(other.Metrics[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(NodeId);
        hash.Add(Timestamp);

        foreach (var metric in Metrics)
        {
            hash.Add(metric);
        }

        return hash.ToHashCode();
    }
}