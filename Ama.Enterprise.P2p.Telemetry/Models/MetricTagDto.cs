namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;

/// <summary>
/// AOT friendly data structure identifying distinct multidimensional metrics mapping attributes safely.
/// </summary>
public readonly record struct MetricTagDto(string Key, string Value) : IEquatable<MetricTagDto>
{
    /// <inheritdoc />
    public bool Equals(MetricTagDto other)
    {
        return string.Equals(Key, other.Key, StringComparison.Ordinal) && 
               string.Equals(Value, other.Value, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(Key, Value);
    }
}