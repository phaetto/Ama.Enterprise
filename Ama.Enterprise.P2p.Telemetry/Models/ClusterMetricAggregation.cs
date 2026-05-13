namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;
using System.Collections.Generic;

/// <summary>
/// Data structure representing the computed aggregated statistics for a specific metric across a cluster of nodes.
/// </summary>
public sealed record ClusterMetricAggregation : IEquatable<ClusterMetricAggregation>
{
    /// <summary>
    /// The name of the metric.
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// The primitive instrument type tracking the metric natively.
    /// </summary>
    public string Type { get; init; } = string.Empty;

    /// <summary>
    /// List of explicit multidimensional tags filtering the specific metric context.
    /// </summary>
    public IReadOnlyList<MetricTagDto> Tags { get; init; } = Array.Empty<MetricTagDto>();

    /// <summary>
    /// The number of distinct active nodes contributing to this aggregation.
    /// </summary>
    public int NodeCount { get; init; }

    /// <summary>
    /// The total accumulated sum of the metric values across evaluated nodes.
    /// </summary>
    public double Sum { get; init; }

    /// <summary>
    /// The lowest value recorded across evaluated nodes.
    /// </summary>
    public double Min { get; init; }

    /// <summary>
    /// The highest value recorded across evaluated nodes.
    /// </summary>
    public double Max { get; init; }

    /// <summary>
    /// The extrapolated rate of change per second evaluated natively.
    /// </summary>
    public double RatePerSecond { get; init; }

    /// <summary>
    /// The extrapolated rate of change per minute evaluated natively.
    /// </summary>
    public double RatePerMinute { get; init; }

    /// <inheritdoc />
    public bool Equals(ClusterMetricAggregation? other)
    {
        if (other is null)
        {
            return false;
        }

        if (!string.Equals(Name, other.Name, StringComparison.Ordinal) ||
            !string.Equals(Type, other.Type, StringComparison.Ordinal) ||
            NodeCount != other.NodeCount ||
            Sum != other.Sum || 
            Min != other.Min || 
            Max != other.Max ||
            RatePerSecond != other.RatePerSecond || 
            RatePerMinute != other.RatePerMinute)
        {
            return false;
        }

        if (Tags.Count != other.Tags.Count)
        {
            return false;
        }

        for (int i = 0; i < Tags.Count; i++)
        {
            if (!Tags[i].Equals(other.Tags[i]))
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
        hash.Add(Name);
        hash.Add(Type);
        hash.Add(NodeCount);
        hash.Add(Sum);
        hash.Add(Min);
        hash.Add(Max);
        hash.Add(RatePerSecond);
        hash.Add(RatePerMinute);

        foreach (var tag in Tags)
        {
            hash.Add(tag);
        }

        return hash.ToHashCode();
    }
}