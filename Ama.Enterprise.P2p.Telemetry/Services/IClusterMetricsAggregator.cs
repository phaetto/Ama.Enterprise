namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Collections.Generic;
using Ama.Enterprise.P2p.Telemetry.Models;

/// <summary>
/// Contract for aggregating cluster-wide telemetry metrics across active nodes natively tracking mathematical trends.
/// </summary>
public interface IClusterMetricsAggregator
{
    /// <summary>
    /// Processes incoming raw telemetry payloads to track historical states.
    /// </summary>
    /// <param name="payloads">The payloads extracted from active nodes.</param>
    void ProcessPayloads(IEnumerable<TelemetryPayloadDto> payloads);

    /// <summary>
    /// Aggregates tracked metric histories for the specified targeted subset, calculating sums and rates natively.
    /// </summary>
    /// <param name="targetNodeIds">The subset of node identifiers to evaluate explicitly.</param>
    /// <returns>A list of aggregated metric projections.</returns>
    IReadOnlyList<ClusterMetricAggregation> AggregateClusterMetrics(IReadOnlySet<Guid> targetNodeIds);

    /// <summary>
    /// Retrieves the complete set of dynamically mapped tracked node identifiers avoiding duplication.
    /// </summary>
    /// <returns>A read-only set of tracked node identifiers.</returns>
    IReadOnlySet<Guid> GetTrackedNodeIds();
}