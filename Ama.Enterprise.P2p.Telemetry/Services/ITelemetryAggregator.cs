namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Collections.Generic;
using Ama.Enterprise.P2p.Telemetry.Models;

/// <summary>
/// Interface for centrally aggregating and retrieving in-memory telemetry network states.
/// </summary>
public interface ITelemetryAggregator
{
    /// <summary>
    /// Updates the stored metrics for a specific node payload.
    /// </summary>
    /// <param name="payload">The inbound telemetry metric payload structure.</param>
    void UpdateNodeTelemetry(TelemetryPayloadDto payload);

    /// <summary>
    /// Retrieves all currently tracked node metrics.
    /// </summary>
    /// <returns>A collection of the latest telemetry states per active node.</returns>
    IEnumerable<TelemetryPayloadDto> GetAllNodeMetrics();

    /// <summary>
    /// Retrieves metrics for a specific node if they exist.
    /// </summary>
    /// <param name="nodeId">The unique identifier of the target node.</param>
    /// <returns>The most recent metric payload or null if unavailable.</returns>
    TelemetryPayloadDto? GetNodeMetrics(Guid nodeId);
    
    /// <summary>
    /// Clears tracked metrics older than the specified threshold.
    /// </summary>
    /// <param name="threshold">The maximum age limit for preserving dormant node states.</param>
    void PruneStaleNodes(TimeSpan threshold);
}