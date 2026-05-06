namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using Ama.Enterprise.P2p.Telemetry.Models;

/// <summary>
/// Thread-safe in-memory aggregator holding the latest telemetry network metrics.
/// </summary>
public sealed class TelemetryAggregator : ITelemetryAggregator
{
    private readonly ConcurrentDictionary<Guid, TelemetryPayloadDto> nodeMetrics = new();

    /// <inheritdoc />
    public void UpdateNodeTelemetry(TelemetryPayloadDto payload)
    {
        if (payload == null || payload.NodeId == Guid.Empty)
        {
            return;
        }

        nodeMetrics.AddOrUpdate(payload.NodeId, payload, (_, existing) => 
            payload.Timestamp > existing.Timestamp ? payload : existing);
    }

    /// <inheritdoc />
    public IEnumerable<TelemetryPayloadDto> GetAllNodeMetrics()
    {
        return nodeMetrics.Values.ToList();
    }

    /// <inheritdoc />
    public TelemetryPayloadDto? GetNodeMetrics(Guid nodeId)
    {
        return nodeMetrics.TryGetValue(nodeId, out var payload) ? payload : null;
    }

    /// <inheritdoc />
    public void PruneStaleNodes(TimeSpan threshold)
    {
        var cutoff = DateTimeOffset.UtcNow.Subtract(threshold);
        foreach (var kvp in nodeMetrics)
        {
            if (kvp.Value.Timestamp < cutoff)
            {
                nodeMetrics.TryRemove(kvp.Key, out _);
            }
        }
    }
}