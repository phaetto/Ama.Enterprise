namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.Enterprise.P2p.Telemetry.Models;

/// <summary>
/// Thread-safe service responsible for computing rates, deltas, and multi-node aggregations over mapped telemetry boundaries.
/// </summary>
public sealed class ClusterMetricsAggregator : IClusterMetricsAggregator
{
    private readonly TimeProvider timeProvider;
    private readonly Dictionary<Guid, NodeTelemetryState> networkState = new();
    private readonly object stateLock = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="ClusterMetricsAggregator"/> class.
    /// </summary>
    /// <param name="timeProvider">The core time provider injecting native temporal resolution limits.</param>
    public ClusterMetricsAggregator(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        this.timeProvider = timeProvider;
    }

    /// <inheritdoc />
    public void ProcessPayloads(IEnumerable<TelemetryPayloadDto> payloads)
    {
        ArgumentNullException.ThrowIfNull(payloads);

        lock (stateLock)
        {
            var currentIds = new HashSet<Guid>();
            var now = timeProvider.GetUtcNow();

            foreach (var payload in payloads)
            {
                currentIds.Add(payload.NodeId);

                if (!networkState.TryGetValue(payload.NodeId, out var state))
                {
                    state = new NodeTelemetryState(payload.NodeId);
                    networkState[payload.NodeId] = state;
                }

                // Store LastSeen using the aggregator's clock ensuring strict local evaluation bounds,
                // bypassing external payload.Timestamp clock-drift issues inherently avoiding instant amnesia.
                state.LastSeen = now;

                foreach (var metric in payload.Metrics)
                {
                    var tagSuffix = string.Join("|", metric.Tags.OrderBy(t => t.Key).Select(t => $"{t.Key}:{t.Value}"));
                    var metricKey = $"{metric.Name}[{tagSuffix}]";

                    if (!state.Metrics.TryGetValue(metricKey, out var stats))
                    {
                        stats = new MetricStats(metric.Name, metric.Type, metric.Tags.ToList());
                        state.Metrics[metricKey] = stats;
                    }

                    stats.Update(metric.Value, payload.Timestamp, now);
                }
            }

            // Implement a 2-minute time-based eviction gracefully covering network jitter dropouts.
            // Eliminates destructive sum shrinking by preserving mapped metrics explicitly properly natively.
            var departedNodes = networkState
                .Where(kvp => (now - kvp.Value.LastSeen).TotalMinutes > 2)
                .Select(kvp => kvp.Key)
                .ToList();

            foreach (var departed in departedNodes)
            {
                networkState.Remove(departed);
            }
        }
    }

    /// <inheritdoc />
    public IReadOnlyList<ClusterMetricAggregation> AggregateClusterMetrics(IReadOnlySet<Guid> targetNodeIds)
    {
        ArgumentNullException.ThrowIfNull(targetNodeIds);

        lock (stateLock)
        {
            var clusterMetrics = new Dictionary<string, AggregationBuilder>();
            var now = timeProvider.GetUtcNow();

            var targetNodes = networkState.Values.Where(n => targetNodeIds.Contains(n.NodeId));

            foreach (var nodeState in targetNodes)
            {
                foreach (var kvp in nodeState.Metrics)
                {
                    var metricKey = kvp.Key;
                    var stats = kvp.Value;

                    if (!clusterMetrics.TryGetValue(metricKey, out var agg))
                    {
                        agg = new AggregationBuilder
                        {
                            Name = stats.Name,
                            Type = stats.Type,
                            Tags = stats.Tags
                        };
                        clusterMetrics[metricKey] = agg;
                    }

                    agg.NodeCount++;
                    agg.Sum += stats.CurrentValue;
                    
                    if (stats.CurrentValue < agg.Min)
                    {
                        agg.Min = stats.CurrentValue;
                    }

                    if (stats.CurrentValue > agg.Max)
                    {
                        agg.Max = stats.CurrentValue;
                    }

                    agg.RatePerSecond += stats.GetPerSecond(now);
                    agg.RatePerMinute += stats.GetPerMinute(now);
                }
            }

            var result = new List<ClusterMetricAggregation>(clusterMetrics.Count);
            
            foreach (var agg in clusterMetrics.Values)
            {
                result.Add(new ClusterMetricAggregation
                {
                    Name = agg.Name,
                    Type = agg.Type,
                    Tags = agg.Tags,
                    NodeCount = agg.NodeCount,
                    Sum = agg.Sum,
                    Min = agg.Min == double.MaxValue ? 0 : agg.Min,
                    Max = agg.Max == double.MinValue ? 0 : agg.Max,
                    RatePerSecond = agg.RatePerSecond,
                    RatePerMinute = agg.RatePerMinute
                });
            }

            return result;
        }
    }

    /// <inheritdoc />
    public IReadOnlySet<Guid> GetTrackedNodeIds()
    {
        lock (stateLock)
        {
            return networkState.Keys.ToHashSet();
        }
    }

    private sealed class AggregationBuilder
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public IReadOnlyList<MetricTagDto> Tags { get; set; } = Array.Empty<MetricTagDto>();
        public int NodeCount { get; set; }
        public double Sum { get; set; }
        public double Min { get; set; } = double.MaxValue;
        public double Max { get; set; } = double.MinValue;
        public double RatePerSecond { get; set; }
        public double RatePerMinute { get; set; }
    }

    private readonly record struct MetricHistoryEntry(DateTimeOffset LocalTime, DateTimeOffset ServerTime, double Value);

    private sealed class NodeTelemetryState
    {
        public NodeTelemetryState(Guid nodeId)
        {
            NodeId = nodeId;
        }

        public Guid NodeId { get; }
        public DateTimeOffset LastSeen { get; set; }
        public Dictionary<string, MetricStats> Metrics { get; } = new();
    }

    private sealed class MetricStats
    {
        private readonly Queue<MetricHistoryEntry> history = new();

        public MetricStats(string name, string type, IReadOnlyList<MetricTagDto> tags)
        {
            Name = name;
            Type = type;
            Tags = tags;
        }

        public string Name { get; }
        public string Type { get; }
        public IReadOnlyList<MetricTagDto> Tags { get; }

        public double CurrentValue { get; private set; }

        public void Update(double value, DateTimeOffset serverTime, DateTimeOffset now)
        {
            if (history.Count > 0 && history.Last().ServerTime >= serverTime)
            {
                Prune(now);
                return;
            }

            bool isObservable = Type.Contains("Observable", StringComparison.OrdinalIgnoreCase);
            bool isGauge = Type.Contains("Gauge", StringComparison.OrdinalIgnoreCase);
            
            // Treat Counters, UpDownCounters, and Histograms as additive deltas natively.
            // Observable metrics and Gauges represent absolute snapshots explicitly.
            bool isDelta = !isObservable && !isGauge;

            if (isDelta)
            {
                CurrentValue += value;
            }
            else
            {
                CurrentValue = value;
            }

            history.Enqueue(new MetricHistoryEntry(now, serverTime, CurrentValue));
            Prune(now);
        }

        private void Prune(DateTimeOffset now)
        {
            while (history.Count > 0 && (now - history.Peek().LocalTime).TotalMinutes > 10)
            {
                history.Dequeue();
            }
        }

        public double GetPerSecond(DateTimeOffset now)
        {
            Prune(now);

            if (history.Count < 2)
            {
                return 0;
            }

            var first = history.Peek();
            var last = history.Last();

            var seconds = Math.Max(1.0, (now - first.LocalTime).TotalSeconds);
            var diff = last.Value - first.Value;

            bool isStrictCounter = Type.Contains("Counter", StringComparison.OrdinalIgnoreCase) && 
                                   !Type.Contains("UpDown", StringComparison.OrdinalIgnoreCase);

            if (isStrictCounter && diff < 0)
            {
                diff = 0;
            }

            return diff / seconds;
        }

        public double GetPerMinute(DateTimeOffset now)
        {
            Prune(now);

            if (history.Count < 2)
            {
                return 0;
            }

            var first = history.Peek();
            var last = history.Last();

            var minutes = Math.Max(1.0 / 60.0, (now - first.LocalTime).TotalMinutes);
            var diff = last.Value - first.Value;

            bool isStrictCounter = Type.Contains("Counter", StringComparison.OrdinalIgnoreCase) && 
                                   !Type.Contains("UpDown", StringComparison.OrdinalIgnoreCase);

            if (isStrictCounter && diff < 0)
            {
                diff = 0;
            }

            return diff / minutes;
        }
    }
}