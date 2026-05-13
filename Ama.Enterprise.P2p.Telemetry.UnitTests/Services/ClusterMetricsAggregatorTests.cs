namespace Ama.Enterprise.P2p.Telemetry.UnitTests.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.Enterprise.P2p.Telemetry.Models;
using Ama.Enterprise.P2p.Telemetry.Services;
using Microsoft.Extensions.Time.Testing;
using Shouldly;
using Xunit;

public sealed class ClusterMetricsAggregatorTests
{
    [Fact]
    public void ProcessPayloads_ShouldThrowOnNull()
    {
        var aggregator = new ClusterMetricsAggregator(TimeProvider.System);
        Should.Throw<ArgumentNullException>(() => aggregator.ProcessPayloads(null!));
    }

    [Fact]
    public void ProcessPayloads_ShouldTrackAndAggregateNewNodes()
    {
        var timeProvider = new FakeTimeProvider();
        var aggregator = new ClusterMetricsAggregator(timeProvider);

        var nodeId = Guid.NewGuid();
        var payloads = new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[]
                {
                    new MetricSnapshotDto { Name = "test.metric", Type = "Gauge", Value = 100 }
                }
            }
        };

        aggregator.ProcessPayloads(payloads);

        var tracked = aggregator.GetTrackedNodeIds();
        tracked.ShouldContain(nodeId);

        var result = aggregator.AggregateClusterMetrics(new HashSet<Guid> { nodeId });
        result.Count.ShouldBe(1);
        result[0].Name.ShouldBe("test.metric");
        result[0].Sum.ShouldBe(100);
        result[0].Min.ShouldBe(100);
        result[0].Max.ShouldBe(100);
        result[0].NodeCount.ShouldBe(1);
    }

    [Fact]
    public void ProcessPayloads_ShouldPruneDepartedNodes()
    {
        var timeProvider = new FakeTimeProvider();
        var aggregator = new ClusterMetricsAggregator(timeProvider);

        var nodeId = Guid.NewGuid();
        var payloads = new[]
        {
            new TelemetryPayloadDto { NodeId = nodeId, Timestamp = timeProvider.GetUtcNow() }
        };

        aggregator.ProcessPayloads(payloads);
        aggregator.GetTrackedNodeIds().ShouldContain(nodeId);

        // Advance explicitly validating native temporal eviction boundaries beyond the 2-minute safety net explicitly natively
        timeProvider.Advance(TimeSpan.FromMinutes(3));

        aggregator.ProcessPayloads(Array.Empty<TelemetryPayloadDto>());
        aggregator.GetTrackedNodeIds().ShouldBeEmpty();
    }

    [Fact]
    public void GetRates_ShouldCalculateCorrectlyOverTime()
    {
        var timeProvider = new FakeTimeProvider();
        timeProvider.SetUtcNow(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
        
        var aggregator = new ClusterMetricsAggregator(timeProvider);
        var nodeId = Guid.NewGuid();

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "requests", Type = "Counter", Value = 10 } }
            }
        });

        timeProvider.Advance(TimeSpan.FromSeconds(10));

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "requests", Type = "Counter", Value = 5 } }
            }
        });

        var result = aggregator.AggregateClusterMetrics(new HashSet<Guid> { nodeId });
        var agg = result.Single(r => r.Name == "requests");
        
        agg.Sum.ShouldBe(15);
        agg.RatePerSecond.ShouldBe(0.5);
        agg.RatePerMinute.ShouldBe(30);
    }

    [Fact]
    public void ProcessPayloads_ShouldHandleHistogramAsDeltaAndCalculateRates()
    {
        var timeProvider = new FakeTimeProvider();
        timeProvider.SetUtcNow(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var aggregator = new ClusterMetricsAggregator(timeProvider);
        var nodeId = Guid.NewGuid();

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "request.duration", Type = "Histogram", Value = 100 } }
            }
        });

        // Advance explicitly tracking time series bounds reliably over 10 seconds natively
        timeProvider.Advance(TimeSpan.FromSeconds(10));

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "request.duration", Type = "Histogram", Value = 200 } }
            }
        });

        var result = aggregator.AggregateClusterMetrics(new HashSet<Guid> { nodeId });
        var agg = result.Single(r => r.Name == "request.duration");
        
        // Sum should explicitly accumulate discrete histogram measurement payloads successfully structurally natively
        agg.Sum.ShouldBe(300);
        
        // Rate per second: (300 - 100) / 10 = 20 (based on baseline difference logic)
        agg.RatePerSecond.ShouldBe(20);
        // Rate per minute: 20 * 60 = 1200
        agg.RatePerMinute.ShouldBe(1200);
    }

    [Fact]
    public void ProcessPayloads_ShouldPreventNegativeRatesForStrictCounters()
    {
        var timeProvider = new FakeTimeProvider();
        timeProvider.SetUtcNow(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var aggregator = new ClusterMetricsAggregator(timeProvider);
        var nodeId = Guid.NewGuid();

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "connections.total", Type = "ObservableCounter", Value = 500 } }
            }
        });

        timeProvider.Advance(TimeSpan.FromSeconds(10));

        // Simulates a system reboot where the absolute cumulative value naturally resets correctly.
        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "connections.total", Type = "ObservableCounter", Value = 10 } }
            }
        });

        var result = aggregator.AggregateClusterMetrics(new HashSet<Guid> { nodeId });
        var agg = result.Single(r => r.Name == "connections.total");
        
        // Ensure mathematically strict metric counters strictly clamp negative derivative drifts dynamically
        agg.RatePerSecond.ShouldBe(0);
        agg.RatePerMinute.ShouldBe(0);
        agg.Sum.ShouldBe(10);
    }

    [Fact]
    public void ProcessPayloads_ShouldAllowNegativeRatesForUpDownCounters()
    {
        var timeProvider = new FakeTimeProvider();
        timeProvider.SetUtcNow(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var aggregator = new ClusterMetricsAggregator(timeProvider);
        var nodeId = Guid.NewGuid();

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "queue.size", Type = "UpDownCounter", Value = 50 } }
            }
        });

        timeProvider.Advance(TimeSpan.FromSeconds(10));

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "queue.size", Type = "UpDownCounter", Value = -20 } }
            }
        });

        var result = aggregator.AggregateClusterMetrics(new HashSet<Guid> { nodeId });
        var agg = result.Single(r => r.Name == "queue.size");
        
        // Summing additive deltas naturally tracks queue depths successfully
        agg.Sum.ShouldBe(30);
        
        // UpDown counters explicitly allow tracking accurate negative reduction flows natively smoothly
        agg.RatePerSecond.ShouldBe(-2);
        agg.RatePerMinute.ShouldBe(-120);
    }

    [Fact]
    public void ProcessPayloads_ShouldHandleGaugesAsAbsolute()
    {
        var timeProvider = new FakeTimeProvider();
        timeProvider.SetUtcNow(new DateTimeOffset(2024, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var aggregator = new ClusterMetricsAggregator(timeProvider);
        var nodeId = Guid.NewGuid();

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "cpu.usage", Type = "Gauge", Value = 80 } }
            }
        });

        timeProvider.Advance(TimeSpan.FromSeconds(10));

        aggregator.ProcessPayloads(new[]
        {
            new TelemetryPayloadDto
            {
                NodeId = nodeId,
                Timestamp = timeProvider.GetUtcNow(),
                Metrics = new[] { new MetricSnapshotDto { Name = "cpu.usage", Type = "Gauge", Value = 40 } }
            }
        });

        var result = aggregator.AggregateClusterMetrics(new HashSet<Guid> { nodeId });
        var agg = result.Single(r => r.Name == "cpu.usage");
        
        // Gauges maintain distinct absolute state transitions explicitly replacing legacy states
        agg.Sum.ShouldBe(40);
        
        agg.RatePerSecond.ShouldBe(-4);
        agg.RatePerMinute.ShouldBe(-240);
    }
}