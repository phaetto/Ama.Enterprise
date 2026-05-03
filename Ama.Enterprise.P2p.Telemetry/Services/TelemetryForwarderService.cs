namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Telemetry.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Built-in hosted evaluation isolating pure native zero dependency MeterListeners evaluating targeted asynchronous payloads natively broadcasting explicit network scopes.
/// </summary>
public sealed class TelemetryForwarderService : BackgroundService
{
    private readonly MeterListener meterListener = new();
    private readonly ConcurrentDictionary<string, MetricState> activeMetrics = new();
    
    private readonly IOptionsMonitor<TelemetryOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly IEnumerable<IP2pProtocol> p2pProtocols;
    private readonly ILogger<TelemetryForwarderService> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="TelemetryForwarderService"/> class.
    /// </summary>
    public TelemetryForwarderService(
        IOptionsMonitor<TelemetryOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        IEnumerable<IP2pProtocol> p2pProtocols,
        ILogger<TelemetryForwarderService> logger)
    {
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.p2pProtocols = p2pProtocols ?? throw new ArgumentNullException(nameof(p2pProtocols));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == Constants.CoreGossipMeterName ||
                instrument.Meter.Name == Constants.PushPullGossipMeterName)
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };

        meterListener.SetMeasurementEventCallback<long>(OnMeasurementRecorded);
        meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
        meterListener.SetMeasurementEventCallback<double>(OnMeasurementRecorded);
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = optionsMonitor.CurrentValue;
        if (!options.IsEnabled)
        {
            return;
        }

        logger.LogInformation("Starting isolated metric forwarder pipeline mapping exact aggregated network states.");
        meterListener.Start();

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(options.FlushInterval, stoppingToken).ConfigureAwait(false);

                var activeOptions = optionsMonitor.CurrentValue;
                if (!activeOptions.IsEnabled)
                {
                    continue;
                }

                meterListener.RecordObservableInstruments();

                await BroadcastTelemetryAsync(activeOptions, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "An error occurred broadcasting metrics evaluating generic telemetry mappings.");
            }
        }
    }

    /// <inheritdoc />
    public override void Dispose()
    {
        meterListener.Dispose();
        base.Dispose();
    }

    private void OnMeasurementRecorded<T>(
        Instrument instrument, 
        T measurement, 
        ReadOnlySpan<KeyValuePair<string, object?>> tags, 
        object? state)
    {
        var options = optionsMonitor.CurrentValue;

        var mappedTags = new List<MetricTagDto>(tags.Length);
        foreach (var tag in tags)
        {
            var valueStr = tag.Value?.ToString() ?? string.Empty;
            
            if (string.Equals(tag.Key, "mesh_id", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(valueStr, options.TargetMeshId, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            mappedTags.Add(new MetricTagDto(tag.Key, valueStr));
        }

        var sortedTags = mappedTags.OrderBy(t => t.Key).ToList();
        var tagKey = string.Join(",", sortedTags.Select(t => $"{t.Key}={t.Value}"));
        var uniqueKey = $"{instrument.Name}|{tagKey}";

        var metricType = "Counter";
        if (instrument.IsObservable)
        {
            metricType = "Gauge";
        }
        else if (instrument.Name.EndsWith("_size", StringComparison.OrdinalIgnoreCase) || 
                 instrument.Name.EndsWith("_bytes", StringComparison.OrdinalIgnoreCase))
        {
            metricType = "Histogram";
        }

        var activeState = activeMetrics.GetOrAdd(uniqueKey, _ => new MetricState(instrument.Name, metricType, sortedTags));

        long numericValue = measurement switch
        {
            long l => l,
            int i => i,
            double d => (long)d,
            _ => 0
        };

        activeState.Record(numericValue, instrument.IsObservable);
    }

    private async Task BroadcastTelemetryAsync(TelemetryOptions options, CancellationToken cancellationToken)
    {
        var snapshots = new List<MetricSnapshotDto>();
        foreach (var kvp in activeMetrics)
        {
            var val = kvp.Value.GetAndResetValue();
            
            if (val == 0 && kvp.Value.Type != "Gauge")
            {
                continue;
            }

            snapshots.Add(new MetricSnapshotDto
            {
                Name = kvp.Value.InstrumentName,
                Type = kvp.Value.Type,
                Value = val,
                Tags = kvp.Value.Tags
            });
        }

        if (snapshots.Count == 0)
        {
            return;
        }

        var nodeOptions = nodeOptionsMonitor.Get(options.TargetMeshId);
        if (nodeOptions is null || nodeOptions.LocalPeerId == Guid.Empty)
        {
            logger.LogWarning("Telemetry targeting explicit network boundary {TargetMeshId} missing distinct local node configuration mapping.", options.TargetMeshId);
            return;
        }

        var payload = new TelemetryPayloadDto
        {
            NodeId = nodeOptions.LocalPeerId,
            Timestamp = DateTimeOffset.UtcNow,
            Metrics = snapshots
        };

        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(payload, TelemetryJsonContext.Default.TelemetryPayloadDto);

        var dispatchTasks = new List<Task>();
        foreach (var protocol in p2pProtocols)
        {
            dispatchTasks.Add(protocol.BroadcastAsync(payloadBytes, cancellationToken));
        }

        logger.LogDebug("Evaluating {Count} telemetry snapshot boundaries broadcasting securely isolated metrics natively.", snapshots.Count);

        await Task.WhenAll(dispatchTasks).ConfigureAwait(false);
    }

    private sealed class MetricState
    {
        private long sumValue;

        public MetricState(string instrumentName, string type, IReadOnlyList<MetricTagDto> tags)
        {
            InstrumentName = instrumentName;
            Type = type;
            Tags = tags;
        }

        public string InstrumentName { get; }

        public string Type { get; }

        public IReadOnlyList<MetricTagDto> Tags { get; }

        public void Record(long value, bool isGauge)
        {
            if (isGauge)
            {
                Interlocked.Exchange(ref sumValue, value);
            }
            else
            {
                Interlocked.Add(ref sumValue, value);
            }
        }

        public long GetAndResetValue()
        {
            if (string.Equals(Type, "Gauge", StringComparison.OrdinalIgnoreCase))
            {
                return Interlocked.Read(ref sumValue);
            }

            return Interlocked.Exchange(ref sumValue, 0);
        }
    }
}