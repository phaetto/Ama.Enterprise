namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Determines peer health based on the time elapsed since the last received heartbeat, decoupled from protocol-specific options.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="TimeBasedFailureDetector"/> class.
/// </remarks>
public sealed class TimeBasedFailureDetector : IFailureDetector, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<FailureDetectorOptions> optionsMonitor;
    private readonly ILogger<TimeBasedFailureDetector> logger;

    private readonly ConcurrentDictionary<PeerId, DateTimeOffset> lastHeartbeats = new ConcurrentDictionary<PeerId, DateTimeOffset>();

    private readonly Meter meter;
    private readonly Counter<long> heartbeatsRecordedCounter;
    private readonly Counter<long> evaluationsCounter;

    public TimeBasedFailureDetector(
        string meshId,
        IOptionsMonitor<FailureDetectorOptions> optionsMonitor,
        ILogger<TimeBasedFailureDetector> logger,
        IMeterFactory? meterFactory = null)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.TimeBasedFailureDetector") ?? new Meter("Ama.Enterprise.P2p.TimeBasedFailureDetector");
        
        this.heartbeatsRecordedCounter = this.meter.CreateCounter<long>(
            "p2p.failure_detector.heartbeats_recorded", 
            "heartbeats", 
            "Total peer heartbeats successfully recorded");
            
        this.evaluationsCounter = this.meter.CreateCounter<long>(
            "p2p.failure_detector.evaluations", 
            "evaluations", 
            "Total peer health evaluations processed");

        this.meter.CreateObservableGauge(
            "p2p.failure_detector.tracked_peers",
            () => new Measurement<int>(
                lastHeartbeats.Count,
                new KeyValuePair<string, object?>("mesh_id", meshId)),
            "peers",
            "Current number of peers tracked by the failure detector");
    }

    /// <inheritdoc />
    public Task RecordHeartbeatAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(peerId));
        }

        lastHeartbeats.AddOrUpdate(
            peerId,
            _ => DateTimeOffset.UtcNow,
            (_, _) => DateTimeOffset.UtcNow);

        heartbeatsRecordedCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));

        logger.LogTrace("[{MeshId}] Recorded heartbeat for peer {PeerId}.", meshId, peerId.Value);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<PeerStatus> EvaluatePeerHealthAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(peerId));
        }

        evaluationsCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));

        if (!lastHeartbeats.TryGetValue(peerId, out var lastSeen))
        {
            // If we have never seen a heartbeat, we assume it's Dead or uninitialized.
            return Task.FromResult(PeerStatus.Dead);
        }

        var timeSinceLastHeartbeat = DateTimeOffset.UtcNow - lastSeen;
        var options = optionsMonitor.Get(meshId);
        var heartbeatInterval = options.HeartbeatInterval;

        var suspectThreshold = TimeSpan.FromTicks(heartbeatInterval.Ticks * options.SuspectThresholdMultiplier);
        var deadThreshold = TimeSpan.FromTicks(heartbeatInterval.Ticks * options.DeadThresholdMultiplier);

        if (timeSinceLastHeartbeat >= deadThreshold)
        {
            return Task.FromResult(PeerStatus.Dead);
        }

        if (timeSinceLastHeartbeat >= suspectThreshold)
        {
            return Task.FromResult(PeerStatus.Suspect);
        }

        return Task.FromResult(PeerStatus.Active);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}