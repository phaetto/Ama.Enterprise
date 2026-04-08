namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Concurrent;
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
public sealed class TimeBasedFailureDetector(
    string meshId,
    IOptionsMonitor<FailureDetectorOptions> optionsMonitor,
    ILogger<TimeBasedFailureDetector> logger) : IFailureDetector
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IOptionsMonitor<FailureDetectorOptions> optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
    private readonly ILogger<TimeBasedFailureDetector> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<PeerId, DateTimeOffset> lastHeartbeats = new ConcurrentDictionary<PeerId, DateTimeOffset>();

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
}