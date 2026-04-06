using System.Collections.Concurrent;
using Ama.Enterprise.P2p.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Determines peer health based on the time elapsed since the last received heartbeat.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="TimeBasedFailureDetector"/> class.
/// </remarks>
public sealed class TimeBasedFailureDetector(
    IOptions<GossipOptions> options,
    ILogger<TimeBasedFailureDetector> logger) : IFailureDetector
{
    private readonly IOptions<GossipOptions> options = options ?? throw new ArgumentNullException(nameof(options));
    private readonly ILogger<TimeBasedFailureDetector> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    private readonly ConcurrentDictionary<PeerId, DateTimeOffset> lastHeartbeats = new ConcurrentDictionary<PeerId, DateTimeOffset>();

    /// <inheritdoc />
    public Task RecordHeartbeatAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(peerId));
        }

        this.lastHeartbeats.AddOrUpdate(
            peerId,
            _ => DateTimeOffset.UtcNow,
            (_, _) => DateTimeOffset.UtcNow);

        this.logger.LogTrace("Recorded heartbeat for peer {PeerId}.", peerId.Value);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task<PeerStatus> EvaluatePeerHealthAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        if (peerId.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(peerId));
        }

        if (!this.lastHeartbeats.TryGetValue(peerId, out var lastSeen))
        {
            // If we have never seen a heartbeat, we assume it's Dead or uninitialized.
            return Task.FromResult(PeerStatus.Dead);
        }

        var timeSinceLastHeartbeat = DateTimeOffset.UtcNow - lastSeen;
        var gossipInterval = this.options.Value.GossipInterval;

        // Suspect if missed 3 gossip intervals
        var suspectThreshold = TimeSpan.FromTicks(gossipInterval.Ticks * 3);
        
        // Dead if missed 6 gossip intervals
        var deadThreshold = TimeSpan.FromTicks(gossipInterval.Ticks * 6);

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