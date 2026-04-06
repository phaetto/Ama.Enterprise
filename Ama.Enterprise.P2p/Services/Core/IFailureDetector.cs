using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Responsible for analyzing the health of peers and marking them as suspect or dead if they fail to respond.
/// </summary>
public interface IFailureDetector
{
    /// <summary>
    /// Records a successful interaction or heartbeat from a peer, resetting its failure suspicion.
    /// </summary>
    /// <param name="peerId">The unique identifier of the peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous record operation.</returns>
    Task RecordHeartbeatAsync(PeerId peerId, CancellationToken cancellationToken);

    /// <summary>
    /// Evaluates the current health status of a specific peer based on recent history.
    /// </summary>
    /// <param name="peerId">The unique identifier of the peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the calculated peer status.</returns>
    Task<PeerStatus> EvaluatePeerHealthAsync(PeerId peerId, CancellationToken cancellationToken);
}