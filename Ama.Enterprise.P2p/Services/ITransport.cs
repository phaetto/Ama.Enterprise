using Ama.Enterprise.P2p.Models;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Defines the outbound network transport capabilities for sending messages to peers.
/// </summary>
public interface ITransport
{
    /// <summary>
    /// Sends a gossip message to a specific peer endpoint.
    /// </summary>
    /// <param name="endpoint">The destination peer's network endpoint.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    Task SendAsync(PeerEndpoint endpoint, GossipMessage message, CancellationToken cancellationToken);
}