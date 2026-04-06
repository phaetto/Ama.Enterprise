using Ama.Enterprise.P2p.Models;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Dispatches incoming, validated gossip messages to the relevant domain handlers (e.g., the CRDT engine).
/// </summary>
public interface IMessageDispatcher
{
    /// <summary>
    /// Routes the incoming gossip message to all registered handlers.
    /// </summary>
    /// <param name="message">The incoming gossip message.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous dispatch operation.</returns>
    Task DispatchAsync(GossipMessage message, CancellationToken cancellationToken);
}