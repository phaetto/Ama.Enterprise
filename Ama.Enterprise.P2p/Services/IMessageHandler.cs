using Ama.Enterprise.P2p.Models;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Defines a domain-level consumer for gossip messages (e.g., the component that merges CRDTs).
/// </summary>
public interface IMessageHandler
{
    /// <summary>
    /// Processes an incoming payload received from the gossip network.
    /// </summary>
    /// <param name="message">The gossip message containing the domain payload.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous handling operation.</returns>
    Task HandleAsync(GossipMessage message, CancellationToken cancellationToken);
}