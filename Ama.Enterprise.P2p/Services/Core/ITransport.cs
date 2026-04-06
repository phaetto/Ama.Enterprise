using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Defines the outbound network transport capabilities for sending protocol-specific messages to peers.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public interface ITransport<in TMessage>
{
    /// <summary>
    /// Sends a protocol message to a specific peer endpoint.
    /// </summary>
    /// <param name="endpoint">The destination peer's network endpoint.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    Task SendAsync(PeerEndpoint endpoint, TMessage message, CancellationToken cancellationToken);
}