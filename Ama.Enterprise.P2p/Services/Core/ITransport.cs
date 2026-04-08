namespace Ama.Enterprise.P2p.Services.Core;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the outbound network transport capabilities for sending protocol-specific messages to peers.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public interface ITransport<in TMessage>
{
    /// <summary>
    /// Determines whether this transport can handle the specified peer endpoint.
    /// </summary>
    /// <param name="endpoint">The peer endpoint to evaluate.</param>
    /// <returns><c>true</c> if the transport can handle the endpoint; otherwise, <c>false</c>.</returns>
    bool CanHandle(PeerEndpoint endpoint);

    /// <summary>
    /// Sends a protocol message to a specific peer endpoint.
    /// </summary>
    /// <param name="endpoint">The destination peer's network endpoint.</param>
    /// <param name="message">The message to send.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    Task SendAsync(PeerEndpoint endpoint, TMessage message, CancellationToken cancellationToken);
}