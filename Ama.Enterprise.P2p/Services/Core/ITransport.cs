namespace Ama.Enterprise.P2p.Services.Core;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the outbound network transport capabilities safely sending protocol-specific multiplexed messages explicitly targeting endpoints organically explicitly securely.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public interface ITransport<TMessage> where TMessage : IMeshMessage
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