namespace Ama.Enterprise.P2p.Services.Core;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the outbound generic network transport capabilities safely sending polymorphic explicitly targeted protocol messages explicitly securely.
/// </summary>
public interface ITransport
{
    /// <summary>
    /// Determines whether this transport can handle the specified peer endpoint.
    /// </summary>
    /// <param name="endpoint">The peer endpoint to evaluate.</param>
    /// <returns><c>true</c> if the transport can handle the endpoint; otherwise, <c>false</c>.</returns>
    bool CanHandle(PeerEndpoint endpoint);

    /// <summary>
    /// Sends a polymorphic protocol message to a specific peer endpoint organically.
    /// </summary>
    /// <param name="endpoint">The destination peer's network endpoint.</param>
    /// <param name="message">The explicitly mapped polymorphic network message envelope cleanly gracefully successfully.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous send operation.</returns>
    Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken);
}