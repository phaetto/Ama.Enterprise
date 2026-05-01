namespace Ama.Enterprise.P2p.Services.Core;

using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines a mechanism to actively negotiate and retrieve a remote peer's identity from an unresolved network endpoint.
/// Represents Phase 2 in a Two-Phase indirect discovery architecture.
/// </summary>
public interface IPeerHandshaker
{
    /// <summary>
    /// Gets the localized port this specific handshaker is actively listening on.
    /// This allows generic Phase 1 discovery mechanisms to broadcast the correct Phase 2 target natively.
    /// </summary>
    int LocalHandshakePort { get; }

    /// <summary>
    /// Initiates an active connection to the specified endpoint identifier to exchange node identities.
    /// </summary>
    /// <param name="localNode">The local node identity to present to the remote peer.</param>
    /// <param name="endpoint">The target network endpoint containing IP and designated Handshake port.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The remote peer's fully formed node details, or null if the handshake fails.</returns>
    Task<PeerNode?> HandshakeAsync(PeerNode localNode, IPEndPoint endpoint, CancellationToken cancellationToken);
}