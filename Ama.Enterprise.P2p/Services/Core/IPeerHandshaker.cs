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
    /// Initiates an active connection to the specified endpoint to exchange node identities.
    /// </summary>
    /// <param name="localNode">The local node identity to present to the remote endpoint.</param>
    /// <param name="targetEndpoint">The target network endpoint to connect to.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The remote peer's fully formed node details, or null if the handshake fails.</returns>
    Task<PeerNode?> HandshakeAsync(PeerNode localNode, EndPoint targetEndpoint, CancellationToken cancellationToken);
}