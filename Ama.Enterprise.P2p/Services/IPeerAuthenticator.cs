using Ama.Enterprise.P2p.Models;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Provides security and validation to ensure only authorized peers can join the gossip network.
/// </summary>
public interface IPeerAuthenticator
{
    /// <summary>
    /// Authenticates an incoming or outgoing peer connection based on handshake credentials.
    /// </summary>
    /// <param name="node">The peer attempting to authenticate.</param>
    /// <param name="handshakeData">The security payload or token provided by the peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result is true if authentication succeeds, false otherwise.</returns>
    Task<bool> AuthenticateAsync(PeerNode node, ReadOnlyMemory<byte> handshakeData, CancellationToken cancellationToken);
}