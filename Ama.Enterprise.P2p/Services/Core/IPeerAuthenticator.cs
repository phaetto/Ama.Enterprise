namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Provides security and validation to ensure only authorized peers can join the gossip network.
/// </summary>
public interface IPeerAuthenticator
{
    /// <summary>
    /// Retrieves the local security handshake payload explicitly mapped for outbound active handshakes natively.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The raw memory byte slice representing the outbound credentials safely natively.</returns>
    Task<ReadOnlyMemory<byte>> GetLocalHandshakeDataAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Authenticates an incoming or outgoing peer connection based on handshake credentials.
    /// </summary>
    /// <param name="node">The peer attempting to authenticate.</param>
    /// <param name="handshakeData">The security payload or token provided by the peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result is true if authentication succeeds, false otherwise.</returns>
    Task<bool> AuthenticateAsync(PeerNode node, ReadOnlyMemory<byte> handshakeData, CancellationToken cancellationToken);
}