using Ama.Enterprise.P2p.Models;
using Microsoft.Extensions.Logging;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// A default implementation of <see cref="IPeerAuthenticator"/> that accepts all peer connections.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="PassThroughPeerAuthenticator"/> class.
/// </remarks>
public sealed class PassThroughPeerAuthenticator(ILogger<PassThroughPeerAuthenticator> logger) : IPeerAuthenticator
{
    private readonly ILogger<PassThroughPeerAuthenticator> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public Task<bool> AuthenticateAsync(PeerNode node, ReadOnlyMemory<byte> handshakeData, CancellationToken cancellationToken)
    {
        if (node.Id.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(node));
        }

        this.logger.LogDebug("Auto-authenticating peer {PeerId} via pass-through strategy.", node.Id.Value);

        return Task.FromResult(true);
    }
}