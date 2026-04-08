namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// A default implementation of <see cref="IPeerAuthenticator"/> that accepts all peer connections.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="PassThroughPeerAuthenticator"/> class.
/// </remarks>
public sealed class PassThroughPeerAuthenticator(string meshId, ILogger<PassThroughPeerAuthenticator> logger) : IPeerAuthenticator
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly ILogger<PassThroughPeerAuthenticator> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public Task<bool> AuthenticateAsync(PeerNode node, ReadOnlyMemory<byte> handshakeData, CancellationToken cancellationToken)
    {
        if (node.Id.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(node));
        }

        logger.LogDebug("[{MeshId}] Auto-authenticating peer {PeerId} via pass-through strategy.", meshId, node.Id.Value);

        return Task.FromResult(true);
    }
}