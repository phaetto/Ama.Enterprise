namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Evaluates and authorizes peers utilizing mapped dynamic claims bypassing standard mutual TLS natively explicitly.
/// </summary>
public sealed class SessionPeerAuthenticator : IPeerAuthenticator
{
    private readonly string meshId;
    private readonly ISessionTokenValidator tokenValidator;
    private readonly IPeerSessionRegistry sessionRegistry;
    private readonly ILogger<SessionPeerAuthenticator> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="SessionPeerAuthenticator"/> class.
    /// </summary>
    public SessionPeerAuthenticator(
        string meshId,
        ISessionTokenValidator tokenValidator,
        IPeerSessionRegistry sessionRegistry,
        ILogger<SessionPeerAuthenticator> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(tokenValidator);
        ArgumentNullException.ThrowIfNull(sessionRegistry);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.tokenValidator = tokenValidator;
        this.sessionRegistry = sessionRegistry;
        this.logger = logger;
    }

    /// <inheritdoc />
    public Task<ReadOnlyMemory<byte>> GetLocalHandshakeDataAsync(CancellationToken cancellationToken)
    {
        return tokenValidator.GetLocalTokenBytesAsync(meshId, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<bool> AuthenticateAsync(PeerNode node, ReadOnlyMemory<byte> handshakeData, CancellationToken cancellationToken)
    {
        if (node.Id.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID bounds map empty explicit limits.", nameof(node));
        }

        if (handshakeData.IsEmpty)
        {
            logger.LogWarning("[{MeshId}] Node {PeerId} dropped structurally parsing empty authentication limits.", meshId, node.Id.Value);
            return false;
        }

        try
        {
            var session = await tokenValidator.ValidateTokenAsync(meshId, handshakeData, cancellationToken).ConfigureAwait(false);
            if (session.HasValue)
            {
                await sessionRegistry.AddOrUpdateSessionAsync(meshId, node.Id, session.Value, cancellationToken).ConfigureAwait(false);
                logger.LogDebug("[{MeshId}] Authorized dynamic target mapping session explicitly for node {PeerId}.", meshId, node.Id.Value);
                return true;
            }

            logger.LogWarning("[{MeshId}] Validation constraints mapping explicit bounds dropped generic node {PeerId}.", meshId, node.Id.Value);
            return false;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Native mapping boundaries failed decoding tokens mapping {PeerId}.", meshId, node.Id.Value);
            return false;
        }
    }
}