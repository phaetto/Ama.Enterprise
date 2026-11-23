namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Evaluates session-based RBAC and regional sovereignty boundaries per CRDT scope for the Topology ShowCase.
/// </summary>
public sealed class RbacScopeTopologyProvider : IScopeTopologyProvider
{
    private readonly IPeerSessionRegistry sessionRegistry;
    private readonly ShowCaseNodeContext context;
    private readonly IRbacScopeAccessEvaluator accessEvaluator;
    private readonly ILogger<RbacScopeTopologyProvider> logger;

    public RbacScopeTopologyProvider(
        IPeerSessionRegistry sessionRegistry,
        ShowCaseNodeContext context,
        IRbacScopeAccessEvaluator accessEvaluator,
        ILogger<RbacScopeTopologyProvider> logger)
    {
        this.sessionRegistry = sessionRegistry ?? throw new ArgumentNullException(nameof(sessionRegistry));
        this.context = context ?? throw new ArgumentNullException(nameof(context));
        this.accessEvaluator = accessEvaluator ?? throw new ArgumentNullException(nameof(accessEvaluator));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async ValueTask<bool> CanAcceptScopeFromPeerAsync(
        ScopeAccessContext accessContext,
        CancellationToken cancellationToken = default)
    {
        var session = await TryGetPeerSessionAsync(accessContext.PeerNetworkId, accessContext.MeshId, cancellationToken).ConfigureAwait(false);
        if (!session.HasValue)
        {
            return false;
        }

        var claims = session.Value.Claims;
        if (!claims.TryGetValue("role", out var remoteRole) ||
            !claims.TryGetValue("region", out var remoteRegion))
        {
            return false;
        }

        // Both the local node and the remote sender must be authorized to participate in this scope.
        if (!accessEvaluator.IsNodeAuthorizedForScope(context.Role, context.Region, accessContext.ScopeId))
        {
            return false;
        }

        if (!accessEvaluator.IsNodeAuthorizedForScope(remoteRole, remoteRegion, accessContext.ScopeId))
        {
            return false;
        }

        return true;
    }

    /// <inheritdoc />
    public async ValueTask<bool> CanReplicateScopeToPeerAsync(
        ScopeAccessContext accessContext,
        CancellationToken cancellationToken = default)
    {
        var session = await TryGetPeerSessionAsync(accessContext.PeerNetworkId, accessContext.MeshId, cancellationToken).ConfigureAwait(false);
        if (!session.HasValue)
        {
            return false;
        }

        var claims = session.Value.Claims;
        if (!claims.TryGetValue("role", out var remoteRole) ||
            !claims.TryGetValue("region", out var remoteRegion))
        {
            return false;
        }

        if (!accessEvaluator.IsNodeAuthorizedForScope(context.Role, context.Region, accessContext.ScopeId))
        {
            return false;
        }

        if (!accessEvaluator.IsNodeAuthorizedForScope(remoteRole, remoteRegion, accessContext.ScopeId))
        {
            return false;
        }

        return true;
    }

    private async ValueTask<SessionContext?> TryGetPeerSessionAsync(
        string peerNetworkId,
        string? meshId,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(peerNetworkId) || !Guid.TryParse(peerNetworkId, out var peerGuid))
        {
            return null;
        }

        var peerId = new PeerId(peerGuid);

        try
        {
            if (!string.IsNullOrWhiteSpace(meshId))
            {
                var explicitSession = await sessionRegistry.GetSessionAsync(meshId, peerId, cancellationToken).ConfigureAwait(false);
                if (explicitSession.HasValue)
                {
                    return explicitSession;
                }
            }

            var session = await sessionRegistry.GetSessionAsync("server", peerId, cancellationToken).ConfigureAwait(false);
            if (!session.HasValue)
            {
                session = await sessionRegistry.GetSessionAsync("user", peerId, cancellationToken).ConfigureAwait(false);
            }

            return session;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to resolve multi-mesh session context for peer '{PeerId}'.", peerId);
            return null;
        }
    }
}