namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Evaluates explicit session-based bounds isolating generic topologies for the ShowCase.
/// </summary>
public sealed class RbacScopeTopologyProvider : IScopeTopologyProvider
{
    private readonly IPeerSessionRegistry sessionRegistry;
    private readonly ShowCaseNodeContext context;
    private readonly ILogger<RbacScopeTopologyProvider> logger;

    public RbacScopeTopologyProvider(
        IPeerSessionRegistry sessionRegistry, 
        ShowCaseNodeContext context, 
        ILogger<RbacScopeTopologyProvider> logger)
    {
        this.sessionRegistry = sessionRegistry ?? throw new ArgumentNullException(nameof(sessionRegistry));
        this.context = context;
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async ValueTask<bool> IsPeerExpectedAsync(string peerNetworkId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(peerNetworkId)) return false;

        if (!Guid.TryParse(peerNetworkId, out var peerGuid)) return false;

        var peerId = new PeerId(peerGuid);

        SessionContext? session = null;
        try
        {
            // Note: Checking both configured meshes natively avoiding hardcoded dead-ends
            session = await sessionRegistry.GetSessionAsync("server", peerId, cancellationToken).ConfigureAwait(false);

            if (!session.HasValue)
            {
                session = await sessionRegistry.GetSessionAsync("user", peerId, cancellationToken).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to asynchronously resolve multi-mesh session context for peer '{PeerId}'.", peerId);
            return false;
        }

        if (!session.HasValue) return false;

        var claims = session.Value.Claims;

        if (!claims.TryGetValue("role", out var remoteRole) || 
            !claims.TryGetValue("region", out var remoteRegion))
        {
            return false;
        }

        var isLocalServer = string.IsNullOrEmpty(context.Role);
        var isRemoteServer = string.IsNullOrEmpty(remoteRole);

        // Rule 1: Admin and user data are completely separated. Servers have no explicit role and host/exchange everything securely natively.
        if (!isLocalServer && !isRemoteServer && !string.Equals(context.Role, remoteRole, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var localRegion = context.Region;

        // Rule 2 & 3: The flow of data is allowed from US to EU, but EU data must stay in the region.
        if (string.Equals(localRegion, "US", StringComparison.OrdinalIgnoreCase))
        {
            // US only expects data from US (EU data is rejected to stay in its region).
            return string.Equals(remoteRegion, "US", StringComparison.OrdinalIgnoreCase);
        }

        if (string.Equals(localRegion, "EU", StringComparison.OrdinalIgnoreCase))
        {
            // EU expects data from EU, and also from US (flow of data is allowed from US to EU).
            return string.Equals(remoteRegion, "EU", StringComparison.OrdinalIgnoreCase) || 
                   string.Equals(remoteRegion, "US", StringComparison.OrdinalIgnoreCase);
        }

        // Fallback for any other regions: strict bidirectional isolation.
        return string.Equals(localRegion, remoteRegion, StringComparison.OrdinalIgnoreCase);
    }
}