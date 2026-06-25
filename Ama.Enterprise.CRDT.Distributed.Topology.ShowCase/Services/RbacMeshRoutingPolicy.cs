namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Evaluates real-time P2P broadcast routing preventing Gossip payloads from leaking across boundaries.
/// </summary>
public sealed class RbacMeshRoutingPolicy : IMeshRoutingPolicy
{
    private readonly ShowCaseNodeContext context;

    public RbacMeshRoutingPolicy(ShowCaseNodeContext context)
    {
        this.context = context ?? throw new ArgumentNullException(nameof(context));
    }

    /// <inheritdoc />
    public Task<bool> EvaluateInboundAsync(string meshId, SessionContext session, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (!session.Claims.TryGetValue("role", out var remoteRole) || 
            !session.Claims.TryGetValue("region", out var remoteRegion))
        {
            return Task.FromResult(false);
        }

        // Inbound flow: Remote is sending data to Local.
        return Task.FromResult(IsDataFlowAllowed(sourceRole: remoteRole, sourceRegion: remoteRegion, targetRole: context.Role, targetRegion: context.Region));
    }

    /// <inheritdoc />
    public Task<bool> EvaluateOutboundAsync(string meshId, SessionContext session, IMeshMessage message, CancellationToken cancellationToken)
    {
        if (!session.Claims.TryGetValue("role", out var remoteRole) || 
            !session.Claims.TryGetValue("region", out var remoteRegion))
        {
            return Task.FromResult(false);
        }

        // Outbound flow: Local is sending data to Remote.
        return Task.FromResult(IsDataFlowAllowed(sourceRole: context.Role, sourceRegion: context.Region, targetRole: remoteRole, targetRegion: remoteRegion));
    }

    private static bool IsDataFlowAllowed(string sourceRole, string sourceRegion, string targetRole, string targetRegion)
    {
        var isSourceServer = string.IsNullOrEmpty(sourceRole);
        var isTargetServer = string.IsNullOrEmpty(targetRole);

        // Rule 1: Admin and user data are completely separated. Servers have no explicit role and host/exchange everything securely natively.
        if (!isSourceServer && !isTargetServer && !string.Equals(sourceRole, targetRole, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // Rule 2 & 3: The flow of data is allowed from US to EU, but EU data must stay in the region.
        if (string.Equals(targetRegion, "US", StringComparison.OrdinalIgnoreCase))
        {
            // US only expects data from US (EU data is rejected to stay in its region).
            return string.Equals(sourceRegion, "US", StringComparison.OrdinalIgnoreCase);
        }

        if (string.Equals(targetRegion, "EU", StringComparison.OrdinalIgnoreCase))
        {
            // EU expects data from EU, and also from US (flow of data is allowed from US to EU).
            return string.Equals(sourceRegion, "EU", StringComparison.OrdinalIgnoreCase) || 
                   string.Equals(sourceRegion, "US", StringComparison.OrdinalIgnoreCase);
        }

        // Fallback for any other regions: strict bidirectional isolation.
        return string.Equals(sourceRegion, targetRegion, StringComparison.OrdinalIgnoreCase);
    }
}