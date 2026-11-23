namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Evaluates real-time P2P transport routing by inspecting the CRDT envelope's ScopeId and enforcing role and regional sovereignty boundaries.
/// </summary>
public sealed class RbacMeshRoutingPolicy : IMeshRoutingPolicy
{
    private readonly ShowCaseNodeContext context;
    private readonly IRbacScopeAccessEvaluator accessEvaluator;
    private readonly ICrdtSerializer? serializer;

    public RbacMeshRoutingPolicy(
        ShowCaseNodeContext context,
        IRbacScopeAccessEvaluator accessEvaluator,
        ICrdtSerializer? serializer = null)
    {
        this.context = context ?? throw new ArgumentNullException(nameof(context));
        this.accessEvaluator = accessEvaluator ?? throw new ArgumentNullException(nameof(accessEvaluator));
        this.serializer = serializer;
    }

    /// <inheritdoc />
    public Task<bool> EvaluateInboundAsync(string meshId, SessionContext session, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (!session.Claims.TryGetValue("role", out var remoteRole) ||
            !session.Claims.TryGetValue("region", out var remoteRegion))
        {
            return Task.FromResult(false);
        }

        var scopeId = TryExtractScopeId(payload);
        if (!string.IsNullOrWhiteSpace(scopeId))
        {
            var allowed = accessEvaluator.IsNodeAuthorizedForScope(context.Role, context.Region, scopeId) &&
                          accessEvaluator.IsNodeAuthorizedForScope(remoteRole, remoteRegion, scopeId);
            return Task.FromResult(allowed);
        }

        return Task.FromResult(accessEvaluator.IsPhysicalNodeFlowAllowed(sourceRole: remoteRole, sourceRegion: remoteRegion, targetRole: context.Role, targetRegion: context.Region));
    }

    /// <inheritdoc />
    public Task<bool> EvaluateOutboundAsync(string meshId, SessionContext session, IMeshMessage message, CancellationToken cancellationToken)
    {
        if (!session.Claims.TryGetValue("role", out var remoteRole) ||
            !session.Claims.TryGetValue("region", out var remoteRegion))
        {
            return Task.FromResult(false);
        }

        if (message is GossipMessage gossipMessage && !gossipMessage.Payload.IsEmpty)
        {
            var scopeId = TryExtractScopeId(gossipMessage.Payload);
            if (!string.IsNullOrWhiteSpace(scopeId))
            {
                var allowed = accessEvaluator.IsNodeAuthorizedForScope(context.Role, context.Region, scopeId) &&
                              accessEvaluator.IsNodeAuthorizedForScope(remoteRole, remoteRegion, scopeId);
                return Task.FromResult(allowed);
            }
        }

        return Task.FromResult(accessEvaluator.IsPhysicalNodeFlowAllowed(sourceRole: context.Role, sourceRegion: context.Region, targetRole: remoteRole, targetRegion: remoteRegion));
    }

    private string? TryExtractScopeId(ReadOnlyMemory<byte> payload)
    {
        if (serializer == null || payload.IsEmpty)
        {
            return null;
        }

        try
        {
            var wrapper = serializer.DeserializeFromBytes<CrdtMessageWrapper>(payload.Span);
            return wrapper.ScopeId;
        }
        catch
        {
            return null;
        }
    }
}