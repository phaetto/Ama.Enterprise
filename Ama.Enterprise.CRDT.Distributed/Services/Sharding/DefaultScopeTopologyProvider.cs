namespace Ama.Enterprise.CRDT.Distributed.Services.Sharding;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Default fallback topology provider that allows all active peers with non-empty network identifiers.
/// </summary>
public sealed class DefaultScopeTopologyProvider : IScopeTopologyProvider
{
    /// <inheritdoc />
    public ValueTask<bool> CanAcceptScopeFromPeerAsync(
        ScopeAccessContext context,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(context.PeerNetworkId))
        {
            return new ValueTask<bool>(false);
        }

        return new ValueTask<bool>(true);
    }

    /// <inheritdoc />
    public ValueTask<bool> CanReplicateScopeToPeerAsync(
        ScopeAccessContext context,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(context.PeerNetworkId))
        {
            return new ValueTask<bool>(false);
        }

        return new ValueTask<bool>(true);
    }
}