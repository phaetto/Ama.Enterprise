namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Default generic fallback topology provider mapping all active peers as natively expected within the causal scope explicitly.
/// </summary>
public sealed class DefaultScopeTopologyProvider : IScopeTopologyProvider
{
    /// <inheritdoc />
    public ValueTask<bool> IsPeerExpectedAsync(string peerNetworkId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(peerNetworkId))
        {
            return new ValueTask<bool>(false);
        }
        
        return new ValueTask<bool>(true);
    }
}