namespace Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Default generic fallback topology provider mapping all active peers as natively expected within the causal scope explicitly.
/// </summary>
public sealed class DefaultScopeTopologyProvider : IScopeTopologyProvider
{
    /// <inheritdoc />
    public bool IsPeerExpected(string peerNetworkId)
    {
        if (string.IsNullOrWhiteSpace(peerNetworkId))
        {
            return false;
        }
        
        return true;
    }
}