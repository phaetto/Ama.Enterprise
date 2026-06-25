namespace Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Contract evaluating expected peer topological inclusions dynamically isolating scope-level causality boundaries.
/// </summary>
/// <example>
/// <code>
/// public sealed class ClaimBasedTopologyProvider : IScopeTopologyProvider
/// {
///     private readonly IPeerSessionRegistry sessionRegistry;
///     
///     public ClaimBasedTopologyProvider(IPeerSessionRegistry sessionRegistry)
///     {
///         this.sessionRegistry = sessionRegistry;
///     }
///     
///     public bool IsPeerExpected(string peerNetworkId)
///     {
///         // Example: check if peer has an active Admin session
///         return sessionRegistry.HasClaim(peerNetworkId, "role", "admin");
///     }
/// }
/// </code>
/// </example>
public interface IScopeTopologyProvider
{
    /// <summary>
    /// Determines whether a specific remote peer is expected to participate in the current CRDT causal scope.
    /// </summary>
    /// <param name="peerNetworkId">The network identifier of the remote peer.</param>
    /// <returns>True if the peer is allowed and expected; otherwise, false.</returns>
    bool IsPeerExpected(string peerNetworkId);
}