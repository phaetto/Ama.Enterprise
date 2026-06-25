namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;

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
///     public async ValueTask&lt;bool&gt; IsPeerExpectedAsync(string peerNetworkId, CancellationToken cancellationToken = default)
///     {
///         // Example: check if peer has an active Admin session
///         if (!Guid.TryParse(peerNetworkId, out var peerGuid)) return false;
///         var session = await sessionRegistry.GetSessionAsync("mesh-id", new PeerId(peerGuid), cancellationToken);
///         return session?.HasClaim("role", "admin") ?? false;
///     }
/// }
/// </code>
/// </example>
public interface IScopeTopologyProvider
{
    /// <summary>
    /// Determines whether a specific remote peer is expected to participate in the current CRDT causal scope asynchronously.
    /// </summary>
    /// <param name="peerNetworkId">The network identifier of the remote peer.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the work.</param>
    /// <returns>True if the peer is allowed and expected; otherwise, false.</returns>
    ValueTask<bool> IsPeerExpectedAsync(string peerNetworkId, CancellationToken cancellationToken = default);
}