namespace Ama.Enterprise.CRDT.Distributed.Services.Sharding;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines rules for deciding whether CRDT scope data can be accepted from or replicated to remote peers.
/// </summary>
/// <remarks>
/// Implement this interface to filter peers based on authentication claims, tenant boundaries,
/// or regional rules. Use <see cref="CanAcceptScopeFromPeerAsync"/> to control inbound state, patches,
/// and requests, and <see cref="CanReplicateScopeToPeerAsync"/> to control outbound state, patches,
/// snapshots, and version vectors.
/// </remarks>
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
///     public async ValueTask&lt;bool&gt; CanAcceptScopeFromPeerAsync(ScopeAccessContext context, CancellationToken cancellationToken = default)
///     {
///         if (context.ScopeId == "read-only-replica" || !Guid.TryParse(context.PeerNetworkId, out var peerGuid))
///         {
///             return false;
///         }
///         
///         var session = await sessionRegistry.GetSessionAsync(context.MeshId ?? "mesh-id", new PeerId(peerGuid), cancellationToken);
///         return session?.HasClaim("role", "admin") ?? false;
///     }
///     
///     public async ValueTask&lt;bool&gt; CanReplicateScopeToPeerAsync(ScopeAccessContext context, CancellationToken cancellationToken = default)
///     {
///         if (!Guid.TryParse(context.PeerNetworkId, out var peerGuid))
///         {
///             return false;
///         }
///         
///         var session = await sessionRegistry.GetSessionAsync(context.MeshId ?? "mesh-id", new PeerId(peerGuid), cancellationToken);
///         return session?.HasClaim("role", "admin") ?? false;
///     }
/// }
/// </code>
/// </example>
public interface IScopeTopologyProvider
{
    /// <summary>
    /// Checks whether the local node can accept incoming state, patches, or requests for a scope from a remote peer.
    /// </summary>
    /// <param name="context">Details about the scope, the remote peer, the mesh, and the message type being evaluated.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous check.</param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> that resolves to <see langword="true"/> if the incoming data or request is allowed;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    ValueTask<bool> CanAcceptScopeFromPeerAsync(
        ScopeAccessContext context,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Checks whether the local node can send state, patches, snapshots, or version vectors for a scope to a remote peer.
    /// </summary>
    /// <param name="context">Details about the scope, the remote peer, the mesh, and the message type being evaluated.</param>
    /// <param name="cancellationToken">A token to cancel the asynchronous check.</param>
    /// <returns>
    /// A <see cref="ValueTask{TResult}"/> that resolves to <see langword="true"/> if data for the scope can be sent to the peer;
    /// otherwise, <see langword="false"/>.
    /// </returns>
    ValueTask<bool> CanReplicateScopeToPeerAsync(
        ScopeAccessContext context,
        CancellationToken cancellationToken = default);
}