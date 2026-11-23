namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

/// <summary>
/// Evaluates role and regional access rules for CRDT scopes and physical nodes in the Topology ShowCase.
/// </summary>
/// <example>
/// <code>
/// public sealed class MyRoutingService
/// {
///     private readonly IRbacScopeAccessEvaluator accessEvaluator;
///     
///     public MyRoutingService(IRbacScopeAccessEvaluator accessEvaluator)
///     {
///         this.accessEvaluator = accessEvaluator;
///     }
///     
///     public bool CanAccess(string role, string region, string scopeId)
///     {
///         return accessEvaluator.IsNodeAuthorizedForScope(role, region, scopeId);
///     }
/// }
/// </code>
/// </example>
public interface IRbacScopeAccessEvaluator
{
    /// <summary>
    /// Checks whether a node with the given role and region is allowed to host or access the specified scope ID.
    /// Scope ID format: "{originRegion}-{roleOrShared}" (for example: "eu-user", "eu-admin", "us-user", "eu-shared-doc1").
    /// </summary>
    /// <param name="nodeRole">The role of the node (empty string for server nodes, or "user" / "admin").</param>
    /// <param name="nodeRegion">The region of the node (for example: "US" or "EU").</param>
    /// <param name="scopeId">The logical scope identifier or composite replica identifier.</param>
    /// <returns><see langword="true"/> if the node is allowed to participate in the scope; otherwise, <see langword="false"/>.</returns>
    bool IsNodeAuthorizedForScope(string nodeRole, string nodeRegion, string? scopeId);

    /// <summary>
    /// Checks whether direct physical P2P traffic is allowed from a source node to a target node when no specific scope ID is attached.
    /// </summary>
    /// <param name="sourceRole">The role of the sending node.</param>
    /// <param name="sourceRegion">The region of the sending node.</param>
    /// <param name="targetRole">The role of the receiving node.</param>
    /// <param name="targetRegion">The region of the receiving node.</param>
    /// <returns><see langword="true"/> if physical node traffic is permitted; otherwise, <see langword="false"/>.</returns>
    bool IsPhysicalNodeFlowAllowed(string sourceRole, string sourceRegion, string targetRole, string targetRegion);
}