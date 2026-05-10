namespace Ama.Enterprise.CRDT.Distributed.Services;

/// <summary>
/// Factory explicitly isolating localized state scopes creating decoupled generic persistent generic models dynamically executing distinct bounds.
/// </summary>
public interface IDistributedCrdtScopeFactory
{
    /// <summary>
    /// Constructs a distinct distributed CRDT boundary explicitly identifying a distinct multi-mesh instance natively avoiding shared states inherently.
    /// </summary>
    /// <param name="replicaId">The specific logical local replica ID.</param>
    IDistributedCrdtScope CreateScope(string replicaId);
}