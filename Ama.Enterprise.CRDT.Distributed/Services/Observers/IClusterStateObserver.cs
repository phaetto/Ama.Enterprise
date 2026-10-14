namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

/// <summary>
/// Observer contract for tracking topological state modifications directly within the CRDT Cluster State Tracker natively.
/// </summary>
public interface IClusterStateObserver
{
    /// <summary>
    /// Invoked when the cluster state tracker detects a change in the active replica topology bounding dynamic limits.
    /// </summary>
    void OnClusterTopologyChanged();
}