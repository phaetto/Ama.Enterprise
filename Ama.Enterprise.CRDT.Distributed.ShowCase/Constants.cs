namespace Ama.Enterprise.CRDT.Distributed.ShowCase;

/// <summary>
/// Global constants for the ShowCase application to prevent magic strings and typos.
/// </summary>
public static class Constants
{
    /// <summary>
    /// The globally unique document identifier for the Task List CRDT state.
    /// </summary>
    public const string TaskListDocumentId = "task-list-doc";

    /// <summary>
    /// The globally unique document identifier for the Fleet Status CRDT state.
    /// </summary>
    public const string FleetDocumentId = "fleet-doc";
}