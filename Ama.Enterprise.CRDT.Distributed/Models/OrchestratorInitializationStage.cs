namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Represents the lifecycle initialization stage of the CRDT document orchestrator.
/// </summary>
public enum OrchestratorInitializationStage
{
    /// <summary>
    /// The orchestrator has not started initializing yet.
    /// </summary>
    NotStarted = 0,

    /// <summary>
    /// The orchestrator is currently initializing its registry and active documents.
    /// </summary>
    Initializing = 1,

    /// <summary>
    /// The orchestrator has finished initializing and is ready for operations.
    /// </summary>
    Initialized = 2
}