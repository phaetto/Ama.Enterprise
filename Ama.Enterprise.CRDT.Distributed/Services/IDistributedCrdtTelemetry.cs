namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;

/// <summary>
/// Centralizes all <see cref="System.Diagnostics.Metrics.Meter"/>, counters, and observable gauges for the distributed CRDT module so individual documents and scopes do not allocate duplicate telemetry instruments.
/// </summary>
/// <example>
/// <code>
/// telemetry.RecordDocumentPatchApplied(documentId);
/// telemetry.RecordOrchestratorCommandProcessed();
/// </code>
/// </example>
public interface IDistributedCrdtTelemetry
{
    /// <summary>
    /// Registers callbacks to observe the shared document command pipeline queue length and object pool size.
    /// </summary>
    /// <param name="queueLengthProvider">Function returning the current number of pending document commands.</param>
    /// <param name="poolSizeProvider">Function returning the current number of pooled document commands.</param>
    /// <returns>An <see cref="IDisposable"/> that unregisters the callbacks when disposed.</returns>
    IDisposable RegisterDocumentPipelineMetrics(Func<int> queueLengthProvider, Func<int> poolSizeProvider);

    /// <summary>
    /// Registers callbacks to observe an orchestrator's command channel queue length and object pool size.
    /// </summary>
    /// <param name="queueLengthProvider">Function returning the current number of pending orchestrator commands.</param>
    /// <param name="poolSizeProvider">Function returning the current number of pooled orchestrator commands.</param>
    /// <returns>An <see cref="IDisposable"/> that unregisters the callbacks when disposed.</returns>
    IDisposable RegisterOrchestratorPipelineMetrics(Func<int> queueLengthProvider, Func<int> poolSizeProvider);

    /// <summary>
    /// Records that a new distributed CRDT scope was generated.
    /// </summary>
    /// <param name="replicaId">The replica identifier of the created scope.</param>
    void RecordScopeGenerated(string replicaId);

    /// <summary>
    /// Records that a document was created or promoted in the orchestrator.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordOrchestratorDocumentCreated(string documentId);

    /// <summary>
    /// Records that a document was deleted from the orchestrator.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordOrchestratorDocumentDeleted(string documentId);

    /// <summary>
    /// Records that an anti-entropy synchronization message was dispatched.
    /// </summary>
    /// <param name="replicaId">The local replica identifier.</param>
    void RecordOrchestratorAntiEntropySync(string replicaId);

    /// <summary>
    /// Records that a full state snapshot was dispatched to a remote peer.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="byteCount">The size of the snapshot message in bytes.</param>
    void RecordOrchestratorSnapshotDispatched(string documentId, long byteCount);

    /// <summary>
    /// Records that an operation intent was forwarded to a remote node.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordOrchestratorIntentForwarded(string documentId);

    /// <summary>
    /// Records that forwarding an operation intent to a remote node failed.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordOrchestratorIntentFailed(string documentId);

    /// <summary>
    /// Records that a CRDT patch was forwarded to a remote node.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordOrchestratorPatchForwarded(string documentId);

    /// <summary>
    /// Records that an orchestrator command was enqueued.
    /// </summary>
    void RecordOrchestratorCommandEnqueued();

    /// <summary>
    /// Records that an orchestrator command was processed.
    /// </summary>
    void RecordOrchestratorCommandProcessed();

    /// <summary>
    /// Records that an orchestrator command failed during processing.
    /// </summary>
    void RecordOrchestratorCommandFailure();

    /// <summary>
    /// Records that a patch was applied to a document.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentPatchApplied(string documentId);

    /// <summary>
    /// Records that a full state snapshot was merged into a document.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentSnapshotMerged(string documentId);

    /// <summary>
    /// Records that a document checkpoint was saved to storage.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentCheckpointSaved(string documentId);

    /// <summary>
    /// Records that an operation intent was applied to a local document.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentIntentApplied(string documentId);

    /// <summary>
    /// Records that an operation intent failed to apply to a local document.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentIntentFailed(string documentId);

    /// <summary>
    /// Records that a replica was evicted from a document's metadata.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentReplicaEviction(string documentId);

    /// <summary>
    /// Records that a document's local state was reset.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentStateReset(string documentId);

    /// <summary>
    /// Records that a document command was enqueued into the shared pipeline.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentCommandEnqueued(string documentId);

    /// <summary>
    /// Records that a document command was processed by the shared pipeline.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentCommandProcessed(string documentId);

    /// <summary>
    /// Records that a document command failed in the shared pipeline.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    void RecordDocumentCommandFailure(string documentId);
}