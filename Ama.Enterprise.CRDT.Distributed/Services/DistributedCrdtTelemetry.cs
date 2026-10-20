namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;

/// <summary>
/// Singleton implementation of <see cref="IDistributedCrdtTelemetry"/> that centralizes all meters, counters, and observable gauges.
/// </summary>
public sealed class DistributedCrdtTelemetry : IDistributedCrdtTelemetry, IDisposable
{
    private readonly Meter scopeMeter;
    private readonly Meter orchestratorMeter;
    private readonly Meter documentMeter;

    // Scope metrics
    private readonly Counter<long> scopesGeneratedCounter;

    // Orchestrator metrics
    private readonly Counter<long> documentCreatedCounter;
    private readonly Counter<long> documentDeletedCounter;
    private readonly Counter<long> antiEntropySyncCounter;
    private readonly Counter<long> snapshotsDispatchedCounter;
    private readonly Counter<long> snapshotBytesDispatchedCounter;
    private readonly Counter<long> intentsForwardedCounter;
    private readonly Counter<long> orchestratorIntentsFailedCounter;
    private readonly Counter<long> patchesForwardedCounter;
    private readonly Counter<long> orchestratorCommandsEnqueuedCounter;
    private readonly Counter<long> orchestratorCommandsProcessedCounter;
    private readonly Counter<long> orchestratorCommandFailuresCounter;

    // Document metrics
    private readonly Counter<long> patchAppliedCounter;
    private readonly Counter<long> snapshotsMergedCounter;
    private readonly Counter<long> checkPointSavedCounter;
    private readonly Counter<long> documentIntentsAppliedCounter;
    private readonly Counter<long> documentIntentsFailedCounter;
    private readonly Counter<long> replicaEvictionsCounter;
    private readonly Counter<long> stateResetsCounter;
    private readonly Counter<long> documentCommandsEnqueuedCounter;
    private readonly Counter<long> documentCommandsProcessedCounter;
    private readonly Counter<long> documentCommandFailuresCounter;

    private readonly ConcurrentDictionary<long, PipelineMetricSource> documentPipelineSources = new();
    private readonly ConcurrentDictionary<long, PipelineMetricSource> orchestratorPipelineSources = new();
    private long nextSourceId;

    private readonly record struct PipelineMetricSource(Func<int> QueueLengthProvider, Func<int> PoolSizeProvider);

    /// <summary>
    /// Initializes a new instance of the <see cref="DistributedCrdtTelemetry"/> class.
    /// </summary>
    /// <param name="meterFactory">Optional meter factory provided by dependency injection.</param>
    public DistributedCrdtTelemetry(IMeterFactory? meterFactory = null)
    {
        this.scopeMeter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeFactory")
            ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtScopeFactory");
        this.orchestratorMeter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtDocumentOrchestrator")
            ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtDocumentOrchestrator");
        this.documentMeter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument")
            ?? new Meter("Ama.Enterprise.CRDT.Distributed.DistributedCrdtDocument");

        this.scopesGeneratedCounter = this.scopeMeter.CreateCounter<long>(
            "crdt.scope.generated",
            "scopes",
            "Total explicitly distinct internal generic bounds generated dynamically");

        this.documentCreatedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.documents_created",
            "documents",
            "Total CRDT documents mapped");
        this.documentDeletedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.documents_deleted",
            "documents",
            "Total CRDT documents deleted");
        this.antiEntropySyncCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.anti_entropy_syncs",
            "syncs",
            "Total anti-entropy synchronizations dispatched");
        this.snapshotsDispatchedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.snapshots_dispatched",
            "snapshots",
            "Total full state snapshots sent");
        this.snapshotBytesDispatchedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.snapshot_bytes_dispatched",
            "bytes",
            "Total bytes sent for full state snapshots");
        this.intentsForwardedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.intents_forwarded",
            "intents",
            "Total operation intents forwarded to remote nodes");
        this.orchestratorIntentsFailedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.intents_failed",
            "intents",
            "Total operation intents that failed remote execution");
        this.patchesForwardedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.patches_forwarded",
            "patches",
            "Total CRDT patches forwarded to remote nodes");
        this.orchestratorCommandsEnqueuedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.channel.commands_enqueued",
            "commands",
            "Total orchestrator commands enqueued");
        this.orchestratorCommandsProcessedCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.channel.commands_processed",
            "commands",
            "Total orchestrator commands processed");
        this.orchestratorCommandFailuresCounter = this.orchestratorMeter.CreateCounter<long>(
            "crdt.orchestrator.channel.command_failures",
            "errors",
            "Total orchestrator command processing errors");

        _ = this.orchestratorMeter.CreateObservableGauge(
            "crdt.orchestrator.channel.queue_length",
            ObserveOrchestratorQueueLength,
            "commands",
            "Current number of pending orchestrator commands");
        _ = this.orchestratorMeter.CreateObservableGauge(
            "crdt.orchestrator.channel.pool_size",
            ObserveOrchestratorPoolSize,
            "commands",
            "Current size of the orchestrator command object pool");

        this.patchAppliedCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.patches_applied",
            "patches",
            "Total local patches applied");
        this.snapshotsMergedCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.snapshots_merged",
            "snapshots",
            "Total incoming full snapshots merged");
        this.checkPointSavedCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.checkpoints_saved",
            "checkpoints",
            "Total underlying storage checkpoints saved");
        this.documentIntentsAppliedCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.intents_applied",
            "intents",
            "Total operation intents applied to this document");
        this.documentIntentsFailedCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.intents_failed",
            "intents",
            "Total operation intents that failed to apply");
        this.replicaEvictionsCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.replica_evictions",
            "evictions",
            "Total replicas evicted from this document state");
        this.stateResetsCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.state_resets",
            "resets",
            "Total local state resets performed on this document");
        this.documentCommandsEnqueuedCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.channel.commands_enqueued",
            "commands",
            "Total document commands enqueued");
        this.documentCommandsProcessedCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.channel.commands_processed",
            "commands",
            "Total document commands processed");
        this.documentCommandFailuresCounter = this.documentMeter.CreateCounter<long>(
            "crdt.document.channel.command_failures",
            "errors",
            "Total document command processing errors");

        _ = this.documentMeter.CreateObservableGauge(
            "crdt.document.channel.queue_length",
            ObserveDocumentQueueLength,
            "commands",
            "Current number of pending document commands");
        _ = this.documentMeter.CreateObservableGauge(
            "crdt.document.channel.pool_size",
            ObserveDocumentPoolSize,
            "commands",
            "Current size of the document command object pool");
    }

    /// <inheritdoc />
    public IDisposable RegisterDocumentPipelineMetrics(Func<int> queueLengthProvider, Func<int> poolSizeProvider)
    {
        ArgumentNullException.ThrowIfNull(queueLengthProvider);
        ArgumentNullException.ThrowIfNull(poolSizeProvider);

        var id = Interlocked.Increment(ref nextSourceId);
        documentPipelineSources[id] = new PipelineMetricSource(queueLengthProvider, poolSizeProvider);
        return new CallbackUnregister(documentPipelineSources, id);
    }

    /// <inheritdoc />
    public IDisposable RegisterOrchestratorPipelineMetrics(Func<int> queueLengthProvider, Func<int> poolSizeProvider)
    {
        ArgumentNullException.ThrowIfNull(queueLengthProvider);
        ArgumentNullException.ThrowIfNull(poolSizeProvider);

        var id = Interlocked.Increment(ref nextSourceId);
        orchestratorPipelineSources[id] = new PipelineMetricSource(queueLengthProvider, poolSizeProvider);
        return new CallbackUnregister(orchestratorPipelineSources, id);
    }

    /// <inheritdoc />
    public void RecordScopeGenerated(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) return;
        scopesGeneratedCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaId));
    }

    /// <inheritdoc />
    public void RecordOrchestratorDocumentCreated(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        documentCreatedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordOrchestratorDocumentDeleted(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        documentDeletedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordOrchestratorAntiEntropySync(string replicaId)
    {
        if (string.IsNullOrWhiteSpace(replicaId)) return;
        antiEntropySyncCounter.Add(1, new KeyValuePair<string, object?>("replica_id", replicaId));
    }

    /// <inheritdoc />
    public void RecordOrchestratorSnapshotDispatched(string documentId, long byteCount)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        var tag = new KeyValuePair<string, object?>("document_id", documentId);
        snapshotsDispatchedCounter.Add(1, tag);
        snapshotBytesDispatchedCounter.Add(byteCount, tag);
    }

    /// <inheritdoc />
    public void RecordOrchestratorIntentForwarded(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        intentsForwardedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordOrchestratorIntentFailed(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        orchestratorIntentsFailedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordOrchestratorPatchForwarded(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        patchesForwardedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordOrchestratorCommandEnqueued() => orchestratorCommandsEnqueuedCounter.Add(1);

    /// <inheritdoc />
    public void RecordOrchestratorCommandProcessed() => orchestratorCommandsProcessedCounter.Add(1);

    /// <inheritdoc />
    public void RecordOrchestratorCommandFailure() => orchestratorCommandFailuresCounter.Add(1);

    /// <inheritdoc />
    public void RecordDocumentPatchApplied(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        patchAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentSnapshotMerged(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        snapshotsMergedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentCheckpointSaved(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        checkPointSavedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentIntentApplied(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        documentIntentsAppliedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentIntentFailed(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        documentIntentsFailedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentReplicaEviction(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        replicaEvictionsCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentStateReset(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        stateResetsCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentCommandEnqueued(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        documentCommandsEnqueuedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentCommandProcessed(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        documentCommandsProcessedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void RecordDocumentCommandFailure(string documentId)
    {
        if (string.IsNullOrWhiteSpace(documentId)) return;
        documentCommandFailuresCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        documentPipelineSources.Clear();
        orchestratorPipelineSources.Clear();
        scopeMeter.Dispose();
        orchestratorMeter.Dispose();
        documentMeter.Dispose();
    }

    private int ObserveOrchestratorQueueLength()
    {
        int total = 0;
        foreach (var source in orchestratorPipelineSources.Values)
        {
            total += source.QueueLengthProvider();
        }
        return total;
    }

    private int ObserveOrchestratorPoolSize()
    {
        int total = 0;
        foreach (var source in orchestratorPipelineSources.Values)
        {
            total += source.PoolSizeProvider();
        }
        return total;
    }

    private int ObserveDocumentQueueLength()
    {
        int total = 0;
        foreach (var source in documentPipelineSources.Values)
        {
            total += source.QueueLengthProvider();
        }
        return total;
    }

    private int ObserveDocumentPoolSize()
    {
        int total = 0;
        foreach (var source in documentPipelineSources.Values)
        {
            total += source.PoolSizeProvider();
        }
        return total;
    }

    private sealed class CallbackUnregister(ConcurrentDictionary<long, PipelineMetricSource> dictionary, long id) : IDisposable
    {
        public void Dispose()
        {
            dictionary.TryRemove(id, out _);
        }
    }
}