namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Controls and monitors an active CRDT migration bridge running on a mediator node.
/// </summary>
/// <example>
/// <code>
/// var status = bridge.GetStatus();
/// if (status.IsConverged)
/// {
///     await bridge.DrainAndFlushAsync(cancellationToken);
/// }
/// </code>
/// </example>
public interface ICrdtMigrationBridge
{
    /// <summary>
    /// Gets the configuration options for this migration bridge.
    /// </summary>
    /// <example>
    /// <code>
    /// string id = bridge.Options.MigrationId;
    /// </code>
    /// </example>
    CrdtMigrationOptions Options { get; }

    /// <summary>
    /// Gets a point-in-time snapshot of the migration progress and queue depth.
    /// </summary>
    /// <returns>The current migration status DTO.</returns>
    /// <example>
    /// <code>
    /// CrdtMigrationStatusDto status = bridge.GetStatus();
    /// </code>
    /// </example>
    CrdtMigrationStatusDto GetStatus();

    /// <summary>
    /// Enqueues a specific source document identifier for immediate state reconciliation to the target replica.
    /// </summary>
    /// <param name="sourceDocumentId">The identifier of the modified source document.</param>
    /// <example>
    /// <code>
    /// bridge.EnqueueDocumentChange("task-list-1");
    /// </code>
    /// </example>
    void EnqueueDocumentChange(string sourceDocumentId);

    /// <summary>
    /// Enqueues a source registry or topology change trigger so the bridge evaluates newly created or tombstoned documents.
    /// </summary>
    /// <example>
    /// <code>
    /// bridge.NotifyTopologyOrRegistryChanged();
    /// </code>
    /// </example>
    void NotifyTopologyOrRegistryChanged();

    /// <summary>
    /// Triggers an immediate reconciliation scan of the source registry and waits for all currently queued documents and target orchestrator buffers to flush.
    /// Use this before cutting over traffic or shutting down old replicas to guarantee zero data loss.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that completes when the bridge has drained all pending items and flushed the target replica.</returns>
    /// <example>
    /// <code>
    /// await bridge.DrainAndFlushAsync(cancellationToken);
    /// </code>
    /// </example>
    Task DrainAndFlushAsync(CancellationToken cancellationToken = default);
}