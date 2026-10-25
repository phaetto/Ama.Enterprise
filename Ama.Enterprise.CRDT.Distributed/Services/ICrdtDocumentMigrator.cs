namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Defines the contract implemented by developers to migrate and synchronize state from an old CRDT document model to a new CRDT document model without downtime.
/// </summary>
/// <typeparam name="TSourceState">The source (legacy) CRDT document state type.</typeparam>
/// <typeparam name="TTargetState">The target (new) CRDT document state type.</typeparam>
/// <example>
/// <code>
/// public sealed class TaskV1ToV2Migrator : ICrdtDocumentMigrator&lt;TaskListStateV1, TaskListStateV2&gt;
/// {
///     public async Task MigrateDocumentAsync(
///         string documentId,
///         CrdtDocument&lt;TaskListStateV1&gt; sourceDocument,
///         CrdtDocument&lt;TaskListStateV2&gt; targetDocument,
///         ICrdtDocumentOrchestrator targetOrchestrator,
///         IntentDeliveryMode deliveryMode,
///         CancellationToken cancellationToken = default)
///     {
///         if (sourceDocument.Data is null) return;
///
///         foreach (var kvp in sourceDocument.Data.Tasks)
///         {
///             var newTask = new TaskItemV2(kvp.Value.Description, kvp.Value.IsDone, Priority: 1);
///             await targetOrchestrator.ApplyIntentAsync&lt;TaskListStateV2&gt;(
///                 documentId,
///                 new MapSetIntent(kvp.Key, newTask),
///                 nameof(TaskListStateV2.Tasks),
///                 deliveryMode,
///                 cancellationToken);
///         }
///     }
/// }
/// </code>
/// </example>
public interface ICrdtDocumentMigrator<TSourceState, TTargetState>
    where TSourceState : class, new()
    where TTargetState : class, new()
{
    /// <summary>
    /// Maps a source document identifier to a target document identifier.
    /// By default, preserves the same document identifier.
    /// </summary>
    /// <param name="sourceDocumentId">The document identifier in the source replica.</param>
    /// <returns>The corresponding document identifier in the target replica.</returns>
    /// <example>
    /// <code>
    /// string targetId = migrator.MapDocumentId("doc-1");
    /// </code>
    /// </example>
    string MapDocumentId(string sourceDocumentId) => sourceDocumentId;

    /// <summary>
    /// Reconciles the current state of the source CRDT document into the target CRDT document by applying idempotent intents or patches to the target orchestrator.
    /// Invoked both during background backfill of existing data and whenever live changes occur on the source document.
    /// </summary>
    /// <param name="targetDocumentId">The resolved document identifier in the target replica.</param>
    /// <param name="sourceDocument">The current materialized state and CRDT metadata of the source document.</param>
    /// <param name="targetDocument">The current materialized state and CRDT metadata of the target document.</param>
    /// <param name="targetOrchestrator">The target CRDT orchestrator used to apply intents or patches.</param>
    /// <param name="deliveryMode">The configured delivery mode for target operations.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous migration operation.</returns>
    /// <example>
    /// <code>
    /// await migrator.MigrateDocumentAsync(
    ///     "doc-1",
    ///     sourceDoc,
    ///     targetDoc,
    ///     targetOrchestrator,
    ///     IntentDeliveryMode.Acknowledged,
    ///     cancellationToken);
    /// </code>
    /// </example>
    Task MigrateDocumentAsync(
        string targetDocumentId,
        CrdtDocument<TSourceState> sourceDocument,
        CrdtDocument<TTargetState> targetDocument,
        ICrdtDocumentOrchestrator targetOrchestrator,
        IntentDeliveryMode deliveryMode,
        CancellationToken cancellationToken = default);
}