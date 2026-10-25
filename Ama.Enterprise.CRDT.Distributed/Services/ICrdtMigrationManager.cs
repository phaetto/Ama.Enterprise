namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Central coordinator providing visibility and drain control across all registered CRDT migration bridges on the node.
/// </summary>
/// <example>
/// <code>
/// bool readyForCutover = migrationManager.AreAllConverged();
/// if (readyForCutover)
/// {
///     await migrationManager.DrainAllAsync(cancellationToken);
/// }
/// </code>
/// </example>
public interface ICrdtMigrationManager
{
    /// <summary>
    /// Retrieves the status snapshots of all registered migration bridges.
    /// </summary>
    /// <returns>A read-only list of migration status snapshots.</returns>
    /// <example>
    /// <code>
    /// var statuses = migrationManager.GetAllStatuses();
    /// </code>
    /// </example>
    IReadOnlyList<CrdtMigrationStatusDto> GetAllStatuses();

    /// <summary>
    /// Retrieves the status of a specific migration bridge by its identifier.
    /// </summary>
    /// <param name="migrationId">The unique migration identifier.</param>
    /// <param name="status">When this method returns, contains the migration status if found.</param>
    /// <returns><c>true</c> if the migration bridge was found; otherwise, <c>false</c>.</returns>
    /// <example>
    /// <code>
    /// if (migrationManager.TryGetStatus("tasks-v1-to-v2", out var status))
    /// {
    ///     Console.WriteLine(status.Stage);
    /// }
    /// </code>
    /// </example>
    bool TryGetStatus(string migrationId, out CrdtMigrationStatusDto status);

    /// <summary>
    /// Checks whether all registered migration bridges have completed their initial backfill and have zero pending or in-flight items.
    /// </summary>
    /// <returns><c>true</c> if all migration bridges are converged; otherwise, <c>false</c>.</returns>
    /// <example>
    /// <code>
    /// if (migrationManager.AreAllConverged())
    /// {
    ///     // Safe to switch reads and writes to the new replica/model
    /// }
    /// </code>
    /// </example>
    bool AreAllConverged();

    /// <summary>
    /// Drains all pending source changes across all registered migration bridges and flushes the target orchestrators.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous drain operation.</returns>
    /// <example>
    /// <code>
    /// await migrationManager.DrainAllAsync(cancellationToken);
    /// </code>
    /// </example>
    Task DrainAllAsync(CancellationToken cancellationToken = default);
}