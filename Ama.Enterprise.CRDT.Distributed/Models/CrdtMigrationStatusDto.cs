namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;

/// <summary>
/// Represents a point-in-time status snapshot of a live CRDT migration bridge.
/// </summary>
/// <param name="MigrationId">The unique identifier for the migration bridge registration.</param>
/// <param name="SourceReplicaId">The source replica identifier.</param>
/// <param name="SourceTypeAlias">The source document type alias.</param>
/// <param name="TargetReplicaId">The target replica identifier.</param>
/// <param name="TargetTypeAlias">The target document type alias.</param>
/// <param name="Stage">The current lifecycle stage of the migration.</param>
/// <param name="BackfilledDocuments">The total number of existing documents processed during the background backfill.</param>
/// <param name="LiveEventsProcessed">The total number of real-time change events translated to the target.</param>
/// <param name="TombstonesProcessed">The total number of document deletions propagated to the target.</param>
/// <param name="PendingDocumentsInQueue">The number of dirty documents currently waiting in the migration queue.</param>
/// <param name="InFlightTranslations">The number of document translations currently executing.</param>
/// <param name="FailedTranslations">The total number of translation errors encountered.</param>
public readonly record struct CrdtMigrationStatusDto(
    string MigrationId,
    string SourceReplicaId,
    string SourceTypeAlias,
    string TargetReplicaId,
    string TargetTypeAlias,
    CrdtMigrationStage Stage,
    long BackfilledDocuments,
    long LiveEventsProcessed,
    long TombstonesProcessed,
    int PendingDocumentsInQueue,
    int InFlightTranslations,
    long FailedTranslations) : IEquatable<CrdtMigrationStatusDto>
{
    /// <summary>
    /// Gets a value indicating whether the migration has finished backfilling and has zero pending or in-flight items.
    /// </summary>
    public bool IsConverged =>
        (Stage == CrdtMigrationStage.Converged || Stage == CrdtMigrationStage.Drained) &&
        PendingDocumentsInQueue == 0 &&
        InFlightTranslations == 0;
}