namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Collections.Generic;

/// <summary>
/// Internal execution result produced by <see cref="Services.ICrdtMapReduceExecutor"/> for a local batch of documents.
/// </summary>
/// <param name="IsSuccess">Indicates whether the local batch execution succeeded.</param>
/// <param name="HasAccumulator">Indicates whether at least one document produced an accumulator.</param>
/// <param name="EvaluatedDocumentCount">The number of documents evaluated locally.</param>
/// <param name="SerializedAccumulator">The serialized combined accumulator payload.</param>
/// <param name="MissingDocumentIds">Optional list of document IDs that were not hosted on this replica.</param>
/// <param name="ErrorMessage">Optional error description if execution failed.</param>
public readonly record struct CrdtMapReduceExecutionOutcomeDto(
    bool IsSuccess,
    bool HasAccumulator,
    int EvaluatedDocumentCount,
    byte[] SerializedAccumulator,
    IList<string>? MissingDocumentIds,
    string? ErrorMessage);