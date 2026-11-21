namespace Ama.Enterprise.CRDT.Distributed.Models;

using System.Collections.Generic;

/// <summary>
/// Encapsulates the final reduced result of a cluster-wide Map/Reduce job along with execution diagnostics.
/// </summary>
/// <typeparam name="TResult">The final result type.</typeparam>
/// <param name="Result">The final reduced result.</param>
/// <param name="TotalDocumentsTargeted">The total number of documents matched for evaluation.</param>
/// <param name="LocalDocumentsEvaluated">The number of documents evaluated in local memory.</param>
/// <param name="RemoteDocumentsEvaluated">The number of documents evaluated on remote peers.</param>
/// <param name="RemotePeersContacted">The number of remote peers contacted via batched RPC.</param>
/// <param name="WasShortCircuited">Indicates whether execution terminated early due to short-circuiting.</param>
/// <param name="UnreachableDocumentIds">The list of document identifiers that could not be reached on any replica.</param>
public readonly record struct CrdtMapReduceResultDto<TResult>(
    TResult Result,
    int TotalDocumentsTargeted,
    int LocalDocumentsEvaluated,
    int RemoteDocumentsEvaluated,
    int RemotePeersContacted,
    bool WasShortCircuited,
    IReadOnlyList<string> UnreachableDocumentIds);