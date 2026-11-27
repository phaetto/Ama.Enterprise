namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Specifies how a distributed CRDT document is represented in memory.
/// </summary>
public enum CrdtDocumentResidencyMode
{
    /// <summary>
    /// Keeps the full <c>CrdtDocument&lt;TState&gt;</c> (both <c>TState</c> and <c>CrdtMetadata</c>) materialized on the heap.
    /// </summary>
    Unpacked = 0,

    /// <summary>
    /// Keeps <c>PackedCrdtDocument&lt;TState&gt;</c> in memory (<c>TState</c> materialized on the heap, <c>CrdtMetadata</c> serialized in a pooled buffer).
    /// </summary>
    PackedMetadata = 1,

    /// <summary>
    /// Keeps the entire <c>CrdtDocument&lt;TState&gt;</c> serialized as bytes in a pooled buffer in memory.
    /// </summary>
    FullySerialized = 2
}