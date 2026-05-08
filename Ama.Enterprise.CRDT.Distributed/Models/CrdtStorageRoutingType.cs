namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Enumeration explicitly mapping the routing criteria applied to dynamic P2P persistence mechanisms.
/// </summary>
public enum CrdtStorageRoutingType
{
    /// <summary>
    /// Evaluates the storage target strictly by the internal document alias type mapping.
    /// </summary>
    DocumentType,
    
    /// <summary>
    /// Evaluates the storage target strictly by the unique active Document ID natively.
    /// </summary>
    DocumentId
}