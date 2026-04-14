namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Imposes a centralized constraint on root CRDT state models to map their own synchronization document identifiers.
/// </summary>
public interface IDistributedCrdtState
{
    /// <summary>
    /// Gets or sets the singleton identifier resolving this exact document structure dynamically across the P2P synchronization topology.
    /// </summary>
    string Id { get; set; }
}