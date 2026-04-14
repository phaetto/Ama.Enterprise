namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.CRDT.Models;

/// <summary>
/// Represents the missing operations and whether a full snapshot is required for synchronization.
/// </summary>
public readonly record struct MissingOperationsResult : IEquatable<MissingOperationsResult>
{
    /// <summary>
    /// Gets the sequence of missing CRDT operations that the remote replica needs.
    /// </summary>
    public IReadOnlyList<CrdtOperation> Operations { get; }

    /// <summary>
    /// Gets a value indicating whether the journal has been truncated, requiring a complete document snapshot fallback.
    /// </summary>
    public bool SnapshotRequired { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MissingOperationsResult"/> struct.
    /// </summary>
    /// <param name="operations">The list of missing operations.</param>
    /// <param name="snapshotRequired">A flag indicating if a full snapshot is required.</param>
    public MissingOperationsResult(IReadOnlyList<CrdtOperation> operations, bool snapshotRequired)
    {
        Operations = operations ?? Array.Empty<CrdtOperation>();
        SnapshotRequired = snapshotRequired;
    }

    /// <inheritdoc />
    public bool Equals(MissingOperationsResult other)
    {
        if (SnapshotRequired != other.SnapshotRequired) return false;
        if (Operations.Count != other.Operations.Count) return false;
        
        return Operations.SequenceEqual(other.Operations);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(SnapshotRequired);
        
        foreach (var op in Operations)
        {
            hash.Add(op);
        }
        
        return hash.ToHashCode();
    }
}