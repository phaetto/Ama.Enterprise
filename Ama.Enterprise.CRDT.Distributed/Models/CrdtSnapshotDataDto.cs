namespace Ama.Enterprise.CRDT.Distributed.Models;

using System;
using System.Linq;
using Ama.CRDT.Models;

/// <summary>
/// Data transfer object holding a serialized snapshot payload and its associated global state.
/// </summary>
public readonly record struct CrdtSnapshotDataDto : IEquatable<CrdtSnapshotDataDto>
{
    /// <summary>
    /// Gets the serialized binary data of the CRDT document snapshot.
    /// </summary>
    public byte[] SnapshotData { get; }

    /// <summary>
    /// Gets the global state version vector associated with the snapshot.
    /// </summary>
    public DottedVersionVector GlobalState { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtSnapshotDataDto"/> struct.
    /// </summary>
    /// <param name="snapshotData">The serialized binary data of the snapshot.</param>
    /// <param name="globalState">The associated global state version vector.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="globalState"/> is null.</exception>
    public CrdtSnapshotDataDto(byte[] snapshotData, DottedVersionVector globalState)
    {
        SnapshotData = snapshotData ?? Array.Empty<byte>();
        GlobalState = globalState ?? throw new ArgumentNullException(nameof(globalState));
    }

    /// <inheritdoc />
    public bool Equals(CrdtSnapshotDataDto other)
    {
        return SnapshotData.SequenceEqual(other.SnapshotData) && 
               GlobalState.Equals(other.GlobalState);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.AddBytes(SnapshotData);
        hash.Add(GlobalState);
        return hash.ToHashCode();
    }
}