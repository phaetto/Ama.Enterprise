namespace Ama.Enterprise.CRDT.MessagePack.UnitTests.Models;

using System;
using global::MessagePack;

[MessagePackObject]
public sealed class TestModel : IEquatable<TestModel>
{
    [Key(0)]
    public string Name { get; set; } = string.Empty;

    [Key(1)]
    public int Value { get; set; }

    public bool Equals(TestModel? other)
    {
        if (other is null)
        {
            return false;
        }

        return Name == other.Name && Value == other.Value;
    }

    public override bool Equals(object? obj)
    {
        return Equals(obj as TestModel);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Name, Value);
    }
}