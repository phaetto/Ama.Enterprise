namespace Ama.Enterprise.CRDT.MessagePack.UnitTests.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

/// <summary>
/// A strict value-type structure ensuring robust baseline formatting for AOT mappings.
/// </summary>
public readonly record struct SimpleValueModel(
    int Id,
    string Name,
    bool IsActive,
    double Score,
    DateTime CreatedAt,
    Guid Uid
);

/// <summary>
/// A strictly bound data model encompassing multi-level collections mapped through explicit standard interfaces.
/// </summary>
public sealed record ComplexCollectionModel(
    IReadOnlyList<string> Tags,
    IList<int> Numbers,
    IDictionary<string, SimpleValueModel> Map,
    byte[] BinaryData
) : IEquatable<ComplexCollectionModel>
{
    public bool Equals(ComplexCollectionModel? other)
    {
        if (other is null) return false;

        if (Tags == null && other.Tags != null) return false;
        if (Tags != null && other.Tags == null) return false;
        if (Tags != null && other.Tags != null && !Tags.SequenceEqual(other.Tags)) return false;

        if (Numbers == null && other.Numbers != null) return false;
        if (Numbers != null && other.Numbers == null) return false;
        if (Numbers != null && other.Numbers != null && !Numbers.SequenceEqual(other.Numbers)) return false;

        if (Map == null && other.Map != null) return false;
        if (Map != null && other.Map == null) return false;
        if (Map != null && other.Map != null)
        {
            if (Map.Count != other.Map.Count) return false;
            foreach (var kvp in Map)
            {
                if (!other.Map.TryGetValue(kvp.Key, out var otherVal) || !kvp.Value.Equals(otherVal))
                {
                    return false;
                }
            }
        }

        if (BinaryData == null && other.BinaryData != null) return false;
        if (BinaryData != null && other.BinaryData == null) return false;
        if (BinaryData != null && other.BinaryData != null && !BinaryData.SequenceEqual(other.BinaryData)) return false;

        return true;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// Deeply nested domain constraint ensuring complex objects cascade inherently natively without failing depth limits.
/// </summary>
public sealed record DeepNestedModel(
    Guid GroupId,
    ComplexCollectionModel Collections,
    IList<SimpleValueModel> Items
) : IEquatable<DeepNestedModel>
{
    public bool Equals(DeepNestedModel? other)
    {
        if (other is null) return false;
        if (GroupId != other.GroupId) return false;

        if (Collections == null && other.Collections != null) return false;
        if (Collections != null && !Collections.Equals(other.Collections)) return false;

        if (Items == null && other.Items != null) return false;
        if (Items != null && other.Items == null) return false;
        if (Items != null && other.Items != null && !Items.SequenceEqual(other.Items)) return false;

        return true;
    }

    public override int GetHashCode() => 0;
}

[JsonDerivedType(typeof(DerivedA), "A")]
[JsonDerivedType(typeof(DerivedB), "B")]
public abstract record BasePolymorphicModel(string BaseProperty);

public sealed record DerivedA(string BaseProperty, int PropA) : BasePolymorphicModel(BaseProperty);

public sealed record DerivedB(string BaseProperty, string PropB) : BasePolymorphicModel(BaseProperty);

/// <summary>
/// A structure carrying polymorphic baseline elements confirming native STJ derived serialization correctly transfers bounds dynamically.
/// </summary>
public sealed record PolymorphicContainer(
    IList<BasePolymorphicModel> Elements
) : IEquatable<PolymorphicContainer>
{
    public bool Equals(PolymorphicContainer? other)
    {
        if (other is null) return false;
        if (Elements == null && other.Elements != null) return false;
        if (Elements != null && other.Elements == null) return false;
        if (Elements != null && other.Elements != null)
        {
            if (Elements.Count != other.Elements.Count) return false;
            for (int i = 0; i < Elements.Count; i++)
            {
                if (Elements[i] == null && other.Elements[i] != null) return false;
                if (Elements[i] != null && !Elements[i]!.Equals(other.Elements[i])) return false;
            }
        }
        return true;
    }

    public override int GetHashCode() => 0;
}

public enum TestStatus
{
    None = 0,
    Active = 1,
    Completed = 2,
    Failed = 3
}

/// <summary>
/// Model validating extreme boundaries seamlessly interacting across AOT bounds.
/// </summary>
public sealed record BoundaryValuesModel(
    int MinInt,
    int MaxInt,
    double NanValue,
    double PosInfinity,
    DateTime MinDate,
    DateTime MaxDate,
    string SpecialString,
    TestStatus Status,
    Guid EmptyGuid
) : IEquatable<BoundaryValuesModel>
{
    public bool Equals(BoundaryValuesModel? other)
    {
        if (other is null) return false;
        if (MinInt != other.MinInt) return false;
        if (MaxInt != other.MaxInt) return false;
        
        // Handle double.NaN comparison explicitly
        if (double.IsNaN(NanValue) && !double.IsNaN(other.NanValue)) return false;
        if (!double.IsNaN(NanValue) && NanValue != other.NanValue) return false;
        
        if (PosInfinity != other.PosInfinity) return false;
        if (MinDate != other.MinDate) return false;
        if (MaxDate != other.MaxDate) return false;
        if (SpecialString != other.SpecialString) return false;
        if (Status != other.Status) return false;
        if (EmptyGuid != other.EmptyGuid) return false;
        return true;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// Model securing standard isolated distinct enumerations inherently.
/// </summary>
public sealed record SetModel(
    ISet<string> UniqueTags
) : IEquatable<SetModel>
{
    public bool Equals(SetModel? other)
    {
        if (other is null) return false;
        if (UniqueTags == null && other.UniqueTags != null) return false;
        if (UniqueTags != null && other.UniqueTags == null) return false;
        if (UniqueTags != null && other.UniqueTags != null && !UniqueTags.SetEquals(other.UniqueTags)) return false;
        return true;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// Multi-level map structure securing explicit polymorphism securely explicitly mapped inherently natively.
/// </summary>
public sealed record ComplexPolymorphicModel(
    IDictionary<string, BasePolymorphicModel> PolyMap
) : IEquatable<ComplexPolymorphicModel>
{
    public bool Equals(ComplexPolymorphicModel? other)
    {
        if (other is null) return false;
        if (PolyMap == null && other.PolyMap != null) return false;
        if (PolyMap != null && other.PolyMap == null) return false;
        if (PolyMap != null && other.PolyMap != null)
        {
            if (PolyMap.Count != other.PolyMap.Count) return false;
            foreach (var kvp in PolyMap)
            {
                if (!other.PolyMap.TryGetValue(kvp.Key, out var otherVal)) return false;
                if (kvp.Value is null && otherVal is not null) return false;
                if (kvp.Value is not null && !kvp.Value.Equals(otherVal)) return false;
            }
        }
        return true;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// AOT friendly data structure identifying distinct multidimensional metrics mapping attributes safely.
/// </summary>
public readonly record struct TestMetricTagDto(string Key, string Value) : IEquatable<TestMetricTagDto>
{
    /// <inheritdoc />
    public bool Equals(TestMetricTagDto other)
    {
        return string.Equals(Key, other.Key, StringComparison.Ordinal) && 
               string.Equals(Value, other.Value, StringComparison.Ordinal);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(Key, Value);
    }
}

/// <summary>
/// Immutable payload holding flattened telemetry captures strictly ensuring AOT constraints natively decoupled from reflection SDK parameters.
/// </summary>
public sealed record TestMetricSnapshotDto : IEquatable<TestMetricSnapshotDto>
{
    /// <summary>
    /// Name representing the distinct mapped instrument constraint.
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// Identifies the primitive evaluation type tracked internally resolving histogram or counters.
    /// </summary>
    public string Type { get; init; } = string.Empty;

    /// <summary>
    /// Total aggregated sum or explicit absolute gauge evaluating measured attributes natively.
    /// </summary>
    public long Value { get; init; }

    /// <summary>
    /// List defining exact metadata properties filtering explicitly evaluated metric scopes.
    /// </summary>
    public IReadOnlyList<TestMetricTagDto> Tags { get; init; } = Array.Empty<TestMetricTagDto>();

    /// <inheritdoc />
    public bool Equals(TestMetricSnapshotDto? other)
    {
        if (other is null)
        {
            return false;
        }

        if (!string.Equals(Name, other.Name, StringComparison.Ordinal) || 
            !string.Equals(Type, other.Type, StringComparison.Ordinal) || 
            Value != other.Value)
        {
            return false;
        }

        if (Tags.Count != other.Tags.Count)
        {
            return false;
        }

        for (int i = 0; i < Tags.Count; i++)
        {
            if (!Tags[i].Equals(other.Tags[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Name);
        hash.Add(Type);
        hash.Add(Value);

        foreach (var tag in Tags)
        {
            hash.Add(tag);
        }

        return hash.ToHashCode();
    }
}

/// <summary>
/// Top-level network payload transmission encapsulating uniquely active node configurations bounding periodic explicitly wrapped metric topologies.
/// </summary>
public sealed record TestTelemetryPayloadDto : IEquatable<TestTelemetryPayloadDto>
{
    /// <summary>
    /// Globally distinct network identity originating generic metric clusters internally.
    /// </summary>
    public Guid NodeId { get; init; }

    /// <summary>
    /// Time sequence bounding exactly when explicit telemetry configurations captured runtime values natively.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// Active measured snapshots capturing specific decoupled metrics natively.
    /// </summary>
    public IReadOnlyList<TestMetricSnapshotDto> Metrics { get; init; } = Array.Empty<TestMetricSnapshotDto>();

    /// <inheritdoc />
    public bool Equals(TestTelemetryPayloadDto? other)
    {
        if (other is null)
        {
            return false;
        }

        if (NodeId != other.NodeId || Timestamp != other.Timestamp)
        {
            return false;
        }

        if (Metrics.Count != other.Metrics.Count)
        {
            return false;
        }

        for (int i = 0; i < Metrics.Count; i++)
        {
            if (!Metrics[i].Equals(other.Metrics[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(NodeId);
        hash.Add(Timestamp);

        foreach (var metric in Metrics)
        {
            hash.Add(metric);
        }

        return hash.ToHashCode();
    }
}

/// <summary>
/// Validates robust handling of explicitly nullable primitive types correctly correctly mapped in AOT schemas.
/// </summary>
public sealed record NullableTypesModel(
    int? OptionalInt,
    double? OptionalDouble,
    DateTime? OptionalDate,
    Guid? OptionalGuid,
    string? OptionalString,
    SimpleValueModel? OptionalStruct
) : IEquatable<NullableTypesModel>
{
    public bool Equals(NullableTypesModel? other)
    {
        if (other is null) return false;
        if (OptionalInt != other.OptionalInt) return false;
        if (OptionalDouble != other.OptionalDouble) return false;
        if (OptionalDate != other.OptionalDate) return false;
        if (OptionalGuid != other.OptionalGuid) return false;
        if (OptionalString != other.OptionalString) return false;
        
        if (OptionalStruct.HasValue != other.OptionalStruct.HasValue) return false;
        if (OptionalStruct.HasValue && !OptionalStruct.Value.Equals(other.OptionalStruct.Value)) return false;
        
        return true;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// A model incorporating advanced specific primitive types requiring precise STJ/MessagePack integrations natively.
/// </summary>
public sealed record AdvancedPrimitivesModel(
    TimeSpan Duration,
    decimal CurrencyValue,
    Uri? Endpoint
) : IEquatable<AdvancedPrimitivesModel>
{
    public bool Equals(AdvancedPrimitivesModel? other)
    {
        if (other is null) return false;
        if (Duration != other.Duration) return false;
        if (CurrencyValue != other.CurrencyValue) return false;
        if (Endpoint != other.Endpoint) return false;
        return true;
    }

    public override int GetHashCode() => 0;
}

[Flags]
public enum TestPermissions
{
    None = 0,
    Read = 1 << 0,
    Write = 1 << 1,
    Execute = 1 << 2,
    Admin = Read | Write | Execute
}

/// <summary>
/// Validates correct mapping and storage of explicit bitwise enum flags natively.
/// </summary>
public sealed record FlagsEnumModel(TestPermissions Permissions) : IEquatable<FlagsEnumModel>
{
    public bool Equals(FlagsEnumModel? other)
    {
        if (other is null) return false;
        return Permissions == other.Permissions;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// Verifies multi-dimensional structures avoiding generic recursion depth bounds gracefully.
/// </summary>
public sealed record JaggedArrayModel(
    IList<byte[]> DataChunks,
    IList<IList<int>> Matrix
) : IEquatable<JaggedArrayModel>
{
    public bool Equals(JaggedArrayModel? other)
    {
        if (other is null) return false;

        if (DataChunks == null && other.DataChunks != null) return false;
        if (DataChunks != null && other.DataChunks == null) return false;
        if (DataChunks != null && other.DataChunks != null)
        {
            if (DataChunks.Count != other.DataChunks.Count) return false;
            for (int i = 0; i < DataChunks.Count; i++)
            {
                if (DataChunks[i] == null && other.DataChunks[i] != null) return false;
                if (DataChunks[i] != null && other.DataChunks[i] == null) return false;
                if (DataChunks[i] != null && other.DataChunks[i] != null && !DataChunks[i].SequenceEqual(other.DataChunks[i])) return false;
            }
        }

        if (Matrix == null && other.Matrix != null) return false;
        if (Matrix != null && other.Matrix == null) return false;
        if (Matrix != null && other.Matrix != null)
        {
            if (Matrix.Count != other.Matrix.Count) return false;
            for (int i = 0; i < Matrix.Count; i++)
            {
                if (Matrix[i] == null && other.Matrix[i] != null) return false;
                if (Matrix[i] != null && other.Matrix[i] == null) return false;
                if (Matrix[i] != null && other.Matrix[i] != null && !Matrix[i].SequenceEqual(other.Matrix[i])) return false;
            }
        }

        return true;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// Confirms that non-string dictionary keys (like ints or Guids) explicitly map reliably across AOT bounds.
/// </summary>
public sealed record NonStringKeyDictionaryModel(
    IDictionary<int, string> IntKeys,
    IDictionary<Guid, SimpleValueModel> GuidKeys
) : IEquatable<NonStringKeyDictionaryModel>
{
    public bool Equals(NonStringKeyDictionaryModel? other)
    {
        if (other is null) return false;

        if (IntKeys == null && other.IntKeys != null) return false;
        if (IntKeys != null && other.IntKeys == null) return false;
        if (IntKeys != null && other.IntKeys != null)
        {
            if (IntKeys.Count != other.IntKeys.Count) return false;
            foreach (var kvp in IntKeys)
            {
                if (!other.IntKeys.TryGetValue(kvp.Key, out var val) || val != kvp.Value) return false;
            }
        }

        if (GuidKeys == null && other.GuidKeys != null) return false;
        if (GuidKeys != null && other.GuidKeys == null) return false;
        if (GuidKeys != null && other.GuidKeys != null)
        {
            if (GuidKeys.Count != other.GuidKeys.Count) return false;
            foreach (var kvp in GuidKeys)
            {
                if (!other.GuidKeys.TryGetValue(kvp.Key, out var val) || !val.Equals(kvp.Value)) return false;
            }
        }

        return true;
    }

    public override int GetHashCode() => 0;
}

/// <summary>
/// Decoupled AOT Source Generation bounds intentionally explicitly triggering MessagePack mappings cleanly via strict Standard System.Text.Json metadata context boundaries natively.
/// </summary>
[JsonSerializable(typeof(SimpleValueModel))]
[JsonSerializable(typeof(ComplexCollectionModel))]
[JsonSerializable(typeof(DeepNestedModel))]
[JsonSerializable(typeof(BasePolymorphicModel))]
[JsonSerializable(typeof(DerivedA))]
[JsonSerializable(typeof(DerivedB))]
[JsonSerializable(typeof(PolymorphicContainer))]
[JsonSerializable(typeof(TestStatus))]
[JsonSerializable(typeof(BoundaryValuesModel))]
[JsonSerializable(typeof(SetModel))]
[JsonSerializable(typeof(ComplexPolymorphicModel))]
[JsonSerializable(typeof(TestMetricTagDto))]
[JsonSerializable(typeof(TestMetricSnapshotDto))]
[JsonSerializable(typeof(TestTelemetryPayloadDto))]
[JsonSerializable(typeof(NullableTypesModel))]
[JsonSerializable(typeof(AdvancedPrimitivesModel))]
[JsonSerializable(typeof(TestPermissions))]
[JsonSerializable(typeof(FlagsEnumModel))]
[JsonSerializable(typeof(JaggedArrayModel))]
[JsonSerializable(typeof(NonStringKeyDictionaryModel))]
public partial class MessagePackIntegrationTestContext : JsonSerializerContext
{
}