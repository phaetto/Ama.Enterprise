namespace Ama.Enterprise.CRDT.Distributed.Services.Documents.Buffers;

using System;
using System.Collections;
using System.Collections.Generic;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Default AOT-friendly implementation of <see cref="ICrdtMemoryEstimator"/> that traverses object graphs using shared pooled tracking sets.
/// </summary>
public sealed class CrdtMemoryEstimator : ICrdtMemoryEstimator
{
    private readonly object syncRoot = new();
    private readonly HashSet<object> visitedSet = new(ReferenceEqualityComparer.Instance);

    /// <inheritdoc />
    public DocumentMemoryFootprintDto EstimateDocumentFootprint<TState>(
        CrdtDocument<TState> document,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap globalState,
        IReadOnlyList<CrdtAotContext> aotContexts)
        where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(shardId);
        ArgumentNullException.ThrowIfNull(globalState);
        ArgumentNullException.ThrowIfNull(aotContexts);

        long dataBytes;
        long metadataBytes;
        long dataAndMetaBytes;
        long wholeObjectBytes;

        lock (syncRoot)
        {
            visitedSet.Clear();

            dataBytes = EstimateGraphSize(document.Data, visitedSet, aotContexts);
            metadataBytes = EstimateGraphSize(document.Metadata, visitedSet, aotContexts);
            dataAndMetaBytes = 32L + dataBytes + metadataBytes;

            long managerOverheadBytes = 416;
            managerOverheadBytes += EstimateStringSize(documentId, visitedSet);
            managerOverheadBytes += EstimateStringSize(shardId, visitedSet);

            if (debounceBufferCapacity >= 0)
            {
                managerOverheadBytes += 160 + (debounceBufferCapacity * 64L);
            }

            wholeObjectBytes = managerOverheadBytes + dataAndMetaBytes;

            visitedSet.Clear();
            visitedSet.TrimExcess(64);
        }

        var shardDvvBytes = EstimateShardVersionVectorSize(globalState, shardId);
        return new DocumentMemoryFootprintDto(dataBytes, metadataBytes, dataAndMetaBytes, wholeObjectBytes, shardDvvBytes);
    }

    /// <inheritdoc />
    public DocumentMemoryFootprintDto EstimatePackedMetadataDocumentFootprint<TState>(
        TState data,
        int metadataByteLength,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap globalState,
        IReadOnlyList<CrdtAotContext> aotContexts,
        int allocatedMetadataBufferCapacity = 0)
        where TState : class, new()
    {
        ArgumentNullException.ThrowIfNull(data);
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(shardId);
        ArgumentNullException.ThrowIfNull(globalState);
        ArgumentNullException.ThrowIfNull(aotContexts);

        long dataBytes;
        int effectiveMetaCapacity = Math.Max(metadataByteLength, allocatedMetadataBufferCapacity);
        long metadataBytes = Math.Max(0, metadataByteLength) > 0 ? 24L + metadataByteLength : 0L;
        long allocatedMetadataBytes = Math.Max(0, effectiveMetaCapacity) > 0 ? 24L + effectiveMetaCapacity : 0L;
        long dataAndMetaBytes;
        long wholeObjectBytes;

        lock (syncRoot)
        {
            visitedSet.Clear();

            dataBytes = EstimateGraphSize(data, visitedSet, aotContexts);
            dataAndMetaBytes = 32L + dataBytes + metadataBytes;

            long managerOverheadBytes = 416;
            managerOverheadBytes += EstimateStringSize(documentId, visitedSet);
            managerOverheadBytes += EstimateStringSize(shardId, visitedSet);

            if (debounceBufferCapacity >= 0)
            {
                managerOverheadBytes += 160 + (debounceBufferCapacity * 64L);
            }

            wholeObjectBytes = managerOverheadBytes + 32L + dataBytes + allocatedMetadataBytes;

            visitedSet.Clear();
            visitedSet.TrimExcess(64);
        }

        var shardDvvBytes = EstimateShardVersionVectorSize(globalState, shardId);
        return new DocumentMemoryFootprintDto(dataBytes, metadataBytes, dataAndMetaBytes, wholeObjectBytes, shardDvvBytes);
    }

    /// <inheritdoc />
    public DocumentMemoryFootprintDto EstimateFullySerializedDocumentFootprint(
        int serializedDocumentByteLength,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap globalState,
        int allocatedBufferCapacity = 0)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(shardId);
        ArgumentNullException.ThrowIfNull(globalState);

        long byteLength = Math.Max(0, serializedDocumentByteLength);
        long effectiveAllocatedLength = Math.Max(byteLength, allocatedBufferCapacity);
        long dataAndMetaBytes = byteLength > 0 ? 24L + byteLength : 0L;
        long allocatedBufferBytes = effectiveAllocatedLength > 0 ? 24L + effectiveAllocatedLength : 0L;

        long managerOverheadBytes = 416;
        managerOverheadBytes += EstimateRawStringSize(documentId);
        managerOverheadBytes += EstimateRawStringSize(shardId);

        if (debounceBufferCapacity >= 0)
        {
            managerOverheadBytes += 160 + (debounceBufferCapacity * 64L);
        }

        long wholeObjectBytes = managerOverheadBytes + allocatedBufferBytes;
        var shardDvvBytes = EstimateShardVersionVectorSize(globalState, shardId);

        return new DocumentMemoryFootprintDto(byteLength, 0L, dataAndMetaBytes, wholeObjectBytes, shardDvvBytes);
    }

    private static long EstimateShardVersionVectorSize(ShardVersionVectorMap globalState, string shardId)
    {
        long size = 128;
        foreach (var kvp in globalState)
        {
            long entrySize = 64 + EstimateRawStringSize(kvp.Key);
            if (kvp.Value.Versions != null)
            {
                entrySize += 80;
                foreach (var versionKvp in kvp.Value.Versions)
                {
                    entrySize += 48 + EstimateRawStringSize(versionKvp.Key);
                }
            }

            if (kvp.Value.Dots != null)
            {
                entrySize += 80;
                foreach (var dotKvp in kvp.Value.Dots)
                {
                    int dotCount = dotKvp.Value?.Count ?? 0;
                    entrySize += 48 + EstimateRawStringSize(dotKvp.Key) + 64 + (dotCount * 24L);
                }
            }

            size += entrySize;
        }

        return size;
    }

    private static long EstimateStringSize(string? str, HashSet<object> visited)
    {
        if (str is null || !visited.Add(str))
        {
            return 0;
        }

        return EstimateRawStringSize(str);
    }

    private static long EstimateRawStringSize(string? str)
    {
        if (str is null)
        {
            return 0;
        }

        long raw = 22L + ((long)str.Length * sizeof(char));
        return (raw + 7L) & ~7L;
    }

    private static long EstimateGraphSize(object? obj, HashSet<object> visited, IReadOnlyList<CrdtAotContext> aotContexts)
    {
        if (obj is null)
        {
            return 0;
        }

        if (obj is string str)
        {
            return EstimateStringSize(str, visited);
        }

        var type = obj.GetType();
        if (IsSimpleValueType(type))
        {
            return GetPrimitiveOrSimpleValueSize(type);
        }

        if (!type.IsValueType && !visited.Add(obj))
        {
            return 0;
        }

        if (obj is byte[] byteArray)
        {
            return 24L + byteArray.LongLength;
        }

        if (obj is Array array)
        {
            var elemType = type.GetElementType();
            if (elemType != null && IsSimpleValueType(elemType))
            {
                return 24L + (array.LongLength * GetPrimitiveOrSimpleValueSize(elemType));
            }

            long arrSize = 24L + (array.LongLength * 8L);
            foreach (var item in array)
            {
                arrSize += EstimateGraphSize(item, visited, aotContexts);
            }

            return arrSize;
        }

        if (obj is CrdtMetadata metadata)
        {
            return EstimateCrdtMetadataSize(metadata, visited, aotContexts);
        }

        if (obj is IDictionary dict)
        {
            // Account for internal Dictionary/ConcurrentDictionary prime-sized bucket and entry arrays (~72 bytes per entry slot with load factor slack).
            long dictSize = 96L + (dict.Count * 72L);
            foreach (DictionaryEntry entry in dict)
            {
                dictSize += EstimateGraphSize(entry.Key, visited, aotContexts);
                dictSize += EstimateGraphSize(entry.Value, visited, aotContexts);
            }

            return dictSize;
        }

        if (obj is ICollection collection)
        {
            long colSize = 48L + (collection.Count * 16L);
            foreach (var item in collection)
            {
                colSize += EstimateGraphSize(item, visited, aotContexts);
            }

            return colSize;
        }

        var aotTypeInfo = ResolveAotTypeInfo(type, aotContexts);
        if (aotTypeInfo != null)
        {
            long objSize = type.IsValueType ? 0L : 24L;
            foreach (var prop in aotTypeInfo.Properties.Values)
            {
                if (!prop.CanRead || prop.Getter == null)
                {
                    continue;
                }

                if (IsSimpleValueType(prop.PropertyType))
                {
                    objSize += GetPrimitiveOrSimpleValueSize(prop.PropertyType);
                }
                else
                {
                    objSize += type.IsValueType ? 0L : 8L;
                    var val = prop.Getter(obj);
                    objSize += EstimateGraphSize(val, visited, aotContexts);
                }
            }

            return Math.Max(objSize, type.IsValueType ? 8L : 24L);
        }

        if (obj is IEnumerable enumerable)
        {
            long enumSize = 48L;
            foreach (var item in enumerable)
            {
                enumSize += 16L + EstimateGraphSize(item, visited, aotContexts);
            }

            return enumSize;
        }

        return 32L;
    }

    private static long EstimateCrdtMetadataSize(CrdtMetadata metadata, HashSet<object> visited, IReadOnlyList<CrdtAotContext> aotContexts)
    {
        long size = 128L;

        size += EstimateGraphSize(metadata.States, visited, aotContexts);
        size += EstimateGraphSize(metadata.VersionVector, visited, aotContexts);

        if (metadata.SeenExceptions != null && visited.Add(metadata.SeenExceptions))
        {
            size += 64L + (metadata.SeenExceptions.Count * 72L);
            foreach (var op in metadata.SeenExceptions)
            {
                size += EstimateStringSize(op.ReplicaId, visited);
                size += EstimateStringSize(op.JsonPath, visited);
                size += EstimateGraphSize(op.Value, visited, aotContexts);
            }
        }

        return size;
    }

    private static CrdtTypeInfo? ResolveAotTypeInfo(Type type, IReadOnlyList<CrdtAotContext> aotContexts)
    {
        for (int i = 0; i < aotContexts.Count; i++)
        {
            var info = aotContexts[i].GetTypeInfo(type);
            if (info != null)
            {
                return info;
            }
        }

        return null;
    }

    private static bool IsSimpleValueType(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        return underlying.IsPrimitive
            || underlying.IsEnum
            || underlying == typeof(decimal)
            || underlying == typeof(Guid)
            || underlying == typeof(DateTime)
            || underlying == typeof(DateTimeOffset)
            || underlying == typeof(TimeSpan);
    }

    private static long GetPrimitiveOrSimpleValueSize(Type type)
    {
        var underlying = Nullable.GetUnderlyingType(type) ?? type;
        long extraNullable = Nullable.GetUnderlyingType(type) != null ? 4L : 0L;

        if (underlying == typeof(bool) || underlying == typeof(byte) || underlying == typeof(sbyte))
        {
            return 1L + extraNullable;
        }

        if (underlying == typeof(short) || underlying == typeof(ushort) || underlying == typeof(char))
        {
            return 2L + extraNullable;
        }

        if (underlying == typeof(int) || underlying == typeof(uint) || underlying == typeof(float))
        {
            return 4L + extraNullable;
        }

        if (underlying == typeof(long) || underlying == typeof(ulong) || underlying == typeof(double) || underlying == typeof(DateTime) || underlying == typeof(TimeSpan) || underlying == typeof(nint) || underlying == typeof(nuint))
        {
            return 8L + extraNullable;
        }

        if (underlying == typeof(decimal) || underlying == typeof(Guid) || underlying == typeof(DateTimeOffset))
        {
            return 16L + extraNullable;
        }

        if (underlying.IsEnum)
        {
            return GetPrimitiveOrSimpleValueSize(Enum.GetUnderlyingType(underlying)) + extraNullable;
        }

        return 8L + extraNullable;
    }
}