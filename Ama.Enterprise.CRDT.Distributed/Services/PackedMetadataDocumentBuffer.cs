namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Buffers;
using System.Collections.Generic;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// In-memory document buffer that keeps <typeparamref name="TState"/> materialized on the heap for O(1) data reads
/// while packing <see cref="CrdtMetadata"/> into an in-place reusable <see cref="ArrayPool{T}"/> byte buffer to minimize Gen-2 GC pressure.
/// </summary>
/// <typeparam name="TState">The CRDT state model type.</typeparam>
public sealed class PackedMetadataDocumentBuffer<TState> : ICrdtDocumentBuffer<TState> where TState : class, new()
{
    private readonly ICrdtMetadataManager metadataManager;
    private readonly ICrdtSerializer serializer;
    private TState data;
    private byte[]? rentedMetadataBuffer;
    private int metadataLength;
    private bool isDisposed;

    /// <inheritdoc />
    public CrdtDocumentResidencyMode Mode => CrdtDocumentResidencyMode.PackedMetadata;

    /// <summary>
    /// Initializes a new instance of the <see cref="PackedMetadataDocumentBuffer{TState}"/> class.
    /// </summary>
    /// <param name="initialDocument">The initial CRDT document to pack.</param>
    /// <param name="metadataManager">The CRDT metadata manager used to pack and unpack metadata.</param>
    /// <param name="serializer">The CRDT serializer.</param>
    public PackedMetadataDocumentBuffer(
        CrdtDocument<TState> initialDocument,
        ICrdtMetadataManager metadataManager,
        ICrdtSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(initialDocument.Data);
        ArgumentNullException.ThrowIfNull(initialDocument.Metadata);
        this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.data = initialDocument.Data;
        PackAndCopyMetadata(initialDocument);
    }

    /// <inheritdoc />
    public TState GetData()
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        return data;
    }

    /// <inheritdoc />
    public CrdtDocument<TState> UnpackFullDocument()
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var metadataMemory = rentedMetadataBuffer != null && metadataLength > 0
            ? new ReadOnlyMemory<byte>(rentedMetadataBuffer, 0, metadataLength)
            : ReadOnlyMemory<byte>.Empty;

        var packed = new PackedCrdtDocument<TState>(data, metadataMemory);
        return metadataManager.Unpack(packed);
    }

    /// <inheritdoc />
    public void StoreFullDocument(CrdtDocument<TState> document)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(document.Data);
        ArgumentNullException.ThrowIfNull(document.Metadata);

        data = document.Data;
        PackAndCopyMetadata(document);
    }

    /// <inheritdoc />
    public void StoreSerializedBytes(ReadOnlySpan<byte> serializedDocumentBytes)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        if (serializedDocumentBytes.IsEmpty)
        {
            throw new ArgumentException("Serialized document bytes cannot be empty.", nameof(serializedDocumentBytes));
        }

        var fullDoc = serializer.DeserializeFromBytes<CrdtDocument<TState>>(serializedDocumentBytes);
        StoreFullDocument(fullDoc);
    }

    /// <inheritdoc />
    public byte[] GetSerializedBytes()
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        var fullDoc = UnpackFullDocument();
        return serializer.SerializeToBytes(fullDoc);
    }

    /// <inheritdoc />
    public DocumentMemoryFootprintDto EstimateFootprint(
        ICrdtMemoryEstimator estimator,
        string documentId,
        string shardId,
        int debounceBufferCapacity,
        ShardVersionVectorMap shardDvv,
        IReadOnlyList<CrdtAotContext> aotContexts)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(estimator);

        return estimator.EstimatePackedMetadataDocumentFootprint(
            data,
            metadataLength,
            documentId,
            shardId,
            debounceBufferCapacity,
            shardDvv,
            aotContexts);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        isDisposed = true;
        var bufferToReturn = rentedMetadataBuffer;
        rentedMetadataBuffer = null;
        metadataLength = 0;

        if (bufferToReturn != null)
        {
            ArrayPool<byte>.Shared.Return(bufferToReturn);
        }
    }

    private void PackAndCopyMetadata(CrdtDocument<TState> document)
    {
        var packed = metadataManager.Pack(document);
        var span = packed.MetadataBytes.Span;
        var requiredLength = span.Length;

        if (requiredLength == 0)
        {
            metadataLength = 0;
            return;
        }

        if (rentedMetadataBuffer == null || rentedMetadataBuffer.Length < requiredLength)
        {
            var previousBuffer = rentedMetadataBuffer;
            rentedMetadataBuffer = ArrayPool<byte>.Shared.Rent(requiredLength);
            if (previousBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(previousBuffer);
            }
        }

        span.CopyTo(rentedMetadataBuffer);
        metadataLength = requiredLength;
    }
}