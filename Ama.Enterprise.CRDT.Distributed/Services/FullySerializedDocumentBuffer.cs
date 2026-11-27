namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Buffers;
using System.Collections.Generic;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// In-memory document buffer that keeps the entire <see cref="CrdtDocument{TState}"/> serialized inside an in-place
/// reusable <see cref="ArrayPool{T}"/> byte buffer to minimize managed heap objects and eliminate checkpoint serialization overhead.
/// </summary>
/// <typeparam name="TState">The CRDT state model type.</typeparam>
public sealed class FullySerializedDocumentBuffer<TState> : ICrdtDocumentBuffer<TState> where TState : class, new()
{
    private readonly ICrdtSerializer serializer;
    private byte[]? rentedDocumentBuffer;
    private int documentLength;
    private bool isDisposed;

    /// <inheritdoc />
    public CrdtDocumentResidencyMode Mode => CrdtDocumentResidencyMode.FullySerialized;

    /// <summary>
    /// Initializes a new instance of the <see cref="FullySerializedDocumentBuffer{TState}"/> class.
    /// </summary>
    /// <param name="initialDocument">The initial CRDT document to serialize into the pooled buffer.</param>
    /// <param name="serializer">The CRDT serializer.</param>
    public FullySerializedDocumentBuffer(CrdtDocument<TState> initialDocument, ICrdtSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(initialDocument.Data);
        ArgumentNullException.ThrowIfNull(initialDocument.Metadata);
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        StoreFullDocument(initialDocument);
    }

    /// <inheritdoc />
    public TState GetData()
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        var fullDoc = UnpackFullDocument();
        return fullDoc.Data!;
    }

    /// <inheritdoc />
    public CrdtDocument<TState> UnpackFullDocument()
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        if (rentedDocumentBuffer == null || documentLength <= 0)
        {
            throw new InvalidOperationException("The serialized document buffer is empty.");
        }

        var span = new ReadOnlySpan<byte>(rentedDocumentBuffer, 0, documentLength);
        return serializer.DeserializeFromBytes<CrdtDocument<TState>>(span);
    }

    /// <inheritdoc />
    public void StoreFullDocument(CrdtDocument<TState> document)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        ArgumentNullException.ThrowIfNull(document.Data);
        ArgumentNullException.ThrowIfNull(document.Metadata);

        var serialized = serializer.SerializeToBytes(document);
        CopyIntoPooledBuffer(serialized);
    }

    /// <inheritdoc />
    public void StoreSerializedBytes(ReadOnlySpan<byte> serializedDocumentBytes)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        if (serializedDocumentBytes.IsEmpty)
        {
            throw new ArgumentException("Serialized document bytes cannot be empty.", nameof(serializedDocumentBytes));
        }

        CopyIntoPooledBuffer(serializedDocumentBytes);
    }

    /// <inheritdoc />
    public byte[] GetSerializedBytes()
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        if (rentedDocumentBuffer == null || documentLength <= 0)
        {
            return Array.Empty<byte>();
        }

        var copy = new byte[documentLength];
        Buffer.BlockCopy(rentedDocumentBuffer, 0, copy, 0, documentLength);
        return copy;
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

        return estimator.EstimateFullySerializedDocumentFootprint(
            documentLength,
            documentId,
            shardId,
            debounceBufferCapacity,
            shardDvv);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        isDisposed = true;
        var bufferToReturn = rentedDocumentBuffer;
        rentedDocumentBuffer = null;
        documentLength = 0;

        if (bufferToReturn != null)
        {
            ArrayPool<byte>.Shared.Return(bufferToReturn);
        }
    }

    private void CopyIntoPooledBuffer(ReadOnlySpan<byte> source)
    {
        var requiredLength = source.Length;
        if (requiredLength == 0)
        {
            documentLength = 0;
            return;
        }

        if (rentedDocumentBuffer == null || rentedDocumentBuffer.Length < requiredLength)
        {
            var previousBuffer = rentedDocumentBuffer;
            rentedDocumentBuffer = ArrayPool<byte>.Shared.Rent(requiredLength);
            if (previousBuffer != null)
            {
                ArrayPool<byte>.Shared.Return(previousBuffer);
            }
        }

        source.CopyTo(rentedDocumentBuffer);
        documentLength = requiredLength;
    }
}