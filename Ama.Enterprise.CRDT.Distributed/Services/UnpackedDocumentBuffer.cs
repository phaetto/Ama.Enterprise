namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Aot;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// In-memory document buffer that keeps the full <see cref="CrdtDocument{TState}"/> (both <typeparamref name="TState"/> and <see cref="CrdtMetadata"/>)
/// materialized on the managed heap for zero-serialization read and write hot paths.
/// </summary>
/// <typeparam name="TState">The CRDT state model type.</typeparam>
public sealed class UnpackedDocumentBuffer<TState> : ICrdtDocumentBuffer<TState> where TState : class, new()
{
    private readonly ICrdtSerializer serializer;
    private CrdtDocument<TState> document;

    /// <inheritdoc />
    public CrdtDocumentResidencyMode Mode => CrdtDocumentResidencyMode.Unpacked;

    /// <summary>
    /// Initializes a new instance of the <see cref="UnpackedDocumentBuffer{TState}"/> class.
    /// </summary>
    /// <param name="initialDocument">The initial materialized CRDT document.</param>
    /// <param name="serializer">The CRDT serializer.</param>
    public UnpackedDocumentBuffer(CrdtDocument<TState> initialDocument, ICrdtSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(initialDocument.Data);
        ArgumentNullException.ThrowIfNull(initialDocument.Metadata);
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.document = initialDocument;
    }

    /// <inheritdoc />
    public TState GetData()
    {
        return document.Data!;
    }

    /// <inheritdoc />
    public CrdtDocument<TState> UnpackFullDocument()
    {
        return document;
    }

    /// <inheritdoc />
    public void StoreFullDocument(CrdtDocument<TState> document)
    {
        ArgumentNullException.ThrowIfNull(document.Data);
        ArgumentNullException.ThrowIfNull(document.Metadata);
        this.document = document;
    }

    /// <inheritdoc />
    public void StoreSerializedBytes(ReadOnlySpan<byte> serializedDocumentBytes)
    {
        if (serializedDocumentBytes.IsEmpty)
        {
            throw new ArgumentException("Serialized document bytes cannot be empty.", nameof(serializedDocumentBytes));
        }

        document = serializer.DeserializeFromBytes<CrdtDocument<TState>>(serializedDocumentBytes);
    }

    /// <inheritdoc />
    public byte[] GetSerializedBytes()
    {
        return serializer.SerializeToBytes(document);
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
        ArgumentNullException.ThrowIfNull(estimator);
        return estimator.EstimateDocumentFootprint(
            document,
            documentId,
            shardId,
            debounceBufferCapacity,
            shardDvv,
            aotContexts);
    }

    /// <inheritdoc />
    public void Dispose()
    {
    }
}