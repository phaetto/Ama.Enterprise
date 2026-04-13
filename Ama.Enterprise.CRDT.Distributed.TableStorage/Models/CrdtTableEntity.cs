namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using Azure;
using Azure.Data.Tables;

/// <summary>
/// Representation of an Azure Table Storage entity capable of chunking CRDT state payloads up to ~960KB avoiding 64KB strict column limits.
/// </summary>
public sealed class CrdtTableEntity : ITableEntity
{
    /// <inheritdoc />
    public string PartitionKey { get; set; } = string.Empty;

    /// <inheritdoc />
    public string RowKey { get; set; } = string.Empty;

    /// <inheritdoc />
    public DateTimeOffset? Timestamp { get; set; }

    /// <inheritdoc />
    public ETag ETag { get; set; }

    public byte[]? P0 { get; set; }
    public byte[]? P1 { get; set; }
    public byte[]? P2 { get; set; }
    public byte[]? P3 { get; set; }
    public byte[]? P4 { get; set; }
    public byte[]? P5 { get; set; }
    public byte[]? P6 { get; set; }
    public byte[]? P7 { get; set; }
    public byte[]? P8 { get; set; }
    public byte[]? P9 { get; set; }
    public byte[]? P10 { get; set; }
    public byte[]? P11 { get; set; }
    public byte[]? P12 { get; set; }
    public byte[]? P13 { get; set; }
    public byte[]? P14 { get; set; }

    /// <summary>
    /// Recombines chunked byte arrays into the unified document payload.
    /// </summary>
    /// <returns>The un-chunked continuous serialized bytes.</returns>
    public byte[] GetPayload()
    {
        var chunks = new List<byte[]>();
        
        if (this.P0 != null) chunks.Add(this.P0);
        if (this.P1 != null) chunks.Add(this.P1);
        if (this.P2 != null) chunks.Add(this.P2);
        if (this.P3 != null) chunks.Add(this.P3);
        if (this.P4 != null) chunks.Add(this.P4);
        if (this.P5 != null) chunks.Add(this.P5);
        if (this.P6 != null) chunks.Add(this.P6);
        if (this.P7 != null) chunks.Add(this.P7);
        if (this.P8 != null) chunks.Add(this.P8);
        if (this.P9 != null) chunks.Add(this.P9);
        if (this.P10 != null) chunks.Add(this.P10);
        if (this.P11 != null) chunks.Add(this.P11);
        if (this.P12 != null) chunks.Add(this.P12);
        if (this.P13 != null) chunks.Add(this.P13);
        if (this.P14 != null) chunks.Add(this.P14);

        var totalLength = chunks.Sum(c => c.Length);
        var result = new byte[totalLength];
        var offset = 0;

        foreach (var chunk in chunks)
        {
            Buffer.BlockCopy(chunk, 0, result, offset, chunk.Length);
            offset += chunk.Length;
        }

        return result;
    }

    /// <summary>
    /// Segments serialized payloads into 64KB bounds properties.
    /// </summary>
    /// <param name="data">The full continuous serialized document or journal payload.</param>
    /// <exception cref="InvalidOperationException">Thrown if data exceeds the 15 segment limit.</exception>
    public void SetPayload(byte[] data)
    {
        if (data == null) throw new ArgumentNullException(nameof(data));

        var offset = 0;
        const int chunkLimit = 64000;

        if (offset < data.Length) this.P0 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P1 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P2 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P3 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P4 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P5 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P6 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P7 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P8 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P9 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P10 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P11 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P12 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P13 = TakeChunk(data, ref offset, chunkLimit);
        if (offset < data.Length) this.P14 = TakeChunk(data, ref offset, chunkLimit);

        if (offset < data.Length)
        {
            throw new InvalidOperationException("Payload exceeds natively supported maximum Table Storage entity sizes bounds.");
        }
    }

    private static byte[] TakeChunk(byte[] data, ref int offset, int chunkLimit)
    {
        var length = Math.Min(data.Length - offset, chunkLimit);
        var chunk = new byte[length];
        Buffer.BlockCopy(data, offset, chunk, 0, length);
        offset += length;
        return chunk;
    }
}