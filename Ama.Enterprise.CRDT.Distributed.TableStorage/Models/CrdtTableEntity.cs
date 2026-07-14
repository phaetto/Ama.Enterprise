namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Data.Tables;

/// <summary>
/// Static helper for mapping Azure Table Storage entity chunking to persist CRDT state payloads up to ~960KB natively avoiding AOT reflection constraints.
/// </summary>
public static class CrdtTableEntity
{
    /// <summary>
    /// Recombines chunked byte arrays into the unified document payload from a native dictionary-backed TableEntity.
    /// </summary>
    /// <param name="entity">The native TableEntity containing the chunked bounds.</param>
    /// <returns>The un-chunked continuous serialized bytes.</returns>
    public static byte[] GetPayload(TableEntity entity)
    {
        ArgumentNullException.ThrowIfNull(entity);

        var chunks = new List<byte[]>(15);

        for (int i = 0; i <= 14; i++)
        {
            if (entity.TryGetValue($"P{i}", out var value) && value is byte[] chunk)
            {
                chunks.Add(chunk);
            }
            else
            {
                break;
            }
        }

        if (chunks.Count == 0)
        {
            return Array.Empty<byte>();
        }

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
    /// Segments serialized payloads into 64KB bounds properties on a native dictionary-backed TableEntity.
    /// </summary>
    /// <param name="entity">The native TableEntity to populate.</param>
    /// <param name="data">The full continuous serialized document or journal payload.</param>
    /// <exception cref="InvalidOperationException">Thrown if data exceeds the 15 segment limit.</exception>
    public static void SetPayload(TableEntity entity, byte[] data)
    {
        ArgumentNullException.ThrowIfNull(entity);
        ArgumentNullException.ThrowIfNull(data);

        var offset = 0;
        const int chunkLimit = 64000;
        var chunkIndex = 0;

        while (offset < data.Length && chunkIndex <= 14)
        {
            var length = Math.Min(data.Length - offset, chunkLimit);
            var chunk = new byte[length];
            Buffer.BlockCopy(data, offset, chunk, 0, length);
            
            entity[$"P{chunkIndex}"] = chunk;
            
            offset += length;
            chunkIndex++;
        }

        if (offset < data.Length)
        {
            throw new InvalidOperationException("Payload exceeds natively supported maximum Table Storage entity sizes bounds.");
        }
    }
}