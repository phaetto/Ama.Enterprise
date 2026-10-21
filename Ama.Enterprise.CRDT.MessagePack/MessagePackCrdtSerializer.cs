namespace Ama.Enterprise.CRDT.MessagePack;

using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using global::MessagePack;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// A Native AOT compatible binary implementation of <see cref="ICrdtSerializer"/>
/// utilizing MessagePack and external source-generated formatters.
/// </summary>
public sealed class MessagePackCrdtSerializer : ICrdtSerializer
{
    private readonly MessagePackSerializerOptions options;

    public MessagePackCrdtSerializer([FromKeyedServices("Ama.CRDT.MessagePack")] MessagePackSerializerOptions options)
    {
        this.options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc/>
    public Task SerializeAsync<T>(Stream stream, T value, CancellationToken cancellationToken = default)
    {
        return MessagePackSerializer.SerializeAsync(stream, value, options, cancellationToken);
    }

    /// <inheritdoc/>
    public Task SerializeAsync(Stream stream, object value, Type inputType, CancellationToken cancellationToken = default)
    {
        return MessagePackSerializer.SerializeAsync(inputType, stream, value, options, cancellationToken);
    }

    /// <inheritdoc/>
    public async Task<T?> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default)
    {
        return await MessagePackSerializer.DeserializeAsync<T>(stream, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public byte[] SerializeToBytes<T>(T value)
    {
        return MessagePackSerializer.Serialize(value, options);
    }

    /// <inheritdoc/>
    public byte[] SerializeToBytes(object value, Type inputType)
    {
        return MessagePackSerializer.Serialize(inputType, value, options);
    }

    /// <inheritdoc/>
    public T? DeserializeFromBytes<T>(ReadOnlySpan<byte> bytes)
    {
        byte[] rented = ArrayPool<byte>.Shared.Rent(bytes.Length);
        try
        {
            bytes.CopyTo(rented);
            return MessagePackSerializer.Deserialize<T>(rented.AsMemory(0, bytes.Length), options);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <inheritdoc/>
    public object? DeserializeFromBytes(ReadOnlySpan<byte> bytes, Type returnType)
    {
        ArgumentNullException.ThrowIfNull(returnType);

        byte[] rented = ArrayPool<byte>.Shared.Rent(bytes.Length);
        try
        {
            bytes.CopyTo(rented);
            return MessagePackSerializer.Deserialize(returnType, rented.AsMemory(0, bytes.Length), options);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <inheritdoc/>
    public T? Clone<T>(T original)
    {
        if (original is null) return default;
        var bytes = MessagePackSerializer.Serialize(original, options);
        return MessagePackSerializer.Deserialize<T>(bytes, options);
    }
}