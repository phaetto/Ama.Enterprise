namespace Ama.Enterprise.CRDT.MessagePack;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using global::MessagePack;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// A highly optimized, Native AOT compatible binary implementation of <see cref="ICrdtSerializer"/>
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
        // MessagePack doesn't natively support deserializing directly from ReadOnlySpan<byte>.
        // To avoid unsafe memory pinning, we convert to an array to satisfy ReadOnlyMemory<byte> requirements.
        return MessagePackSerializer.Deserialize<T>(bytes.ToArray(), options);
    }

    /// <inheritdoc/>
    public object? DeserializeFromBytes(ReadOnlySpan<byte> bytes, Type returnType)
    {
        return MessagePackSerializer.Deserialize(returnType, bytes.ToArray(), options);
    }

    /// <inheritdoc/>
    public T? Clone<T>(T original)
    {
        if (original is null) return default;
        var bytes = MessagePackSerializer.Serialize(original, options);
        return MessagePackSerializer.Deserialize<T>(bytes, options);
    }
}