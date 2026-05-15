namespace Ama.Enterprise.CRDT.MessagePack.Formatters;

using System;
using global::MessagePack;
using global::MessagePack.Formatters;
using Ama.CRDT.Models.Serialization;

/// <summary>
/// A dynamically mapped MessagePack formatter for handling generic or abstract payloads
/// (e.g., <see cref="object"/>, <see cref="IComparable"/>, ICrdtTimestamp).
/// This bridges the STJ string-based polymorphic 'CrdtTypeRegistry' with binary MessagePack cleanly.
/// </summary>
public sealed class CrdtPolymorphicMessagePackFormatter<T> : IMessagePackFormatter<T?>
{
    public void Serialize(ref MessagePackWriter writer, T? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }

        var type = value.GetType();
        var discriminator = CrdtTypeRegistry.GetDiscriminator(type);

        if (discriminator is null)
        {
            throw new NotSupportedException($"Type '{type}' is not registered in CrdtTypeRegistry. Explicit type registration is required.");
        }

        // We format it as a 2-element array: [ discriminator_string, binary_payload ]
        writer.WriteArrayHeader(2);
        writer.Write(discriminator);

        // Serialize the actual concrete type via the resolver
        MessagePackSerializer.Serialize(type, ref writer, value, options);
    }

    public T? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
        {
            return default;
        }

        var count = reader.ReadArrayHeader();
        if (count != 2)
        {
            throw new MessagePackSerializationException($"Expected array of length 2 for polymorphic type, but got {count}.");
        }

        var discriminator = reader.ReadString();
        if (discriminator is null || !CrdtTypeRegistry.TryGetType(discriminator, out var targetType))
        {
            throw new NotSupportedException($"Type with discriminator '{discriminator}' is not registered in CrdtTypeRegistry.");
        }

        var deserialized = MessagePackSerializer.Deserialize(targetType, ref reader, options);
        return (T?)deserialized;
    }
}