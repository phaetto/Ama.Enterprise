namespace Ama.Enterprise.CRDT.MessagePack.UnitTests.Formatters;

using System;
using System.Buffers;
using Ama.Enterprise.CRDT.MessagePack.Formatters;
using global::MessagePack;
using Shouldly;
using Xunit;

public sealed class CrdtPolymorphicMessagePackFormatterTests
{
    private readonly CrdtPolymorphicMessagePackFormatter<IPolymorphicDummy> sut;
    private readonly MessagePackSerializerOptions options;

    public CrdtPolymorphicMessagePackFormatterTests()
    {
        sut = new CrdtPolymorphicMessagePackFormatter<IPolymorphicDummy>();
        options = MessagePackSerializerOptions.Standard;
    }

    public interface IPolymorphicDummy { }
    
    public sealed record UnregisteredDummy : IPolymorphicDummy { }

    [Fact]
    public void Serialize_WhenValueIsNull_WritesNil()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);

        sut.Serialize(ref writer, null, options);
        writer.Flush();

        var reader = new MessagePackReader(buffer.WrittenMemory);
        var isNil = reader.TryReadNil();
        
        isNil.ShouldBeTrue();
    }

    [Fact]
    public void Serialize_WhenTypeIsNotRegisteredInCrdtTypeRegistry_ThrowsNotSupportedException()
    {
        var value = new UnregisteredDummy();

        // Create the writer inside the lambda to avoid capturing it as a ref variable
        var exception = Should.Throw<NotSupportedException>(() =>
        {
            var buffer = new ArrayBufferWriter<byte>();
            var writer = new MessagePackWriter(buffer);
            sut.Serialize(ref writer, value, options);
        });
        
        exception.Message.ShouldContain("is not registered in CrdtTypeRegistry");
    }

    [Fact]
    public void Deserialize_WhenReaderIsNil_ReturnsDefault()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteNil();
        writer.Flush();

        var reader = new MessagePackReader(buffer.WrittenMemory);
        var result = sut.Deserialize(ref reader, options);

        result.ShouldBeNull();
    }

    [Fact]
    public void Deserialize_WhenNotAnArray_ThrowsMessagePackSerializationException()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.Write("invalid payload");
        writer.Flush();

        // Create the reader inside the lambda to avoid capturing it as a ref variable
        Should.Throw<MessagePackSerializationException>(() =>
        {
            var reader = new MessagePackReader(buffer.WrittenMemory);
            sut.Deserialize(ref reader, options);
        });
    }

    [Fact]
    public void Deserialize_WhenArrayLengthIsNotTwo_ThrowsMessagePackSerializationException()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(3); // Invalid size
        writer.Write("discriminator");
        writer.Write("payload1");
        writer.Write("payload2");
        writer.Flush();

        // Create the reader inside the lambda to avoid capturing it as a ref variable
        var exception = Should.Throw<MessagePackSerializationException>(() =>
        {
            var reader = new MessagePackReader(buffer.WrittenMemory);
            sut.Deserialize(ref reader, options);
        });
        
        exception.Message.ShouldContain("Expected array of length 2");
    }

    [Fact]
    public void Deserialize_WhenDiscriminatorIsUnknown_ThrowsNotSupportedException()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(2);
        writer.Write("UnknownDiscriminator999");
        writer.WriteNil();
        writer.Flush();

        // Create the reader inside the lambda to avoid capturing it as a ref variable
        var exception = Should.Throw<NotSupportedException>(() =>
        {
            var reader = new MessagePackReader(buffer.WrittenMemory);
            sut.Deserialize(ref reader, options);
        });
        
        exception.Message.ShouldContain("not registered in CrdtTypeRegistry");
    }
}