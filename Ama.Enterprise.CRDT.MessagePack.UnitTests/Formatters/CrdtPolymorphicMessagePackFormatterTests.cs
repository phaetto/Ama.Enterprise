namespace Ama.Enterprise.CRDT.MessagePack.UnitTests.Formatters;

using System;
using Ama.Enterprise.CRDT.MessagePack.Formatters;
using global::MessagePack;
using Shouldly;
using Xunit;

public sealed class CrdtPolymorphicMessagePackFormatterTests
{
    private readonly CrdtPolymorphicMessagePackFormatter<object> sut;
    private readonly MessagePackSerializerOptions options;

    public CrdtPolymorphicMessagePackFormatterTests()
    {
        sut = new CrdtPolymorphicMessagePackFormatter<object>();
        options = MessagePackSerializerOptions.Standard;
    }

    [Fact]
    public void Serialize_NullValue_WritesNil()
    {
        var buffer = new global::System.Buffers.ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);

        sut.Serialize(ref writer, null, options);
        writer.Flush();

        var bytes = buffer.WrittenSpan.ToArray();
        bytes.Length.ShouldBe(1);
        bytes[0].ShouldBe(MessagePackCode.Nil);
    }

    [Fact]
    public void Deserialize_NilValue_ReturnsNull()
    {
        var bytes = new byte[] { MessagePackCode.Nil };
        var reader = new MessagePackReader(bytes);

        var result = sut.Deserialize(ref reader, options);

        result.ShouldBeNull();
    }

    [Fact]
    public void Serialize_UnregisteredType_ThrowsNotSupportedException()
    {
        var buffer = new global::System.Buffers.ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        var obj = new UnregisteredModel();

        NotSupportedException? exception = null;

        try
        {
            sut.Serialize(ref writer, obj, options);
        }
        catch (NotSupportedException ex)
        {
            exception = ex;
        }

        exception.ShouldNotBeNull();
        exception!.Message.ShouldContain("is not registered in CrdtTypeRegistry");
    }

    [Fact]
    public void Deserialize_InvalidArrayLength_ThrowsMessagePackSerializationException()
    {
        var buffer = new global::System.Buffers.ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(3);
        writer.WriteNil();
        writer.WriteNil();
        writer.WriteNil();
        writer.Flush();

        var reader = new MessagePackReader(buffer.WrittenSpan.ToArray());

        MessagePackSerializationException? exception = null;

        try
        {
            sut.Deserialize(ref reader, options);
        }
        catch (MessagePackSerializationException ex)
        {
            exception = ex;
        }

        exception.ShouldNotBeNull();
        exception!.Message.ShouldContain("Expected array of length 2");
    }

    [Fact]
    public void Deserialize_UnregisteredDiscriminator_ThrowsNotSupportedException()
    {
        var buffer = new global::System.Buffers.ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(2);
        writer.Write("unknown_discriminator_123");
        writer.WriteNil();
        writer.Flush();

        var reader = new MessagePackReader(buffer.WrittenSpan.ToArray());

        NotSupportedException? exception = null;

        try
        {
            sut.Deserialize(ref reader, options);
        }
        catch (NotSupportedException ex)
        {
            exception = ex;
        }

        exception.ShouldNotBeNull();
        exception!.Message.ShouldContain("is not registered in CrdtTypeRegistry");
    }

    private sealed class UnregisteredModel
    {
    }
}