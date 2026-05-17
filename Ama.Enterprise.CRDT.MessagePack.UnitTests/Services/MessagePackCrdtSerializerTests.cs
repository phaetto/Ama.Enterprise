namespace Ama.Enterprise.CRDT.MessagePack.UnitTests.Services;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.MessagePack;
using Ama.Enterprise.CRDT.MessagePack.UnitTests.Models;
using global::MessagePack;
using Shouldly;
using Xunit;

public sealed class MessagePackCrdtSerializerTests
{
    private readonly MessagePackCrdtSerializer sut;

    public MessagePackCrdtSerializerTests()
    {
        var options = MessagePackSerializerOptions.Standard;
        sut = new MessagePackCrdtSerializer(options);
    }

    [Fact]
    public void Constructor_NullOptions_ThrowsArgumentNullException()
    {
        Action action = () => new MessagePackCrdtSerializer(null!);
        action.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void SerializeToBytes_ShouldSerializeAndDeserializeGeneric()
    {
        var data = new TestModel { Name = "Test", Value = 123 };

        var bytes = sut.SerializeToBytes(data);
        var result = sut.DeserializeFromBytes<TestModel>(bytes);

        result.ShouldNotBeNull();
        result.ShouldBe(data);
    }

    [Fact]
    public void SerializeToBytes_ShouldSerializeAndDeserializeObject()
    {
        var data = new TestModel { Name = "Test2", Value = 456 };

        var bytes = sut.SerializeToBytes((object)data, typeof(TestModel));
        var result = sut.DeserializeFromBytes(bytes, typeof(TestModel)) as TestModel;

        result.ShouldNotBeNull();
        result.ShouldBe(data);
    }
    
    [Fact]
    public void SerializeToBytes_ShouldSerializeAndDeserializeRawByteArray()
    {
        byte[] data = [10, 20, 30, 40, 255];
        
        var bytes = sut.SerializeToBytes(data);
        var result = sut.DeserializeFromBytes<byte[]>(bytes);

        result.ShouldNotBeNull();
        result.ShouldBe(data);
        
        // Verifies the payload correctly utilizes MessagePack BIN 8 (0xc4)
        // instead of falling back to a structured ARRAY serialization (0x95).
        bytes[0].ShouldBe((byte)0xc4);
    }

    [Fact]
    public void SerializeToBytes_ShouldSerializeAndDeserializeEmptyByteArray()
    {
        byte[] data = Array.Empty<byte>();
        
        var bytes = sut.SerializeToBytes(data);
        var result = sut.DeserializeFromBytes<byte[]>(bytes);

        result.ShouldNotBeNull();
        result.ShouldBeEmpty();
        
        // Verifies empty arrays also map strictly as MessagePack BIN 8
        bytes[0].ShouldBe((byte)0xc4);
        bytes[1].ShouldBe((byte)0);
    }

    [Fact]
    public void SerializeToBytes_NullInput_ReturnsNilByte()
    {
        TestModel? data = null;
        var bytes = sut.SerializeToBytes(data!);
        bytes.ShouldNotBeNull();
        bytes.Length.ShouldBeGreaterThan(0);
        bytes[0].ShouldBe((byte)0xc0); // MessagePack Nil byte
    }

    [Fact]
    public void DeserializeFromBytes_EmptyInput_ThrowsMessagePackSerializationException()
    {
        Should.Throw<MessagePackSerializationException>(() => sut.DeserializeFromBytes<TestModel>(Array.Empty<byte>()));
    }

    [Fact]
    public async Task SerializeAsync_ShouldSerializeAndDeserializeGeneric()
    {
        var data = new TestModel { Name = "TestAsync", Value = 999 };
        using var stream = new MemoryStream();

        await sut.SerializeAsync(stream, data, CancellationToken.None);
        stream.Position = 0;
        
        var result = await sut.DeserializeAsync<TestModel>(stream, CancellationToken.None);

        result.ShouldNotBeNull();
        result.ShouldBe(data);
    }

    [Fact]
    public async Task SerializeAsync_ShouldSerializeObject()
    {
        var data = new TestModel { Name = "TestAsyncObj", Value = 888 };
        using var stream = new MemoryStream();

        await sut.SerializeAsync(stream, (object)data, typeof(TestModel), CancellationToken.None);
        stream.Position = 0;
        
        var result = await sut.DeserializeAsync<TestModel>(stream, CancellationToken.None);

        result.ShouldNotBeNull();
        result.ShouldBe(data);
    }

    [Fact]
    public async Task SerializeAsync_NullStream_ThrowsException()
    {
        var data = new TestModel { Name = "TestAsyncObj", Value = 888 };
        await Should.ThrowAsync<Exception>(async () => await sut.SerializeAsync(null!, data, CancellationToken.None));
    }

    [Fact]
    public async Task DeserializeAsync_NullStream_ThrowsException()
    {
        await Should.ThrowAsync<Exception>(async () => await sut.DeserializeAsync<TestModel>(null!, CancellationToken.None));
    }

    [Fact]
    public void Clone_ShouldReturnDeepCopy()
    {
        var data = new TestModel { Name = "CloneMe", Value = 777 };

        var result = sut.Clone(data);

        result.ShouldNotBeNull();
        result.ShouldNotBeSameAs(data);
        result.ShouldBe(data);
    }

    [Fact]
    public void Clone_NullInput_ReturnsDefault()
    {
        TestModel? data = null;
        var result = sut.Clone(data);
        result.ShouldBeNull();
    }
}