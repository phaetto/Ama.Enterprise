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
    public void SerializeToString_ShouldSerializeAndDeserializeGeneric()
    {
        var data = new TestModel { Name = "Test3", Value = 789 };

        var base64 = sut.SerializeToString(data);
        var result = sut.DeserializeFromString<TestModel>(base64);

        result.ShouldNotBeNull();
        result.ShouldBe(data);
    }

    [Fact]
    public void SerializeToString_ShouldSerializeAndDeserializeObject()
    {
        var data = new TestModel { Name = "Test4", Value = 101112 };

        var base64 = sut.SerializeToString((object)data, typeof(TestModel));
        var result = sut.DeserializeFromString(base64, typeof(TestModel)) as TestModel;

        result.ShouldNotBeNull();
        result.ShouldBe(data);
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