namespace Ama.Enterprise.CRDT.MessagePack.UnitTests.Formatters;

using System;
using Ama.Enterprise.CRDT.MessagePack.Formatters;
using Shouldly;
using Xunit;

public sealed class CrdtPolymorphicMessagePackRegistryTests
{
    // Use unique types to prevent state collisions across parallel executing tests inside the static generic dictionary.
    private sealed record UnregisteredDummyRecord(Guid Id);
    private sealed record RegisteredDummyRecord(Guid Id);

    [Fact]
    public void TryGetSerializer_WhenTypeNotRegistered_ReturnsFalse()
    {
        var result = CrdtPolymorphicMessagePackRegistry.TryGetSerializer(typeof(UnregisteredDummyRecord), out var serializer);

        result.ShouldBeFalse();
        serializer.ShouldBeNull();
    }

    [Fact]
    public void TryGetDeserializer_WhenTypeNotRegistered_ReturnsFalse()
    {
        var result = CrdtPolymorphicMessagePackRegistry.TryGetDeserializer(typeof(UnregisteredDummyRecord), out var deserializer);

        result.ShouldBeFalse();
        deserializer.ShouldBeNull();
    }

    [Fact]
    public void Register_WhenInvoked_PopulatesDelegates()
    {
        CrdtPolymorphicMessagePackRegistry.Register<RegisteredDummyRecord>();

        var hasSerializer = CrdtPolymorphicMessagePackRegistry.TryGetSerializer(typeof(RegisteredDummyRecord), out var serializer);
        var hasDeserializer = CrdtPolymorphicMessagePackRegistry.TryGetDeserializer(typeof(RegisteredDummyRecord), out var deserializer);

        hasSerializer.ShouldBeTrue();
        serializer.ShouldNotBeNull();

        hasDeserializer.ShouldBeTrue();
        deserializer.ShouldNotBeNull();
    }
}