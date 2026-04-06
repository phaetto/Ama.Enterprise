namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Gossip;
using Shouldly;
using Xunit;

/// <summary>
/// AOT-friendly JSON context tailored for testing to avoid dependency on internal implementation contexts.
/// </summary>
[JsonSerializable(typeof(GossipMessage))]
internal partial class TestP2pJsonContext : JsonSerializerContext
{
}

public sealed class SystemTextJsonGossipSerializerTests
{
    private readonly SystemTextJsonGossipSerializer serializer;

    public SystemTextJsonGossipSerializerTests()
    {
        var options = new JsonSerializerOptions
        {
            TypeInfoResolver = TestP2pJsonContext.Default
        };
        
        this.serializer = new SystemTextJsonGossipSerializer(options);
    }

    [Fact]
    public void Constructor_NullOptions_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        Should.Throw<ArgumentNullException>(() => new SystemTextJsonGossipSerializer(null!));
    }

    [Fact]
    public void SerializeAndDeserialize_ValidMessage_ShouldReconstructSuccessfully()
    {
        // Arrange
        var messageId = Guid.NewGuid();
        var senderId = new PeerId(Guid.NewGuid());
        var payload = new byte[] { 10, 20, 30, 40 };
        var message = new GossipMessage(messageId, senderId, 5, payload);

        // Act
        var serializedData = this.serializer.Serialize(message);
        var deserializedMessage = this.serializer.Deserialize(serializedData);

        // Assert
        serializedData.IsEmpty.ShouldBeFalse();
        deserializedMessage.MessageId.ShouldBe(messageId);
        deserializedMessage.SenderId.ShouldBe(senderId);
        deserializedMessage.TimeToLive.ShouldBe(5);
        deserializedMessage.Payload.ToArray().ShouldBe(payload);
    }

    [Fact]
    public void Deserialize_EmptyData_ShouldReturnDefault()
    {
        // Act
        var result = this.serializer.Deserialize(ReadOnlyMemory<byte>.Empty);

        // Assert
        result.MessageId.ShouldBe(Guid.Empty);
    }

    [Fact]
    public void Deserialize_InvalidJsonData_ShouldReturnDefault()
    {
        // Arrange
        var invalidJson = new byte[] { 123, 34, 105, 110, 118, 97, 108, 105, 100 }; // Broken JSON array/object bytes

        // Act
        var result = this.serializer.Deserialize(invalidJson);

        // Assert
        result.MessageId.ShouldBe(Guid.Empty);
    }
}