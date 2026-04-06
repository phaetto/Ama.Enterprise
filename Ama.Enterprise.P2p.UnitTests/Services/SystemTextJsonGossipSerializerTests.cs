using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Shouldly;

namespace Ama.Enterprise.P2p.UnitTests.Services;

public sealed class SystemTextJsonGossipSerializerTests
{
    private readonly SystemTextJsonGossipSerializer serializer;

    public SystemTextJsonGossipSerializerTests()
    {
        this.serializer = new SystemTextJsonGossipSerializer();
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
        deserializedMessage.ShouldNotBeNull();
        deserializedMessage.Value.MessageId.ShouldBe(messageId);
        deserializedMessage.Value.SenderId.ShouldBe(senderId);
        deserializedMessage.Value.TimeToLive.ShouldBe(5);
        deserializedMessage.Value.Payload.ToArray().ShouldBe(payload);
    }

    [Fact]
    public void Deserialize_EmptyData_ShouldReturnNull()
    {
        // Act
        var result = this.serializer.Deserialize(ReadOnlyMemory<byte>.Empty);

        // Assert
        result.ShouldBeNull();
    }

    [Fact]
    public void Deserialize_InvalidJsonData_ShouldReturnNull()
    {
        // Arrange
        var invalidJson = new byte[] { 123, 34, 105, 110, 118, 97, 108, 105, 100 }; // Broken JSON array/object bytes

        // Act
        var result = this.serializer.Deserialize(invalidJson);

        // Assert
        result.ShouldBeNull();
    }
}