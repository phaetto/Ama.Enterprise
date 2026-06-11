namespace Ama.Enterprise.CRDT.MessagePack.IntegrationTests.Services;

using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.MessagePack.Extensions;
using Ama.Enterprise.CRDT.MessagePack.Formatters;
using Ama.Enterprise.CRDT.MessagePack.UnitTests.Models;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using Xunit;

public sealed class TolerantReaderIntegrationTests
{
    private readonly ICrdtSerializer serializer;

    public TolerantReaderIntegrationTests()
    {
        var services = new ServiceCollection();
        services.AddCrdtMessagePack(Ama_Enterprise_CRDT_MessagePack_IntegrationTests_MessagePackResolver.Instance);
        var provider = services.BuildServiceProvider();
        serializer = provider.GetRequiredService<ICrdtSerializer>();
    }

    [Fact]
    public void TolerantReader_UpgradePath_V2ToV1ToV2_ShouldPreserveUnknownTrailingElements()
    {
        // 1. Arrange: Create a V2 payload with newly added fields.
        var v2Original = new TolerantReaderPayloadV2
        {
            Id = 42,
            Name = "Alice",
            FutureIntField = 999,
            FutureStringField = "FutureAddress"
        };

        var v2Bytes = serializer.SerializeToBytes(v2Original);

        // 2. Act: Read as V1 mapping known properties and trapping trailing items.
        var v1Payload = serializer.DeserializeFromBytes<TolerantReaderPayload>(v2Bytes);
        
        // Assert resolution and trap mapping 
        v1Payload.ShouldNotBeNull();
        v1Payload.Id.ShouldBe(42);
        v1Payload.Name.ShouldBe("Alice");
        v1Payload.BinaryExtensionData.ShouldNotBeNull();
        
        // MessagePack array format mapping maps properties in sequence. 
        // V2 appended FutureIntField and FutureStringField mapped dynamically.
        v1Payload.BinaryExtensionData.Count.ShouldBe(2);

        // 3. Act: Serialize the V1 payload back out 
        var v1Bytes = serializer.SerializeToBytes(v1Payload);
        
        // 4. Assert: Verify the exact original binary V2 structure was preserved.
        var v2Restored = serializer.DeserializeFromBytes<TolerantReaderPayloadV2>(v1Bytes);
        
        v2Restored.ShouldNotBeNull();
        v2Restored.Id.ShouldBe(42);
        v2Restored.Name.ShouldBe("Alice");
        v2Restored.FutureIntField.ShouldBe(999);
        v2Restored.FutureStringField.ShouldBe("FutureAddress");
    }

    [Fact]
    public void TolerantReader_UpgradePath_V1ToV2_ShouldMapKnownPropertiesAndDefaultNewFields()
    {
        // 1. Arrange: Create an explicit V1 payload evaluating backward compatibility.
        var v1Original = new TolerantReaderPayload
        {
            Id = 100,
            Name = "Bob"
        };
        
        var v1Bytes = serializer.SerializeToBytes(v1Original);
        
        // 2. Act: Read as V2 payload mapping default missing array elements.
        var v2Payload = serializer.DeserializeFromBytes<TolerantReaderPayloadV2>(v1Bytes);
        
        // 3. Assert: Verify standard mappings initialized and defaults appended.
        v2Payload.ShouldNotBeNull();
        v2Payload.Id.ShouldBe(100);
        v2Payload.Name.ShouldBe("Bob");
        v2Payload.FutureIntField.ShouldBe(0);
        v2Payload.FutureStringField.ShouldBeNullOrEmpty();
    }
}