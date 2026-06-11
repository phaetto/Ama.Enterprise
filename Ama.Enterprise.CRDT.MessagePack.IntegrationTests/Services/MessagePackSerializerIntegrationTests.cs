namespace Ama.Enterprise.CRDT.MessagePack.IntegrationTests.Services;

using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.MessagePack.Extensions;
using Ama.Enterprise.CRDT.MessagePack.Formatters;
using Ama.Enterprise.CRDT.MessagePack.UnitTests.Models;
using Microsoft.Extensions.DependencyInjection;
using Shouldly;
using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

public sealed class MessagePackSerializerIntegrationTests
{
    private readonly ICrdtSerializer crdtSerializer;

    public MessagePackSerializerIntegrationTests()
    {
        crdtSerializer = InitializeSerializer();
    }

    [Fact]
    public void SerializeDeserialize_SimpleValueTypes_ReturnsEqualInstance()
    {
        // Arrange
        var model = new SimpleValueModel(
            Id: 42,
            Name: "Integration Test Value",
            IsActive: true,
            Score: 99.9,
            CreatedAt: new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc),
            Uid: Guid.NewGuid()
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<SimpleValueModel>(bytes);

        // Assert
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_ComplexCollectionModel_ReturnsEqualInstance()
    {
        // Arrange
        var model = new ComplexCollectionModel(
            Tags: new List<string> { "tag-alpha", "tag-beta" },
            Numbers: new List<int> { 1, 1, 2, 3, 5, 8 },
            Map: new Dictionary<string, SimpleValueModel>
            {
                { "key1", new SimpleValueModel(1, "A", true, 1.1, DateTime.UtcNow, Guid.NewGuid()) },
                { "key2", new SimpleValueModel(2, "B", false, -5.5, DateTime.UtcNow.AddDays(-1), Guid.NewGuid()) }
            },
            BinaryData: new byte[] { 0x01, 0x02, 0x03, 0xFF }
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<ComplexCollectionModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_EmptyCollections_ReturnsEqualInstance()
    {
        // Arrange
        var model = new ComplexCollectionModel(
            Tags: new List<string>(),
            Numbers: new List<int>(),
            Map: new Dictionary<string, SimpleValueModel>(),
            BinaryData: Array.Empty<byte>()
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<ComplexCollectionModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_DeepNestedModel_ReturnsEqualInstance()
    {
        // Arrange
        var collections = new ComplexCollectionModel(
            Tags: new List<string> { "nested-tag" },
            Numbers: new List<int> { 42 },
            Map: new Dictionary<string, SimpleValueModel>(),
            BinaryData: Array.Empty<byte>()
        );

        var model = new DeepNestedModel(
            GroupId: Guid.NewGuid(),
            Collections: collections,
            Items: new List<SimpleValueModel>
            {
                new SimpleValueModel(100, "Item 1", false, 0.0, DateTime.UtcNow, Guid.NewGuid()),
                new SimpleValueModel(101, "Item 2", true, 9.9, DateTime.UtcNow, Guid.NewGuid())
            }
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<DeepNestedModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_PolymorphicContainer_RestoresDerivedTypes()
    {
        // Arrange
        var model = new PolymorphicContainer(
            Elements: new List<BasePolymorphicModel>
            {
                new DerivedA("Base1", 123),
                new DerivedB("Base2", "DerivedBString")
            }
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<PolymorphicContainer>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.Elements.ShouldNotBeNull();
        result.Elements.Count.ShouldBe(2);

        var derivedA = result.Elements[0] as DerivedA;
        derivedA.ShouldNotBeNull();
        derivedA.BaseProperty.ShouldBe("Base1");
        derivedA.PropA.ShouldBe(123);

        var derivedB = result.Elements[1] as DerivedB;
        derivedB.ShouldNotBeNull();
        derivedB.BaseProperty.ShouldBe("Base2");
        derivedB.PropB.ShouldBe("DerivedBString");

        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_BoundaryValues_ReturnsEqualInstance()
    {
        // Arrange
        var model = new BoundaryValuesModel(
            MinInt: int.MinValue,
            MaxInt: int.MaxValue,
            NanValue: double.NaN,
            PosInfinity: double.PositiveInfinity,
            MinDate: new DateTime(1, 1, 1, 0, 0, 0, DateTimeKind.Utc), // Ensure UTC enforcement
            MaxDate: new DateTime(9999, 12, 31, 23, 59, 59, 999, DateTimeKind.Utc),
            SpecialString: "Emojis 🚀🔥 and Unicode 漢字 \0 NullChar",
            Status: TestStatus.Completed,
            EmptyGuid: Guid.Empty
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<BoundaryValuesModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_SetModel_ReturnsEqualInstance()
    {
        // Arrange
        var model = new SetModel(
            UniqueTags: new HashSet<string> { "alpha", "beta", "gamma", "alpha" } // Hashset deduplicates automatically
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<SetModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.UniqueTags.Count.ShouldBe(3);
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_ComplexPolymorphicModel_ReturnsEqualInstance()
    {
        // Arrange
        var model = new ComplexPolymorphicModel(
            PolyMap: new Dictionary<string, BasePolymorphicModel>
            {
                { "key-a", new DerivedA("BaseForA", 999) },
                { "key-b", new DerivedB("BaseForB", "DerivedString") }
            }
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<ComplexPolymorphicModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_LargeBinaryPayload_ReturnsEqualInstance()
    {
        // Arrange
        var largeArray = new byte[1024 * 512]; // 512KB to test bulk conversion bypassing limits
        new Random(42).NextBytes(largeArray);

        var model = new ComplexCollectionModel(
            Tags: new List<string>(),
            Numbers: new List<int>(),
            Map: new Dictionary<string, SimpleValueModel>(),
            BinaryData: largeArray
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<ComplexCollectionModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.BinaryData.ShouldNotBeNull();
        result.BinaryData.Length.ShouldBe(largeArray.Length);
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_ExtremelyLongString_ReturnsEqualInstance()
    {
        // Arrange
        var builder = new StringBuilder(1024 * 1024);
        for (int i = 0; i < 10000; i++)
        {
            builder.Append("LongStringPaddingData1234567890-");
        }
        var massiveString = builder.ToString();

        var model = new SimpleValueModel(
            Id: 999,
            Name: massiveString,
            IsActive: true,
            Score: 100.0,
            CreatedAt: DateTime.UtcNow,
            Uid: Guid.NewGuid()
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<SimpleValueModel>(bytes);

        // Assert
        result.Name.ShouldBe(massiveString);
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_TelemetryPayload_EmptyLists_ReturnsEqualInstance()
    {
        // Arrange
        var model = new TestTelemetryPayloadDto
        {
            NodeId = Guid.NewGuid(),
            Timestamp = DateTimeOffset.UtcNow,
            // Explicitly verify the default initialization using Array.Empty<T> mapped via the `init` property
            Metrics = Array.Empty<TestMetricSnapshotDto>() 
        };

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<TestTelemetryPayloadDto>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
        result.Metrics.ShouldNotBeNull();
        result.Metrics.Count.ShouldBe(0);
    }

    [Fact]
    public void SerializeDeserialize_TelemetryPayload_PopulatedData_ReturnsEqualInstance()
    {
        // Arrange
        var model = new TestTelemetryPayloadDto
        {
            NodeId = Guid.NewGuid(),
            Timestamp = new DateTimeOffset(2025, 1, 1, 12, 0, 0, TimeSpan.FromHours(2)),
            Metrics = new List<TestMetricSnapshotDto>
            {
                new TestMetricSnapshotDto
                {
                    Name = "process.cpu.utilization",
                    Type = "gauge",
                    Value = 42,
                    Tags = new List<TestMetricTagDto>
                    {
                        new TestMetricTagDto("host", "node-1"),
                        new TestMetricTagDto("environment", "production")
                    }
                },
                new TestMetricSnapshotDto
                {
                    Name = "http.server.requests",
                    Type = "counter",
                    Value = 9001,
                    Tags = Array.Empty<TestMetricTagDto>() // Verify mixed state collections
                }
            }
        };

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<TestTelemetryPayloadDto>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
        result.Metrics.ShouldNotBeNull();
        result.Metrics.Count.ShouldBe(2);
        
        result.Metrics[0].Tags.ShouldNotBeNull();
        result.Metrics[0].Tags.Count.ShouldBe(2);
        
        result.Metrics[1].Tags.ShouldNotBeNull();
        result.Metrics[1].Tags.Count.ShouldBe(0);
    }
    
    [Fact]
    public void SerializeDeserialize_NullableTypes_WithValues_ReturnsEqualInstance()
    {
        // Arrange
        var structModel = new SimpleValueModel(1, "Inner", true, 5.5, DateTime.UtcNow, Guid.NewGuid());
        var model = new NullableTypesModel(
            OptionalInt: 42,
            OptionalDouble: 3.14159,
            OptionalDate: new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            OptionalGuid: Guid.NewGuid(),
            OptionalString: "NotNullString",
            OptionalStruct: structModel
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<NullableTypesModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_NullableTypes_WithNulls_ReturnsEqualInstance()
    {
        // Arrange
        var model = new NullableTypesModel(
            OptionalInt: null,
            OptionalDouble: null,
            OptionalDate: null,
            OptionalGuid: null,
            OptionalString: null,
            OptionalStruct: null
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<NullableTypesModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
        result.OptionalInt.ShouldBeNull();
        result.OptionalString.ShouldBeNull();
    }

    [Fact]
    public void SerializeDeserialize_AdvancedPrimitives_ReturnsEqualInstance()
    {
        // Arrange
        var model = new AdvancedPrimitivesModel(
            Duration: TimeSpan.FromDays(1.5).Add(TimeSpan.FromSeconds(30)),
            CurrencyValue: 123456789.99m,
            Endpoint: new Uri("https://p2p.ama-enterprise.local/metrics?active=true")
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<AdvancedPrimitivesModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_FlagsEnum_ReturnsEqualInstance()
    {
        // Arrange
        var model = new FlagsEnumModel(TestPermissions.Read | TestPermissions.Execute);

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<FlagsEnumModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_JaggedArrays_ReturnsEqualInstance()
    {
        // Arrange
        var model = new JaggedArrayModel(
            DataChunks: new List<byte[]>
            {
                new byte[] { 1, 2, 3 },
                new byte[] { 4, 5, 6, 7 },
                Array.Empty<byte>()
            },
            Matrix: new List<IList<int>>
            {
                new List<int> { 10, 20 },
                new List<int> { 30, 40, 50 },
                new List<int>()
            }
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<JaggedArrayModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    [Fact]
    public void SerializeDeserialize_NonStringKeyDictionary_ReturnsEqualInstance()
    {
        // Arrange
        var model = new NonStringKeyDictionaryModel(
            IntKeys: new Dictionary<int, string>
            {
                { 1, "First" },
                { 999, "Last" }
            },
            GuidKeys: new Dictionary<Guid, SimpleValueModel>
            {
                { Guid.NewGuid(), new SimpleValueModel(1, "A", true, 1.1, DateTime.UtcNow, Guid.NewGuid()) },
                { Guid.NewGuid(), new SimpleValueModel(2, "B", false, 0.0, DateTime.UtcNow, Guid.NewGuid()) }
            }
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<NonStringKeyDictionaryModel>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }
    
    [Fact]
    public void SerializeDeserialize_NullValues_HandledAvoidingErrors()
    {
        // Act
        var bytes = crdtSerializer.SerializeToBytes<DeepNestedModel>(null!);
        var result = crdtSerializer.DeserializeFromBytes<DeepNestedModel>(bytes);
        
        // Assert
        result.ShouldBeNull();
    }

    [Fact]
    public void SerializeDeserialize_StandaloneCustomEnum_ReturnsEqualInstance()
    {
        // Arrange
        var enumValue = SimulatedGossipMessageType.PushPull;

        // Act
        var bytes = crdtSerializer.SerializeToBytes(enumValue);
        var result = crdtSerializer.DeserializeFromBytes<SimulatedGossipMessageType>(bytes);

        // Assert
        result.ShouldBe(enumValue);
    }

    [Fact]
    public void SerializeDeserialize_CustomEnumWithinModel_ReturnsEqualInstance()
    {
        // Arrange
        var model = new SimulatedGossipMessage(
            MessageId: Guid.NewGuid(),
            MessageType: SimulatedGossipMessageType.Pull
        );

        // Act
        var bytes = crdtSerializer.SerializeToBytes(model);
        var result = crdtSerializer.DeserializeFromBytes<SimulatedGossipMessage>(bytes);

        // Assert
        result.ShouldNotBeNull();
        result.ShouldBe(model);
    }

    private static ICrdtSerializer InitializeSerializer()
    {
        var services = new ServiceCollection();

        // The Roslyn Source Generator intercepts STJ [JsonSerializable] models during project build time
        // and maps the explicit resolver matching the consumer Application/Assembly namespace boundaries.
        // For 'Ama.Enterprise.CRDT.MessagePack.UnitTests' the generated output evaluates as:
        services.AddCrdtMessagePack(Ama_Enterprise_CRDT_MessagePack_IntegrationTests_MessagePackResolver.Instance);
        
        var provider = services.BuildServiceProvider();
        return provider.GetRequiredService<ICrdtSerializer>();
    }
}