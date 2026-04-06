namespace Ama.Enterprise.FeatureFlags.UnitTests.Services.P2p;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Models.P2p;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.FeatureFlags.Services.P2p;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using Xunit;

/// <summary>
/// A manual fake for ICrdtSerializer to bypass a known bug in Castle.DynamicProxy / Moq 
/// where dynamically proxying generic methods that return value types (structs) 
/// emits invalid IL and throws an "InvalidProgramException" at runtime on modern .NET.
/// </summary>
internal sealed class FakeCrdtSerializer : ICrdtSerializer
{
    public bool DeserializeCalled { get; private set; }
    public bool ThrowOnDeserialize { get; set; }

    public object? NextWrapperResult { get; set; }
    public object? NextStateSyncResult { get; set; }
    public object? NextOperationsResult { get; set; }

    public byte[] NextSerializeOpsResult { get; set; } = Array.Empty<byte>();
    public byte[] NextSerializeWrapperResult { get; set; } = Array.Empty<byte>();

    public T? DeserializeFromBytes<T>(ReadOnlySpan<byte> bytes)
    {
        DeserializeCalled = true;
        
        if (ThrowOnDeserialize)
        {
            throw new Exception("Invalid format");
        }

        if (typeof(T) == typeof(FeatureFlagMessageWrapper) && NextWrapperResult != null)
        {
            return (T)NextWrapperResult;
        }
            
        if (typeof(T) == typeof(FeatureFlagStateSyncMessage) && NextStateSyncResult != null)
        {
            return (T)NextStateSyncResult;
        }
            
        if (typeof(T) == typeof(FeatureFlagOperationsMessage) && NextOperationsResult != null)
        {
            return (T)NextOperationsResult;
        }

        return default;
    }

    public byte[] SerializeToBytes<T>(T value)
    {
        if (typeof(T) == typeof(FeatureFlagOperationsMessage))
        {
            return NextSerializeOpsResult;
        }
            
        if (typeof(T) == typeof(FeatureFlagMessageWrapper))
        {
            return NextSerializeWrapperResult;
        }
            
        return Array.Empty<byte>();
    }

    // --- Unused interface members required for compilation ---
    
    public Task SerializeAsync<T>(Stream stream, T value, CancellationToken cancellationToken = default) 
        => throw new NotImplementedException();

    public Task SerializeAsync(Stream stream, object value, Type inputType, CancellationToken cancellationToken = default) 
        => throw new NotImplementedException();

    public Task<T?> DeserializeAsync<T>(Stream stream, CancellationToken cancellationToken = default) 
        => throw new NotImplementedException();

    public byte[] SerializeToBytes(object value, Type inputType) 
        => throw new NotImplementedException();

    public object? DeserializeFromBytes(ReadOnlySpan<byte> bytes, Type returnType) 
        => throw new NotImplementedException();

    public T? Clone<T>(T original) 
        => throw new NotImplementedException();
}

public sealed class FeatureFlagGossipHandlerTests
{
    private DottedVersionVector CreateDvv()
    {
        var dvvJson = """{"Versions":{},"versions":{},"Dots":{},"dots":{}}""";
        return JsonSerializer.Deserialize(dvvJson, FeatureFlagP2pJsonContext.Default.DottedVersionVector)!;
    }

    private CrdtOperation CreateOperation()
    {
        var id = Guid.NewGuid();
        var json = $$"""{"Id":"{{id}}","id":"{{id}}","ReplicaId":"rep1","replicaId":"rep1","GlobalClock":1,"globalClock":1}""";
        return JsonSerializer.Deserialize(json, FeatureFlagP2pJsonContext.Default.CrdtOperation)!;
    }

    private GossipMessage CreateGossipMessage(byte[] payload)
    {
        var base64 = Convert.ToBase64String(payload);
        var json = $$"""{"MessageId":"{{Guid.NewGuid()}}","messageId":"{{Guid.NewGuid()}}","SenderNodeId":"node1","senderNodeId":"node1","Payload":"{{base64}}","payload":"{{base64}}"}""";
        return JsonSerializer.Deserialize(json, Ama.Enterprise.P2p.Models.Gossip.P2pJsonSerializerContext.Default.GossipMessage)!;
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenDependenciesAreNull()
    {
        var scopeFactory = new Mock<ICrdtScopeFactory>().Object;
        var options = Options.Create(new FeatureFlagOptions { ReplicaId = "rep1" });
        var serializer = new FakeCrdtSerializer();
        var logger = new Mock<ILogger<FeatureFlagGossipHandler>>().Object;

        Should.Throw<ArgumentNullException>(() => new FeatureFlagGossipHandler(null!, options, serializer, logger));
        Should.Throw<ArgumentNullException>(() => new FeatureFlagGossipHandler(scopeFactory, null!, serializer, logger));
        Should.Throw<ArgumentNullException>(() => new FeatureFlagGossipHandler(scopeFactory, options, null!, logger));
        Should.Throw<ArgumentNullException>(() => new FeatureFlagGossipHandler(scopeFactory, options, serializer, null!));
    }

    [Fact]
    public async Task HandleAsync_ShouldIgnoreEmptyPayload()
    {
        var scopeFactoryMock = new Mock<ICrdtScopeFactory>();
        var options = Options.Create(new FeatureFlagOptions { ReplicaId = "rep1" });
        var serializer = new FakeCrdtSerializer();
        var loggerMock = new Mock<ILogger<FeatureFlagGossipHandler>>();

        var handler = new FeatureFlagGossipHandler(scopeFactoryMock.Object, options, serializer, loggerMock.Object);

        var message = CreateGossipMessage(Array.Empty<byte>());
        
        await handler.HandleAsync(message, CancellationToken.None);

        serializer.DeserializeCalled.ShouldBeFalse();
    }

    [Fact]
    public async Task HandleAsync_ShouldIgnoreInvalidPayload()
    {
        var scopeFactoryMock = new Mock<ICrdtScopeFactory>();
        var options = Options.Create(new FeatureFlagOptions { ReplicaId = "rep1" });
        var serializer = new FakeCrdtSerializer { ThrowOnDeserialize = true };
        var loggerMock = new Mock<ILogger<FeatureFlagGossipHandler>>();

        var handler = new FeatureFlagGossipHandler(scopeFactoryMock.Object, options, serializer, loggerMock.Object);

        var message = CreateGossipMessage(new byte[] { 1, 2, 3 });
        
        await handler.HandleAsync(message, CancellationToken.None);

        scopeFactoryMock.Verify(s => s.CreateScope(It.IsAny<string>()), Times.Never);
    }

    [Fact]
    public async Task HandleAsync_ShouldProcessFeatureFlagSync_AndBroadcastOps()
    {
        var scopeFactoryMock = new Mock<ICrdtScopeFactory>();
        var options = Options.Create(new FeatureFlagOptions { ReplicaId = "rep1" });
        var gossipMock = new Mock<IGossipProtocol>();
        var loggerMock = new Mock<ILogger<FeatureFlagGossipHandler>>();

        var serializer = new FakeCrdtSerializer
        {
            NextWrapperResult = new FeatureFlagMessageWrapper("FeatureFlagSync", new byte[] { 1 }),
            NextStateSyncResult = new FeatureFlagStateSyncMessage("rep2", CreateDvv()),
            NextSerializeOpsResult = new byte[] { 2 },
            NextSerializeWrapperResult = new byte[] { 3 }
        };

        var scopeMock = new Mock<IServiceScope>();
        var serviceProviderMock = new Mock<IServiceProvider>();

        scopeFactoryMock.Setup(s => s.CreateScope("rep1")).Returns(scopeMock.Object);
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);

        var ctx = (ReplicaContext)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(ReplicaContext));
        typeof(ReplicaContext).GetProperty(nameof(ReplicaContext.ReplicaId))?.SetValue(ctx, "rep1");
        serviceProviderMock.Setup(sp => sp.GetService(typeof(ReplicaContext))).Returns(ctx);
        
        // Setup resolution of IGossipProtocol from DI scope inside the handler
        serviceProviderMock.Setup(sp => sp.GetService(typeof(IGossipProtocol))).Returns(gossipMock.Object);

        var clusterManagerMock = new Mock<IFeatureFlagClusterManager>();
        var ops = new List<CrdtOperation> { CreateOperation() };
        
        clusterManagerMock.Setup(c => c.GetMissingOperationsAsync("rep2", It.IsAny<DottedVersionVector>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(ops);
            
        serviceProviderMock.Setup(sp => sp.GetService(typeof(IFeatureFlagClusterManager))).Returns(clusterManagerMock.Object);

        var handler = new FeatureFlagGossipHandler(scopeFactoryMock.Object, options, serializer, loggerMock.Object);

        var message = CreateGossipMessage(new byte[] { 1, 2, 3 });
        
        await handler.HandleAsync(message, CancellationToken.None);

        gossipMock.Verify(g => g.BroadcastAsync(It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task HandleAsync_ShouldProcessFeatureFlagOps_AndApplyThem()
    {
        var scopeFactoryMock = new Mock<ICrdtScopeFactory>();
        var options = Options.Create(new FeatureFlagOptions { ReplicaId = "rep1" });
        var loggerMock = new Mock<ILogger<FeatureFlagGossipHandler>>();

        var serializer = new FakeCrdtSerializer
        {
            NextWrapperResult = new FeatureFlagMessageWrapper("FeatureFlagOps", new byte[] { 1 }),
            NextOperationsResult = new FeatureFlagOperationsMessage("rep2", new[] { CreateOperation() })
        };

        var scopeMock = new Mock<IServiceScope>();
        var serviceProviderMock = new Mock<IServiceProvider>();

        scopeFactoryMock.Setup(s => s.CreateScope("rep1")).Returns(scopeMock.Object);
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);

        var clusterManagerMock = new Mock<IFeatureFlagClusterManager>();
        serviceProviderMock.Setup(sp => sp.GetService(typeof(IFeatureFlagClusterManager))).Returns(clusterManagerMock.Object);

        var handler = new FeatureFlagGossipHandler(scopeFactoryMock.Object, options, serializer, loggerMock.Object);

        var message = CreateGossipMessage(new byte[] { 1, 2, 3 });
        
        await handler.HandleAsync(message, CancellationToken.None);

        clusterManagerMock.Verify(c => c.ApplyOperationsAsync(It.IsAny<IReadOnlyList<CrdtOperation>>(), It.IsAny<CancellationToken>()), Times.Once);
    }
}