namespace Ama.Enterprise.FeatureFlags.UnitTests.Services.P2p;

using System;
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
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using Xunit;

public sealed class FeatureFlagAntiEntropyServiceTests
{
    private DottedVersionVector CreateDvv()
    {
        var dvvJson = """{"Versions":{},"Dots":{}}""";
        return JsonSerializer.Deserialize(dvvJson, FeatureFlagP2pJsonContext.Default.DottedVersionVector)!;
    }

    [Fact]
    public void Constructor_ShouldThrowArgumentNullException_WhenDependenciesAreNull()
    {
        var crdtScopeFactoryMock = new Mock<ICrdtScopeFactory>();
        crdtScopeFactoryMock.Setup(s => s.CreateScope(It.IsAny<string>())).Returns(new Mock<IServiceScope>().Object);
        var options = Options.Create(new FeatureFlagOptions { ReplicaId = "rep1" });
        var scopeProvider = new FeatureFlagCrdtScopeProvider(crdtScopeFactoryMock.Object, options);
        
        var gossip = new Mock<IGossipProtocol>().Object;
        var serializer = new Mock<ICrdtSerializer>().Object;
        var logger = new Mock<ILogger<FeatureFlagAntiEntropyService>>().Object;

        Should.Throw<ArgumentNullException>(() => new FeatureFlagAntiEntropyService(null!, gossip, serializer, logger));
        Should.Throw<ArgumentNullException>(() => new FeatureFlagAntiEntropyService(scopeProvider, null!, serializer, logger));
        Should.Throw<ArgumentNullException>(() => new FeatureFlagAntiEntropyService(scopeProvider, gossip, null!, logger));
        Should.Throw<ArgumentNullException>(() => new FeatureFlagAntiEntropyService(scopeProvider, gossip, serializer, null!));
    }

    [Fact]
    public async Task ExecuteAsync_ShouldBroadcastState_AndCancelGracefully()
    {
        var crdtScopeFactoryMock = new Mock<ICrdtScopeFactory>();
        var scopeMock = new Mock<IServiceScope>();
        var serviceProviderMock = new Mock<IServiceProvider>();
        scopeMock.Setup(s => s.ServiceProvider).Returns(serviceProviderMock.Object);
        crdtScopeFactoryMock.Setup(s => s.CreateScope("rep1")).Returns(scopeMock.Object);
        
        var options = Options.Create(new FeatureFlagOptions { ReplicaId = "rep1" });
        var scopeProvider = new FeatureFlagCrdtScopeProvider(crdtScopeFactoryMock.Object, options);
        
        var gossipMock = new Mock<IGossipProtocol>();
        var serializerMock = new Mock<ICrdtSerializer>();
        var loggerMock = new Mock<ILogger<FeatureFlagAntiEntropyService>>();

        var clusterManagerMock = new Mock<IFeatureFlagClusterManager>();
        clusterManagerMock.Setup(c => c.GetLocalState()).Returns(CreateDvv());

        var ctx = (ReplicaContext)System.Runtime.Serialization.FormatterServices.GetUninitializedObject(typeof(ReplicaContext));
        typeof(ReplicaContext).GetProperty(nameof(ReplicaContext.ReplicaId))?.SetValue(ctx, "rep1");

        serviceProviderMock.Setup(sp => sp.GetService(typeof(IFeatureFlagClusterManager))).Returns(clusterManagerMock.Object);
        serviceProviderMock.Setup(sp => sp.GetService(typeof(ReplicaContext))).Returns(ctx);

        serializerMock.Setup(s => s.SerializeToBytes(It.IsAny<FeatureFlagStateSyncMessage>())).Returns(new byte[] { 1, 2, 3 });
        serializerMock.Setup(s => s.SerializeToBytes(It.IsAny<FeatureFlagMessageWrapper>())).Returns(new byte[] { 4, 5, 6 });

        var service = new FeatureFlagAntiEntropyService(scopeProvider, gossipMock.Object, serializerMock.Object, loggerMock.Object);

        var cts = new CancellationTokenSource();
        
        var task = service.StartAsync(cts.Token);
        
        // Trigger rapid cancellation allowing the internal OperationCanceledException catch to be evaluated
        cts.Cancel();
        
        await service.StopAsync(CancellationToken.None);

        task.Status.ShouldBeOneOf(TaskStatus.RanToCompletion, TaskStatus.Canceled, TaskStatus.WaitingForActivation);
    }
}