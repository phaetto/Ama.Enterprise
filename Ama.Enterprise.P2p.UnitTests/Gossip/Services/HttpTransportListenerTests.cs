namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Transports;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using Xunit;

public sealed class HttpTransportListenerTests
{
    private const string TestMeshId = "TestMesh";
    private readonly Mock<ICrdtSerializer> serializerMock;
    private readonly Mock<ILogger<HttpTransportListener>> loggerMock;
    private readonly Mock<IOptionsMonitor<GossipOptions>> optionsMock;

    public HttpTransportListenerTests()
    {
        serializerMock = new Mock<ICrdtSerializer>();
        loggerMock = new Mock<ILogger<HttpTransportListener>>();
        optionsMock = new Mock<IOptionsMonitor<GossipOptions>>();
        
        // Use a dynamic port to avoid conflicting with actual services if this runs fully
        optionsMock.Setup(o => o.Get(TestMeshId)).Returns(new GossipOptions { ListenPort = 0 }); 
    }

    [Fact]
    public void Constructor_ShouldThrow_WhenOptionsIsNull()
    {
        // Act & Assert
        Should.Throw<ArgumentNullException>(() => new HttpTransportListener(TestMeshId, null!, serializerMock.Object, loggerMock.Object));
    }

    [Fact(Skip = "Integration test binding to real OS ports")]
    public async Task StartListeningAsync_ShouldStartWithoutExceptions()
    {
        // Arrange
        // We use port 8080 explicitly for integration skip
        var localOptionsMock = new Mock<IOptionsMonitor<GossipOptions>>();
        localOptionsMock.Setup(o => o.Get(TestMeshId)).Returns(new GossipOptions { ListenPort = 8080 });
        
        using var listener = new HttpTransportListener(TestMeshId, localOptionsMock.Object, serializerMock.Object, loggerMock.Object);

        // Act
        await listener.StartListeningAsync(_ => Task.CompletedTask, CancellationToken.None);

        // Assert
        // If it reaches here without HttpListenerException, it started correctly.
        await listener.StopListeningAsync(CancellationToken.None);
    }
}