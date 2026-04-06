using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using Xunit;

namespace Ama.Enterprise.P2p.UnitTests.Services;

public sealed class HttpTransportListenerTests
{
    private readonly Mock<IGossipSerializer> serializerMock;
    private readonly Mock<ILogger<HttpTransportListener>> loggerMock;
    private readonly IOptions<GossipOptions> options;

    public HttpTransportListenerTests()
    {
        this.serializerMock = new Mock<IGossipSerializer>();
        this.loggerMock = new Mock<ILogger<HttpTransportListener>>();
        // Use a dynamic port to avoid conflicting with actual services if this runs fully
        this.options = Options.Create(new GossipOptions { ListenPort = 0 }); 
    }

    [Fact]
    public void Constructor_ShouldThrow_WhenOptionsIsNull()
    {
        // Act & Assert
        Should.Throw<ArgumentNullException>(() => new HttpTransportListener(null!, this.serializerMock.Object, this.loggerMock.Object));
    }

    [Fact(Skip = "Integration test binding to real OS ports")]
    public async Task StartListeningAsync_ShouldStartWithoutExceptions()
    {
        // Arrange
        // We use port 8080 explicitly for integration skip
        var localOptions = Options.Create(new GossipOptions { ListenPort = 8080 });
        using var listener = new HttpTransportListener(localOptions, this.serializerMock.Object, this.loggerMock.Object);

        // Act
        await listener.StartListeningAsync(_ => Task.CompletedTask, CancellationToken.None);

        // Assert
        // If it reaches here without HttpListenerException, it started correctly.
        await listener.StopListeningAsync(CancellationToken.None);
    }
}