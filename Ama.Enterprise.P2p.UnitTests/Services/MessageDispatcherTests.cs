namespace Ama.Enterprise.P2p.UnitTests.Services;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

public sealed class ApplicationPayloadDispatcherTests
{
    private const string TestMeshId = "TestMesh";
    private readonly Mock<ILogger<ApplicationPayloadDispatcher>> loggerMock;

    public ApplicationPayloadDispatcherTests()
    {
        loggerMock = new Mock<ILogger<ApplicationPayloadDispatcher>>();
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenHandlersIsNull()
    {
        // Act
        var exception = Record.Exception(() => new ApplicationPayloadDispatcher(TestMeshId, null!, loggerMock.Object));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<ArgumentNullException>();
        ((ArgumentNullException)exception).ParamName.ShouldBe("handlers");
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenLoggerIsNull()
    {
        // Arrange
        var handlers = Enumerable.Empty<IApplicationPayloadHandler>();

        // Act
        var exception = Record.Exception(() => new ApplicationPayloadDispatcher(TestMeshId, handlers, null!));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<ArgumentNullException>();
        ((ArgumentNullException)exception).ParamName.ShouldBe("logger");
    }

    [Fact]
    public async Task DispatchAsync_CallsHandleAsyncOnAllRegisteredHandlers()
    {
        // Arrange
        var handler1Mock = new Mock<IApplicationPayloadHandler>();
        var handler2Mock = new Mock<IApplicationPayloadHandler>();
        
        var handlers = new[] { handler1Mock.Object, handler2Mock.Object };
        var dispatcher = new ApplicationPayloadDispatcher(TestMeshId, handlers, loggerMock.Object);

        var senderId = new PeerId(Guid.NewGuid());
        var payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        var cancellationToken = CancellationToken.None;

        // Act
        await dispatcher.DispatchAsync(TestMeshId, senderId, payload, cancellationToken);

        // Assert
        handler1Mock.Verify(h => h.HandlePayloadAsync(TestMeshId, senderId, payload, cancellationToken), Times.Once);
        handler2Mock.Verify(h => h.HandlePayloadAsync(TestMeshId, senderId, payload, cancellationToken), Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_ContinuesToNextHandler_WhenOneHandlerThrows()
    {
        // Arrange
        var failingHandlerMock = new Mock<IApplicationPayloadHandler>();
        failingHandlerMock
            .Setup(h => h.HandlePayloadAsync(It.IsAny<string>(), It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Handler failed"));

        var successfulHandlerMock = new Mock<IApplicationPayloadHandler>();
        
        var handlers = new[] { failingHandlerMock.Object, successfulHandlerMock.Object };
        var dispatcher = new ApplicationPayloadDispatcher(TestMeshId, handlers, loggerMock.Object);

        var senderId = new PeerId(Guid.NewGuid());
        var payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        var cancellationToken = CancellationToken.None;

        // Act
        var exception = await Record.ExceptionAsync(() => dispatcher.DispatchAsync(TestMeshId, senderId, payload, cancellationToken));

        // Assert
        exception.ShouldBeNull(); // Dispatcher should swallow domain handler exceptions natively gracefully cleanly
        failingHandlerMock.Verify(h => h.HandlePayloadAsync(TestMeshId, senderId, payload, cancellationToken), Times.Once);
        successfulHandlerMock.Verify(h => h.HandlePayloadAsync(TestMeshId, senderId, payload, cancellationToken), Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_CompletesSuccessfully_WhenNoHandlersRegistered()
    {
        // Arrange
        var handlers = Enumerable.Empty<IApplicationPayloadHandler>();
        var dispatcher = new ApplicationPayloadDispatcher(TestMeshId, handlers, loggerMock.Object);

        var senderId = new PeerId(Guid.NewGuid());
        var payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        var cancellationToken = CancellationToken.None;

        // Act
        var exception = await Record.ExceptionAsync(() => dispatcher.DispatchAsync(TestMeshId, senderId, payload, cancellationToken));

        // Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task DispatchAsync_ThrowsOperationCanceledException_WhenCanceled()
    {
        // Arrange
        var handlerMock = new Mock<IApplicationPayloadHandler>();
        var handlers = new[] { handlerMock.Object };
        var dispatcher = new ApplicationPayloadDispatcher(TestMeshId, handlers, loggerMock.Object);

        var senderId = new PeerId(Guid.NewGuid());
        var payload = new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 });
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancel to trigger immediate throw

        // Act
        var exception = await Record.ExceptionAsync(() => dispatcher.DispatchAsync(TestMeshId, senderId, payload, cts.Token));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<OperationCanceledException>();
        handlerMock.Verify(h => h.HandlePayloadAsync(It.IsAny<string>(), It.IsAny<PeerId>(), It.IsAny<ReadOnlyMemory<byte>>(), It.IsAny<CancellationToken>()), Times.Never);
    }
}