using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

namespace Ama.Enterprise.P2p.UnitTests.Services;

public sealed class MessageDispatcherTests
{
    private readonly Mock<ILogger<MessageDispatcher>> loggerMock;

    public MessageDispatcherTests()
    {
        this.loggerMock = new Mock<ILogger<MessageDispatcher>>();
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenHandlersIsNull()
    {
        // Act
        var exception = Record.Exception(() => new MessageDispatcher(null!, this.loggerMock.Object));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<ArgumentNullException>();
        ((ArgumentNullException)exception).ParamName.ShouldBe("handlers");
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenLoggerIsNull()
    {
        // Arrange
        var handlers = Enumerable.Empty<IMessageHandler>();

        // Act
        var exception = Record.Exception(() => new MessageDispatcher(handlers, null!));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<ArgumentNullException>();
        ((ArgumentNullException)exception).ParamName.ShouldBe("logger");
    }

    [Fact]
    public async Task DispatchAsync_CallsHandleAsyncOnAllRegisteredHandlers()
    {
        // Arrange
        var handler1Mock = new Mock<IMessageHandler>();
        var handler2Mock = new Mock<IMessageHandler>();
        
        var handlers = new[] { handler1Mock.Object, handler2Mock.Object };
        var dispatcher = new MessageDispatcher(handlers, this.loggerMock.Object);

        var message = CreateSampleMessage();
        var cancellationToken = CancellationToken.None;

        // Act
        await dispatcher.DispatchAsync(message, cancellationToken);

        // Assert
        handler1Mock.Verify(h => h.HandleAsync(message, cancellationToken), Times.Once);
        handler2Mock.Verify(h => h.HandleAsync(message, cancellationToken), Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_ContinuesToNextHandler_WhenOneHandlerThrows()
    {
        // Arrange
        var failingHandlerMock = new Mock<IMessageHandler>();
        failingHandlerMock
            .Setup(h => h.HandleAsync(It.IsAny<GossipMessage>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Handler failed"));

        var successfulHandlerMock = new Mock<IMessageHandler>();
        
        var handlers = new[] { failingHandlerMock.Object, successfulHandlerMock.Object };
        var dispatcher = new MessageDispatcher(handlers, this.loggerMock.Object);

        var message = CreateSampleMessage();
        var cancellationToken = CancellationToken.None;

        // Act
        var exception = await Record.ExceptionAsync(() => dispatcher.DispatchAsync(message, cancellationToken));

        // Assert
        exception.ShouldBeNull(); // Dispatcher should swallow domain handler exceptions
        failingHandlerMock.Verify(h => h.HandleAsync(message, cancellationToken), Times.Once);
        successfulHandlerMock.Verify(h => h.HandleAsync(message, cancellationToken), Times.Once);
    }

    [Fact]
    public async Task DispatchAsync_CompletesSuccessfully_WhenNoHandlersRegistered()
    {
        // Arrange
        var handlers = Enumerable.Empty<IMessageHandler>();
        var dispatcher = new MessageDispatcher(handlers, this.loggerMock.Object);

        var message = CreateSampleMessage();
        var cancellationToken = CancellationToken.None;

        // Act
        var exception = await Record.ExceptionAsync(() => dispatcher.DispatchAsync(message, cancellationToken));

        // Assert
        exception.ShouldBeNull();
    }

    [Fact]
    public async Task DispatchAsync_ThrowsOperationCanceledException_WhenCanceled()
    {
        // Arrange
        var handlerMock = new Mock<IMessageHandler>();
        var handlers = new[] { handlerMock.Object };
        var dispatcher = new MessageDispatcher(handlers, this.loggerMock.Object);

        var message = CreateSampleMessage();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancel to trigger immediate throw

        // Act
        var exception = await Record.ExceptionAsync(() => dispatcher.DispatchAsync(message, cts.Token));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<OperationCanceledException>();
        handlerMock.Verify(h => h.HandleAsync(It.IsAny<GossipMessage>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    private static GossipMessage CreateSampleMessage()
    {
        return new GossipMessage(
            Guid.NewGuid(),
            new PeerId(Guid.NewGuid()),
            5,
            new ReadOnlyMemory<byte>(new byte[] { 1, 2, 3 })
        );
    }
}