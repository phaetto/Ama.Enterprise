namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;

public sealed class MessageDispatcherTests
{
    private readonly Mock<ILogger<MessageDispatcher<GossipMessage>>> loggerMock;

    public MessageDispatcherTests()
    {
        this.loggerMock = new Mock<ILogger<MessageDispatcher<GossipMessage>>>();
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenHandlersIsNull()
    {
        // Act
        var exception = Record.Exception(() => new MessageDispatcher<GossipMessage>(null!, this.loggerMock.Object));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<ArgumentNullException>();
        ((ArgumentNullException)exception).ParamName.ShouldBe("handlers");
    }

    [Fact]
    public void Constructor_ThrowsArgumentNullException_WhenLoggerIsNull()
    {
        // Arrange
        var handlers = Enumerable.Empty<IMessageHandler<GossipMessage>>();

        // Act
        var exception = Record.Exception(() => new MessageDispatcher<GossipMessage>(handlers, null!));

        // Assert
        exception.ShouldNotBeNull();
        exception.ShouldBeOfType<ArgumentNullException>();
        ((ArgumentNullException)exception).ParamName.ShouldBe("logger");
    }

    [Fact]
    public async Task DispatchAsync_CallsHandleAsyncOnAllRegisteredHandlers()
    {
        // Arrange
        var handler1Mock = new Mock<IMessageHandler<GossipMessage>>();
        var handler2Mock = new Mock<IMessageHandler<GossipMessage>>();
        
        var handlers = new[] { handler1Mock.Object, handler2Mock.Object };
        var dispatcher = new MessageDispatcher<GossipMessage>(handlers, this.loggerMock.Object);

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
        var failingHandlerMock = new Mock<IMessageHandler<GossipMessage>>();
        failingHandlerMock
            .Setup(h => h.HandleAsync(It.IsAny<GossipMessage>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new InvalidOperationException("Handler failed"));

        var successfulHandlerMock = new Mock<IMessageHandler<GossipMessage>>();
        
        var handlers = new[] { failingHandlerMock.Object, successfulHandlerMock.Object };
        var dispatcher = new MessageDispatcher<GossipMessage>(handlers, this.loggerMock.Object);

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
        var handlers = Enumerable.Empty<IMessageHandler<GossipMessage>>();
        var dispatcher = new MessageDispatcher<GossipMessage>(handlers, this.loggerMock.Object);

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
        var handlerMock = new Mock<IMessageHandler<GossipMessage>>();
        var handlers = new[] { handlerMock.Object };
        var dispatcher = new MessageDispatcher<GossipMessage>(handlers, this.loggerMock.Object);

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