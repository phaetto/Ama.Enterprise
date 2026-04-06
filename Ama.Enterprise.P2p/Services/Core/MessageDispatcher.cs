namespace Ama.Enterprise.P2p.Services.Core;

using Microsoft.Extensions.Logging;

/// <summary>
/// Implements the generic message dispatcher, routing incoming protocol messages to all registered domain handlers.
/// </summary>
/// <typeparam name="TMessage">The type of the message being dispatched.</typeparam>
/// <remarks>
/// Initializes a new instance of the <see cref="MessageDispatcher{TMessage}"/> class.
/// </remarks>
/// <param name="handlers">The collection of registered message handlers.</param>
/// <param name="logger">The logger instance.</param>
public sealed class MessageDispatcher<TMessage>(
    IEnumerable<IMessageHandler<TMessage>> handlers,
    ILogger<MessageDispatcher<TMessage>> logger) : IMessageDispatcher<TMessage>
{
    private readonly IEnumerable<IMessageHandler<TMessage>> handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
    private readonly ILogger<MessageDispatcher<TMessage>> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task DispatchAsync(TMessage message, CancellationToken cancellationToken)
    {
        foreach (var handler in this.handlers)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                await handler.HandleAsync(message, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                this.logger.LogInformation("Message dispatching was canceled.");
                throw;
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error occurred in handler {HandlerType} while processing message.", handler.GetType().Name);
            }
        }
    }
}