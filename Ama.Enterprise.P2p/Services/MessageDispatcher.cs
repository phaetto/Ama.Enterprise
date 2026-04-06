using Ama.Enterprise.P2p.Models;
using Microsoft.Extensions.Logging;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Implements the message dispatcher, routing incoming gossip messages to all registered domain handlers.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="MessageDispatcher"/> class.
/// </remarks>
/// <param name="handlers">The collection of registered message handlers.</param>
/// <param name="logger">The logger instance.</param>
public sealed class MessageDispatcher(
    IEnumerable<IMessageHandler> handlers,
    ILogger<MessageDispatcher> logger) : IMessageDispatcher
{
    private readonly IEnumerable<IMessageHandler> handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
    private readonly ILogger<MessageDispatcher> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task DispatchAsync(GossipMessage message, CancellationToken cancellationToken)
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
                this.logger.LogInformation("Message dispatching was canceled for message {MessageId}.", message.MessageId);
                throw;
            }
            catch (Exception ex)
            {
                this.logger.LogError(ex, "Error occurred in handler {HandlerType} while processing message {MessageId}.", handler.GetType().Name, message.MessageId);
            }
        }
    }
}