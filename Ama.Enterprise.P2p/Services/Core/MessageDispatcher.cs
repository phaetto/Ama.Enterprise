namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements the generic message dispatcher, routing incoming protocol messages to all registered domain handlers for the mesh.
/// </summary>
/// <typeparam name="TMessage">The type of the message being dispatched.</typeparam>
/// <remarks>
/// Initializes a new instance of the <see cref="MessageDispatcher{TMessage}"/> class.
/// </remarks>
public sealed class MessageDispatcher<TMessage>(
    string meshId,
    IEnumerable<IMessageHandler<TMessage>> handlers,
    ILogger<MessageDispatcher<TMessage>> logger) : IMessageDispatcher<TMessage>
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IEnumerable<IMessageHandler<TMessage>> handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
    private readonly ILogger<MessageDispatcher<TMessage>> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task DispatchAsync(TMessage message, CancellationToken cancellationToken)
    {
        foreach (var handler in handlers)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                await handler.HandleAsync(message, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("[{MeshId}] Message dispatching was canceled.", meshId);
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error occurred in handler {HandlerType} while processing message.", meshId, handler.GetType().Name);
            }
        }
    }
}