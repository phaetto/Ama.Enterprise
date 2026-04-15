namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implements the generic application payload dispatcher, routing raw unwrapped payloads to all registered domain handlers.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="ApplicationPayloadDispatcher"/> class.
/// </remarks>
public sealed class ApplicationPayloadDispatcher(
    string meshId,
    IEnumerable<IApplicationPayloadHandler> handlers,
    ILogger<ApplicationPayloadDispatcher> logger) : IApplicationPayloadDispatcher
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IEnumerable<IApplicationPayloadHandler> handlers = handlers ?? throw new ArgumentNullException(nameof(handlers));
    private readonly ILogger<ApplicationPayloadDispatcher> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task DispatchAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        foreach (var handler in handlers)
        {
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                await handler.HandlePayloadAsync(meshId, senderId, payload, cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                logger.LogInformation("[{MeshId}] Application payload dispatching was canceled.", meshId);
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error occurred in payload handler {HandlerType} while processing.", meshId, handler.GetType().Name);
            }
        }
    }
}