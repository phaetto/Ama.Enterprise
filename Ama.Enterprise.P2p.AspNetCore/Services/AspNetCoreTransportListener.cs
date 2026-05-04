namespace Ama.Enterprise.P2p.AspNetCore.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Http.Core.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Bridge implementation connecting decentralized internal generic processing logic exposing mesh subscriptions directly evaluating existing decoupled orchestrators reliably.
/// </summary>
public sealed class AspNetCoreTransportListener(
    string meshId,
    IHttpInboundDispatcher dispatcher,
    ILogger<AspNetCoreTransportListener> logger) : ITransportListener, IDisposable
{
    private readonly string meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
    private readonly IHttpInboundDispatcher dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
    private readonly ILogger<AspNetCoreTransportListener> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken)
    {
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

        dispatcher.RegisterListener(meshId, onMessageReceived);
        
        logger.LogInformation("[{MeshId}] ASP.NET Core listener bridge registered successfully tracking generic HTTP inbound traffic.", meshId);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopListeningAsync(CancellationToken cancellationToken)
    {
        dispatcher.UnregisterListener(meshId);
        logger.LogInformation("[{MeshId}] ASP.NET Core listener bridge explicitly detached bounds.", meshId);
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        dispatcher.UnregisterListener(meshId);
    }
}