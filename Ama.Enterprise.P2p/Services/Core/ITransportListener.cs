namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the globally shared inbound network listener capabilities.
/// </summary>
public interface ITransportListener
{
    /// <summary>
    /// Starts listening globally.
    /// </summary>
    /// <param name="onMessageReceived">The callback to invoke when a polymorphic mapped network envelope arrives.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the ongoing listening operation.</returns>
    Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken);
    
    /// <summary>
    /// Stops the underlying global network listener.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    Task StopListeningAsync(CancellationToken cancellationToken);
}