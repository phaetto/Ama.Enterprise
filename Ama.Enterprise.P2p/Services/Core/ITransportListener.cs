namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the globally shared inbound network listener capabilities inherently receiving protocol-specific multi-mesh multiplexed messages explicitly correctly.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public interface ITransportListener<TMessage> where TMessage : IMeshMessage
{
    /// <summary>
    /// Starts listening globally for incoming network traffic natively invoking the provided callback multiplexing correctly upon receiving a message.
    /// </summary>
    /// <param name="onMessageReceived">The callback to invoke cleanly when a dynamically mapped mesh message successfully resolves natively.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the ongoing listening operation.</returns>
    Task StartListeningAsync(Func<TMessage, Task> onMessageReceived, CancellationToken cancellationToken);
    
    /// <summary>
    /// Stops the underlying global network listener smoothly correctly natively.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    Task StopListeningAsync(CancellationToken cancellationToken);
}