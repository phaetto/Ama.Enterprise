namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Defines the inbound network listener capabilities for receiving protocol-specific messages from peers.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public interface ITransportListener<out TMessage>
{
    /// <summary>
    /// Starts listening for incoming network traffic and invokes the provided callback upon receiving a message.
    /// </summary>
    /// <param name="onMessageReceived">The callback to invoke when a message is successfully received and parsed.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the ongoing listening operation.</returns>
    Task StartListeningAsync(Func<TMessage, Task> onMessageReceived, CancellationToken cancellationToken);
    
    /// <summary>
    /// Stops the underlying network listener.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    Task StopListeningAsync(CancellationToken cancellationToken);
}