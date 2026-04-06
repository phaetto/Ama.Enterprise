namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Dispatches incoming, validated P2P network messages to the relevant domain handlers (e.g., the CRDT engine).
/// </summary>
/// <typeparam name="TMessage">The type of the message being dispatched.</typeparam>
public interface IMessageDispatcher<in TMessage>
{
    /// <summary>
    /// Routes the incoming protocol message to all registered handlers.
    /// </summary>
    /// <param name="message">The incoming message.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous dispatch operation.</returns>
    Task DispatchAsync(TMessage message, CancellationToken cancellationToken);
}