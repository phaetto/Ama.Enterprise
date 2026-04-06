namespace Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Defines a domain-level consumer for P2P network messages (e.g., the component that merges CRDTs).
/// </summary>
/// <typeparam name="TMessage">The type of the message being handled.</typeparam>
public interface IMessageHandler<in TMessage>
{
    /// <summary>
    /// Processes an incoming payload received from the network.
    /// </summary>
    /// <param name="message">The message containing the domain payload.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous handling operation.</returns>
    Task HandleAsync(TMessage message, CancellationToken cancellationToken);
}