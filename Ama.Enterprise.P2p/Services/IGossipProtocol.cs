using Ama.Enterprise.P2p.Models;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// Defines the orchestrator for the Gossip protocol, managing the lifecycle of the P2P node.
/// </summary>
public interface IGossipProtocol
{
    /// <summary>
    /// Starts the gossip protocol, including periodic syncing and listening to incoming messages.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous start operation.</returns>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Gracefully stops the gossip protocol and underlying network listeners.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    Task StopAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Submits a payload to be gossiped to the network.
    /// </summary>
    /// <param name="payload">The serialized CRDT or business payload to distribute.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous broadcast operation.</returns>
    Task BroadcastAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}