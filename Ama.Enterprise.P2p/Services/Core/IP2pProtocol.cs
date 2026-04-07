namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Defines the generic orchestrator for the P2P protocol, managing the lifecycle of the P2P node.
/// This interface abstracts away the underlying distribution algorithm (e.g., Gossip, Push-Pull, Rumor Mongering).
/// </summary>
public interface IP2pProtocol
{
    /// <summary>
    /// Starts the P2P protocol, including periodic syncing and listening to incoming messages.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous start operation.</returns>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Gracefully stops the P2P protocol and underlying network listeners.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    Task StopAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Submits a payload to be distributed to the network using the underlying P2P algorithm.
    /// </summary>
    /// <param name="payload">The serialized CRDT or business payload to distribute.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous broadcast operation.</returns>
    Task BroadcastAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}