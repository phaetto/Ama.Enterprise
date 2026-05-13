namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the generic orchestrator for the P2P protocol, managing the lifecycle of the P2P node.
/// This interface abstracts away the underlying distribution algorithm (e.g., Gossip, Push-Pull, Rumor Mongering)
/// and acts universally across all defined meshes.
/// </summary>
public interface IP2pAlgorithm
{
    /// <summary>
    /// Starts the P2P protocol across all configured meshes, including periodic syncing and listening to incoming messages.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous start operation.</returns>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Gracefully stops the P2P protocol and underlying network operations across all configured meshes.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    Task StopAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Submits a payload to be distributed to all active network meshes using the underlying P2P algorithm.
    /// </summary>
    /// <param name="payload">The serialized CRDT or business payload to distribute.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous broadcast operation.</returns>
    Task BroadcastAsync(ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);

    /// <summary>
    /// Processes an incoming generic mesh message delegating it to the underlying concrete protocol algorithm.
    /// </summary>
    /// <param name="message">The incoming mapped protocol message.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous processing operation.</returns>
    Task ProcessMessageAsync(IMeshMessage message, CancellationToken cancellationToken);
}