namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines mechanisms for discovering other peers within the network.
/// Exposes lifecycle hooks for passive listeners alongside active polling intervals natively decoupled from loops.
/// </summary>
public interface IPeerDiscovery
{
    /// <summary>
    /// Gets the explicitly configured interval between active discovery polling loop iterations.
    /// </summary>
    TimeSpan DiscoveryInterval { get; }

    /// <summary>
    /// Starts the inbound passive listener for remote discoveries (e.g., multicast or broadcast receivers).
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    Task StartListeningAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Stops the inbound passive listener gracefully.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    Task StopListeningAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Executes an active discovery process to find available peers on the network explicitly explicitly decoupled from polling.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task containing an enumerable of discovered peers natively.</returns>
    Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken);
}