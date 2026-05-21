namespace Ama.Enterprise.P2p.Services.Core;

using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Interface for orchestrating Phase 2 protocol negotiations and active listener bounds natively decoupled from background orchestration.
/// </summary>
public interface IPeerHandshaker
{
    /// <summary>
    /// Gets the explicitly configured local port used for handshaking natively.
    /// </summary>
    int LocalHandshakePort { get; }

    /// <summary>
    /// Starts the inbound passive listener resolving remote incoming Phase 2 negotiations.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    Task StartListeningAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Stops the inbound passive listener cleanly.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    Task StopListeningAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Initiates an active outbound Phase 2 handshake against a strictly specified remote endpoint safely natively.
    /// </summary>
    /// <param name="localNode">The explicit local peer identity initializing the network probe.</param>
    /// <param name="endpoint">The explicitly bounded remote address target.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>The negotiated remote peer identity implicitly verified.</returns>
    Task<PeerNode?> HandshakeAsync(PeerNode localNode, IPEndPoint endpoint, CancellationToken cancellationToken);
}