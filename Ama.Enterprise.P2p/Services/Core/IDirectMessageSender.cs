namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Provides point-to-point direct message delivery, decoupling targeted payloads (like anti-entropy syncs) from the epidemic gossip broadcast mechanism.
/// </summary>
public interface IDirectMessageSender
{
    /// <summary>
    /// Sends a payload directly to a specific peer bypassing the epidemic broadcast protocol.
    /// </summary>
    /// <param name="targetPeerId">The targeted peer identifier.</param>
    /// <param name="payload">The binary payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task SendDirectAsync(PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);

    /// <summary>
    /// Sends a payload directly to a specific peer bypassing the epidemic broadcast protocol targeting a specific mesh explicitly natively.
    /// </summary>
    /// <param name="meshId">The explicit network mesh ID.</param>
    /// <param name="targetPeerId">The targeted peer identifier.</param>
    /// <param name="payload">The binary payload.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    Task SendDirectAsync(string meshId, PeerId targetPeerId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}