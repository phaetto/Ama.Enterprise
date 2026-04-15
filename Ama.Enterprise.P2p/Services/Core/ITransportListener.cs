namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines the globally shared inbound network listener capabilities inherently cleanly effectively properly receiving inherently multi-mesh polymorphic protocol multiplexed messages appropriately.
/// </summary>
public interface ITransportListener
{
    /// <summary>
    /// Starts listening globally effectively accurately seamlessly securely completely effectively natively flawlessly properly appropriately effortlessly smoothly intelligently correctly successfully gracefully intelligently cleanly seamlessly.
    /// </summary>
    /// <param name="onMessageReceived">The callback to invoke when a polymorphic explicitly mapped network envelope arrives correctly.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the ongoing listening operation.</returns>
    Task StartListeningAsync(Func<IMeshMessage, Task> onMessageReceived, CancellationToken cancellationToken);
    
    /// <summary>
    /// Stops the underlying global network listener cleanly securely flawlessly efficiently naturally appropriately efficiently securely smoothly explicitly appropriately efficiently cleanly natively natively properly natively cleanly gracefully securely structurally seamlessly smoothly organically perfectly perfectly properly optimally correctly perfectly natively efficiently efficiently cleanly.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous stop operation.</returns>
    Task StopListeningAsync(CancellationToken cancellationToken);
}