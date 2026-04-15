namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Dispatches incoming, validated abstract P2P network payloads natively routing them correctly to the relevant domain handlers reliably cleanly smoothly.
/// </summary>
public interface IApplicationPayloadDispatcher
{
    /// <summary>
    /// Routes the incoming unwrapped application payload to all localized registered domain consumers explicitly explicitly natively.
    /// </summary>
    /// <param name="meshId">The explicit network mesh ID context.</param>
    /// <param name="senderId">The localized peer identity representing the origin natively.</param>
    /// <param name="payload">The raw unboxed application business payload safely effectively correctly securely smoothly reliably cleanly natively effectively.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous dispatch operation.</returns>
    Task DispatchAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}