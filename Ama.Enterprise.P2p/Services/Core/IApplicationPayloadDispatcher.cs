namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Dispatches incoming, validated abstract P2P network payloads routing them to the relevant domain handlers.
/// </summary>
public interface IApplicationPayloadDispatcher
{
    /// <summary>
    /// Routes the incoming unwrapped application payload to all localized registered domain consumers.
    /// </summary>
    /// <param name="meshId">The explicit network mesh ID context.</param>
    /// <param name="senderId">The localized peer identity representing the origin.</param>
    /// <param name="payload">The raw unboxed application business payload.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous dispatch operation.</returns>
    Task DispatchAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}