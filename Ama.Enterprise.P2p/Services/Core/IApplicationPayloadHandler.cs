namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines a domain-level consumer for abstract P2P application payloads.
/// </summary>
public interface IApplicationPayloadHandler
{
    /// <summary>
    /// Processes an incoming application payload unwrapped from the network protocol envelope.
    /// </summary>
    /// <param name="meshId">The explicit network mesh ID context.</param>
    /// <param name="senderId">The localized peer identity representing the origin of the payload.</param>
    /// <param name="payload">The raw unboxed application business payload.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task representing the asynchronous handling operation.</returns>
    Task HandlePayloadAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}