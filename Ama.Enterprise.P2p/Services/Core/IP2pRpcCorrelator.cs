namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Service responsible for mapping outbound point-to-point P2P requests to inbound asynchronous responses, providing a generic RPC-like experience.
/// </summary>
public interface IP2pRpcCorrelator
{
    /// <summary>
    /// Registers a pending RPC request and awaits the network response asynchronously.
    /// </summary>
    /// <typeparam name="TResponse">The expected response type to deserialize.</typeparam>
    /// <param name="correlationId">The unique identifier mapping the outbound request.</param>
    /// <param name="timeout">The maximum duration to wait before aborting the request.</param>
    /// <param name="cancellationToken">A token to observe for early cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, containing the deserialized response.</returns>
    /// <example>
    /// <code>
    /// var response = await correlator.RegisterRequestAsync&lt;CrdtReadResponseDto&gt;(guid, TimeSpan.FromSeconds(5), token);
    /// </code>
    /// </example>
    Task<TResponse> RegisterRequestAsync<TResponse>(Guid correlationId, TimeSpan timeout, CancellationToken cancellationToken);

    /// <summary>
    /// Resolves a pending RPC request, fulfilling the awaiting task with the inbound payload.
    /// </summary>
    /// <param name="correlationId">The unique identifier extracted from the network envelope.</param>
    /// <param name="payload">The raw unboxed byte payload to be deserialized by the requester.</param>
    void ResolveRequest(Guid correlationId, ReadOnlyMemory<byte> payload);
}