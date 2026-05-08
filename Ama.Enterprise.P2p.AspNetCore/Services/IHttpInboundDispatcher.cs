namespace Ama.Enterprise.P2p.AspNetCore.Services;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines a centralized dispatcher to route decentralized HTTP payloads into the P2P engine decoupled from hosting frameworks.
/// </summary>
public interface IHttpInboundDispatcher
{
    /// <summary>
    /// Registers an inbound message processor delegate for a specific mesh topology.
    /// </summary>
    /// <param name="meshId">The identifier of the localized mesh.</param>
    /// <param name="onMessageReceived">The delegate representing the protocol logic.</param>
    void RegisterListener(string meshId, Func<IMeshMessage, Task> onMessageReceived);

    /// <summary>
    /// Unregisters an existing message processor for a specific mesh topology.
    /// </summary>
    /// <param name="meshId">The identifier of the localized mesh.</param>
    void UnregisterListener(string meshId);

    /// <summary>
    /// Validates, deserializes, and routes an incoming HTTP payload into the underlying registered protocol logic.
    /// </summary>
    /// <param name="targetMeshId">The explicit mesh ID this payload is intended for.</param>
    /// <param name="protocolVersionHeader">The extracted protocol version from the HTTP headers, if any.</param>
    /// <param name="bodyStream">The raw stream of the HTTP request body.</param>
    /// <param name="cancellationToken">Cancellation token for aborting the operation.</param>
    /// <returns>An enumeration detailing the processing outcome.</returns>
    Task<HttpPayloadProcessResult> ProcessPayloadAsync(string targetMeshId, string? protocolVersionHeader, Stream bodyStream, CancellationToken cancellationToken);
}