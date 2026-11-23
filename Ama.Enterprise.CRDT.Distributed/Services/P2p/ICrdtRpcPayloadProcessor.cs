namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Processes incoming P2P RPC request and response payloads for remote document reads, intent forwarding, and Map/Reduce batches.
/// </summary>
/// <example>
/// <code>
/// var rpcProcessor = scope.ServiceProvider.GetRequiredService&lt;ICrdtRpcPayloadProcessor&gt;();
/// await rpcProcessor.ProcessRpcPayloadAsync(scope, meshId, wrapper, senderId, cancellationToken);
/// </code>
/// </example>
public interface ICrdtRpcPayloadProcessor
{
    /// <summary>
    /// Handles an inbound CRDT RPC message envelope (<c>CrdtReadReq</c>, <c>CrdtReadRes</c>, <c>CrdtIntentReq</c>, <c>CrdtIntentRes</c>, <c>CrdtMapReduceReq</c>, or <c>CrdtMapReduceRes</c>).
    /// </summary>
    /// <param name="scope">The target CRDT replica scope.</param>
    /// <param name="meshId">The P2P mesh identifier.</param>
    /// <param name="wrapper">The deserialized message envelope.</param>
    /// <param name="senderId">The sender network peer identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ProcessRpcPayloadAsync(
        IDistributedCrdtScope scope,
        string meshId,
        CrdtMessageWrapper wrapper,
        PeerId senderId,
        CancellationToken cancellationToken = default);
}