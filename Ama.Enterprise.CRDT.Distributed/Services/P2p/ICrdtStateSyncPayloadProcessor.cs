namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Processes incoming anti-entropy <c>CrdtSync</c> messages, evaluates causal journal requirements, and dispatches missing operations or snapshots.
/// </summary>
/// <example>
/// <code>
/// var syncProcessor = scope.ServiceProvider.GetRequiredService&lt;ICrdtStateSyncPayloadProcessor&gt;();
/// await syncProcessor.ProcessStateSyncAsync(scope, meshId, wrapper, senderId, rejectCallback, cancellationToken);
/// </code>
/// </example>
public interface ICrdtStateSyncPayloadProcessor
{
    /// <summary>
    /// Evaluates an incoming <c>CrdtSync</c> payload against the local replica's shard version vectors and journals.
    /// </summary>
    /// <param name="scope">The local CRDT replica scope.</param>
    /// <param name="meshId">The P2P mesh identifier.</param>
    /// <param name="wrapper">The deserialized message envelope.</param>
    /// <param name="senderId">The sender network peer identifier.</param>
    /// <param name="rejectEvictedReplicaAsync">Callback invoked if the sender's replica ID is tombstoned.</param>
    /// <param name="isLocalLoopbackSender">Predicate determining whether a message claiming the local replica ID is a harmless loopback.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ProcessStateSyncAsync(
        IDistributedCrdtScope scope,
        string meshId,
        CrdtMessageWrapper wrapper,
        PeerId senderId,
        Func<IDistributedCrdtScope, string, string, string, CancellationToken, Task> rejectEvictedReplicaAsync,
        Func<IDistributedCrdtScope, string, PeerId, bool> isLocalLoopbackSender,
        CancellationToken cancellationToken = default);
}