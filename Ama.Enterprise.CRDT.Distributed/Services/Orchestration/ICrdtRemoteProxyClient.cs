namespace Ama.Enterprise.CRDT.Distributed.Services.Orchestration;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Documents;

/// <summary>
/// Handles outbound P2P RPC forwarding for document reads, operation intents, patches, and demoted shard snapshots.
/// </summary>
/// <example>
/// <code>
/// var proxy = serviceProvider.GetRequiredService&lt;ICrdtRemoteProxyClient&gt;();
/// var doc = await proxy.GetRemoteDocumentStateAsync&lt;MyState&gt;("doc-1", "shard-1", "my-type", cancellationToken);
/// </code>
/// </example>
public interface ICrdtRemoteProxyClient
{
    /// <summary>
    /// Reads the materialized state of a document from a remote replica hosting the target shard.
    /// </summary>
    /// <typeparam name="TState">The document state type.</typeparam>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="shardId">The target shard identifier.</param>
    /// <param name="documentTypeAlias">The optional document type alias.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The deserialized remote <see cref="CrdtDocument{TState}"/>.</returns>
    ValueTask<CrdtDocument<TState>> GetRemoteDocumentStateAsync<TState>(
        string documentId,
        string shardId,
        string? documentTypeAlias,
        CancellationToken cancellationToken = default)
        where TState : class, new();

    /// <summary>
    /// Forwards an operation intent to a remote replica hosting the target shard.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="shardId">The target shard identifier.</param>
    /// <param name="documentTypeAlias">The optional document type alias.</param>
    /// <param name="intent">The operation intent to forward.</param>
    /// <param name="targetProperty">The target property name on the document state.</param>
    /// <param name="deliveryMode">Whether to wait for an RPC acknowledgment or send fire-and-forget.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns><see langword="true"/> if the intent was accepted or dispatched; otherwise, <see langword="false"/>.</returns>
    Task<bool> ForwardIntentAsync(
        string documentId,
        string shardId,
        string? documentTypeAlias,
        IOperationIntent intent,
        string targetProperty,
        IntentDeliveryMode deliveryMode,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Forwards a pre-generated CRDT patch to the target shard's remote replicas and broadcasts it.
    /// </summary>
    /// <param name="documentId">The document identifier.</param>
    /// <param name="shardId">The target shard identifier.</param>
    /// <param name="documentTypeAlias">The optional document type alias.</param>
    /// <param name="patch">The CRDT patch to forward.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ForwardPatchAsync(
        string documentId,
        string shardId,
        string? documentTypeAlias,
        CrdtPatch patch,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Pushes a final snapshot of a locally demoted document to the shard's current target replicas.
    /// </summary>
    /// <param name="document">The demoted document instance.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PushDemotedDocumentSnapshotToTargetPeersAsync(
        IDistributedCrdtDocument document,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Resolves the ordered list of candidate remote replica identifiers for a given shard.
    /// </summary>
    /// <param name="shardId">The shard identifier.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A list of remote replica identifiers.</returns>
    Task<List<string>> GetCandidateRemoteReplicasForShardAsync(
        string shardId,
        CancellationToken cancellationToken = default);
}