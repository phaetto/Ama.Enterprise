namespace Ama.Enterprise.CRDT.Distributed.Services.Commands;

using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Represents a document instance capable of executing commands dispatched by the shared <see cref="IDocumentCommandPipeline"/>.
/// </summary>
/// <example>
/// <code>
/// await target.WaitForInitializationGateAsync();
/// await target.ExecuteCommandInternalAsync(command);
/// </code>
/// </example>
public interface IDocumentCommandTarget
{
    /// <summary>
    /// Gets the composite replica identifier of the scope owning this document.
    /// </summary>
    string ScopeReplicaId { get; }

    /// <summary>
    /// Gets the unique identifier of the document.
    /// </summary>
    string DocumentId { get; }

    /// <summary>
    /// Gets the shard identifier of the document.
    /// </summary>
    string ShardId { get; }

    /// <summary>
    /// Gets a value indicating whether the document has completed initialization and opened its command processing gate.
    /// </summary>
    bool IsInitialized { get; }

    /// <summary>
    /// Waits asynchronously until the document has finished initializing before processing any enqueued stripe commands.
    /// </summary>
    /// <returns>A value task that completes once initialization has finished.</returns>
    ValueTask WaitForInitializationGateAsync();

    /// <summary>
    /// Executes the specified pooled command against the document state machine.
    /// </summary>
    /// <param name="command">The pooled command to execute.</param>
    /// <returns>A task representing the asynchronous command execution.</returns>
    Task ExecuteCommandInternalAsync(PooledDocumentCommand command);
}