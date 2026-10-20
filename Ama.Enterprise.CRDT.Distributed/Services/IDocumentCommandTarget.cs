namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Represents a document instance capable of executing commands dispatched by the shared <see cref="IDocumentCommandPipeline"/>.
/// </summary>
/// <example>
/// <code>
/// await target.ExecuteCommandInternalAsync(command);
/// </code>
/// </example>
public interface IDocumentCommandTarget
{
    /// <summary>
    /// Gets the unique identifier of the document.
    /// </summary>
    string DocumentId { get; }

    /// <summary>
    /// Gets the shard identifier of the document.
    /// </summary>
    string ShardId { get; }

    /// <summary>
    /// Executes the specified pooled command against the document state machine.
    /// </summary>
    /// <param name="command">The pooled command to execute.</param>
    /// <returns>A task representing the asynchronous command execution.</returns>
    Task ExecuteCommandInternalAsync(PooledDocumentCommand command);
}