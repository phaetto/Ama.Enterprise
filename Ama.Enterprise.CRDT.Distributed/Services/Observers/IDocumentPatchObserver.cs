namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Observer interface for components that need to intercept and broadcast locally generated CRDT patches.
/// </summary>
/// <example>
/// <code>
/// public sealed class CustomPatchObserver : IDocumentPatchObserver
/// {
///     public Task OnPatchGeneratedAsync(string documentId, string? documentTypeAlias, CrdtPatch patch, CancellationToken cancellationToken = default)
///     {
///         return Task.CompletedTask;
///     }
/// }
/// </code>
/// </example>
public interface IDocumentPatchObserver
{
    /// <summary>
    /// Invoked asynchronously when a new CRDT patch is natively generated and verified locally.
    /// </summary>
    /// <param name="documentId">The unique identifier of the target document.</param>
    /// <param name="documentTypeAlias">The registered type alias of the target document.</param>
    /// <param name="patch">The generated logical patch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OnPatchGeneratedAsync(string documentId, string? documentTypeAlias, CrdtPatch patch, CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes any queued outbound patches to the underlying transport before shutdown.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous flush operation.</returns>
    Task FlushAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;
}