namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;

/// <summary>
/// Observer interface for components that need to intercept and broadcast locally generated CRDT patches.
/// </summary>
public interface IDocumentPatchObserver
{
    /// <summary>
    /// Invoked asynchronously when a new CRDT patch is natively generated and verified locally.
    /// </summary>
    /// <param name="documentId">The unique identifier of the target document.</param>
    /// <param name="patch">The generated logical patch.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OnPatchGeneratedAsync(string documentId, CrdtPatch patch, CancellationToken cancellationToken = default);
}