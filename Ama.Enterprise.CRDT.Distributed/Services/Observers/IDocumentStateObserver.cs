namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Observer interface for components that need to react to state modifications within a specific CRDT document.
/// </summary>
public interface IDocumentStateObserver<TState> where TState : class, new()
{
    /// <summary>
    /// Invoked asynchronously whenever the underlying CRDT document state structurally changes.
    /// </summary>
    /// <param name="documentId">The unique identifier of the modified document.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default);
}