namespace Ama.Enterprise.CRDT.Distributed.Services.Observers;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Observer interface for domain components relying on broader topological changes within the active CRDT document matrix.
/// </summary>
public interface ICrdtOrchestratorObserver
{
    /// <summary>
    /// Invoked asynchronously when the orchestrator dynamically maps, promotes, demotes, or deletes documents.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OnTopologyChangedAsync(CancellationToken cancellationToken = default);
}