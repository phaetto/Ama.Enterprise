namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Coordinates the graceful shutdown and state persistence of all active distributed CRDT replica scopes.
/// </summary>
/// <example>
/// <code>
/// public sealed class CustomLifecycleHandler
/// {
///     private readonly ICrdtShutdownCoordinator shutdownCoordinator;
///
///     public CustomLifecycleHandler(ICrdtShutdownCoordinator shutdownCoordinator)
///     {
///         this.shutdownCoordinator = shutdownCoordinator;
///     }
///
///     public async Task OnPreStopAsync(CancellationToken cancellationToken)
///     {
///         await shutdownCoordinator.FlushAllScopesAsync("KubernetesPreStopHook", cancellationToken);
///     }
/// }
/// </code>
/// </example>
public interface ICrdtShutdownCoordinator
{
    /// <summary>
    /// Gets a value indicating whether a shutdown flush has been initiated.
    /// </summary>
    bool IsShuttingDown { get; }

    /// <summary>
    /// Gets a <see cref="CancellationToken"/> that is triggered as soon as a shutdown or scope flush is initiated.
    /// </summary>
    CancellationToken ShutdownToken { get; }

    /// <summary>
    /// Flushes all active CRDT scopes idempotently according to the configured <see cref="Models.CrdtShutdownFlushMode"/>.
    /// If a flush is already in progress or has completed, subsequent callers await the same operation.
    /// </summary>
    /// <param name="reason">A descriptive reason for the flush trigger (e.g., "SIGTERM", "HostStopping").</param>
    /// <param name="cancellationToken">A token to cancel or bound the flush execution.</param>
    /// <returns>A task representing the asynchronous flush operation.</returns>
    Task FlushAllScopesAsync(string reason, CancellationToken cancellationToken = default);

    /// <summary>
    /// Flushes all active CRDT scopes and then requests the host application to stop if an <see cref="Microsoft.Extensions.Hosting.IHostApplicationLifetime"/> is registered.
    /// </summary>
    /// <param name="reason">A descriptive reason for the shutdown request.</param>
    /// <param name="cancellationToken">A token to cancel or bound the shutdown operation.</param>
    /// <returns>A task representing the asynchronous flush and stop request.</returns>
    Task RequestShutdownAsync(string reason, CancellationToken cancellationToken = default);
}