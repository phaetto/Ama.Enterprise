namespace Ama.Enterprise.FeatureFlags.Services;

using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Observer interface allowing external consumers to react dynamically to replicated feature flag modifications decoupled structurally.
/// </summary>
public interface IFeatureFlagStateObserver
{
    /// <summary>
    /// Invoked asynchronously whenever the distributed multi-mesh topology successfully applies a structural update reflecting feature flag modifications.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OnFeatureFlagsUpdatedAsync(CancellationToken cancellationToken = default);
}