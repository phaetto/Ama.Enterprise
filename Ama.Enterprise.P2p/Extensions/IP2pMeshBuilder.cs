namespace Ama.Enterprise.P2p.Extensions;

using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Exposes mechanisms to build and configure a specific Keyed DI mesh profile.
/// </summary>
public interface IP2pMeshBuilder
{
    /// <summary>
    /// Gets the underlying service collection.
    /// </summary>
    IServiceCollection Services { get; }

    /// <summary>
    /// Gets the unique identifier for this specific mesh network context.
    /// </summary>
    string MeshId { get; }
}