namespace Ama.Enterprise.P2p.Extensions;

using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Implementation of <see cref="IP2pMeshBuilder"/> to handle multi-mesh dependency injection configurations.
/// </summary>
internal sealed class P2pMeshBuilder : IP2pMeshBuilder
{
    /// <summary>
    /// Initializes a new instance of the <see cref="P2pMeshBuilder"/> class.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="meshId">The mesh identifier.</param>
    public P2pMeshBuilder(IServiceCollection services, string meshId)
    {
        Services = services;
        MeshId = meshId;
    }

    /// <inheritdoc />
    public IServiceCollection Services { get; }

    /// <inheritdoc />
    public string MeshId { get; }
}