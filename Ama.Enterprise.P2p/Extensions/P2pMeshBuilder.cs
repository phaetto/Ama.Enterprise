namespace Ama.Enterprise.P2p.Extensions;

using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Implementation of <see cref="IP2pMeshBuilder"/> to handle multi-mesh dependency injection configurations.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="P2pMeshBuilder"/> class.
/// </remarks>
/// <param name="services">The service collection.</param>
/// <param name="meshId">The mesh identifier.</param>
internal sealed class P2pMeshBuilder(IServiceCollection services, string meshId) : IP2pMeshBuilder
{

    /// <inheritdoc />
    public IServiceCollection Services { get; } = services;

    /// <inheritdoc />
    public string MeshId { get; } = meshId;
}