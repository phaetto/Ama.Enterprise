namespace Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Metadata tracking registered meshes for the hosted service orchestrator.
/// </summary>
/// <param name="MeshId">The unique identifier associated with a configured P2P mesh context.</param>
public sealed record P2pMeshMetadata(string MeshId);