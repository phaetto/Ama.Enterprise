namespace Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Imposes a centralized generic constraint on protocol messages to inherently map their own synchronization mesh identifiers natively, enabling protocol-agnostic multiplexing globally.
/// </summary>
public interface IMeshMessage
{
    /// <summary>
    /// Gets the unique identifier of the target P2P mesh network context.
    /// </summary>
    string MeshId { get; }
}