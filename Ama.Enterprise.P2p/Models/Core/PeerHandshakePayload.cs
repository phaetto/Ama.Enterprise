namespace Ama.Enterprise.P2p.Models.Core;

using System.Text.Json;
using System.Text.Json.Serialization;

/// <summary>
/// DTO encapsulating the peer identity and its specific authentication handshake payload natively during Phase 2 generic negotiations.
/// </summary>
public record struct PeerHandshakePayload: IExtensibleDistributedPayload
{
    /// <summary>
    /// Gets the peer node identity and endpoint structurally mapped explicitly natively.
    /// </summary>
    public PeerNode Node { get; set; }

    /// <summary>
    /// Gets the raw security handshake data (e.g., certificate bytes) passed explicitly safely directly natively.
    /// </summary>
    public byte[] HandshakeData { get; set; }

    /// <inheritdoc />
    [JsonExtensionData]
    public IDictionary<string, JsonElement>? JsonExtensionData { get; set; }

    /// <inheritdoc />
    [JsonIgnore]
    public IList<ReadOnlyMemory<byte>>? BinaryExtensionData { get; set; }
}