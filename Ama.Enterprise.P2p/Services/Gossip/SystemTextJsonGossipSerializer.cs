using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;

namespace Ama.Enterprise.P2p.Services.Gossip;

/// <summary>
/// AOT-friendly JSON context for P2P models.
/// </summary>
[JsonSerializable(typeof(GossipMessage))]
internal partial class P2pJsonSerializerContext : JsonSerializerContext
{
}

/// <summary>
/// Implements AOT-friendly generic serialization for gossip messages using System.Text.Json.
/// </summary>
public sealed class SystemTextJsonGossipSerializer : IMessageSerializer<GossipMessage>
{
    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize(GossipMessage message)
    {
        return JsonSerializer.SerializeToUtf8Bytes(message, P2pJsonSerializerContext.Default.GossipMessage);
    }

    /// <inheritdoc />
    public GossipMessage Deserialize(ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty)
        {
            return default;
        }

        try
        {
            return JsonSerializer.Deserialize(data.Span, P2pJsonSerializerContext.Default.GossipMessage);
        }
        catch (JsonException)
        {
            return default;
        }
    }
}