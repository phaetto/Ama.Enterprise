namespace Ama.Enterprise.P2p.Services.Gossip;

using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;

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
    private readonly JsonSerializerOptions serializerOptions;

    /// <summary>
    /// Initializes a new instance of the <see cref="SystemTextJsonGossipSerializer"/> class.
    /// </summary>
    /// <param name="serializerOptions">The JSON serializer options provided by the base library.</param>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public SystemTextJsonGossipSerializer(
        [FromKeyedServices("Ama.CRDT")] JsonSerializerOptions serializerOptions)
    {
        this.serializerOptions = serializerOptions ?? throw new ArgumentNullException(nameof(serializerOptions));
    }

    /// <inheritdoc />
    public ReadOnlyMemory<byte> Serialize(GossipMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);

        var typeInfo = this.serializerOptions.GetTypeInfo(typeof(GossipMessage));
        return JsonSerializer.SerializeToUtf8Bytes(message, typeInfo);
    }

    /// <inheritdoc />
    public GossipMessage Deserialize(ReadOnlyMemory<byte> data)
    {
        if (data.IsEmpty)
        {
            return default!;
        }

        try
        {
            var typeInfo = this.serializerOptions.GetTypeInfo(typeof(GossipMessage));
            var result = JsonSerializer.Deserialize(data.Span, typeInfo);
            return result is null ? default! : (GossipMessage)result;
        }
        catch (JsonException)
        {
            return default!;
        }
    }
}