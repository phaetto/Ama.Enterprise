namespace Ama.Enterprise.P2p.Mqtt.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Mqtt.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering the MQTT transport mechanism into the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    private sealed record MqttSerializationMarker;

    internal static void TryAddMqttSerialization(IServiceCollection services)
    {
        var tracker = P2pMeshRegistrationTracker.GetOrCreate(services);
        if (!tracker.TryRegister<MqttSerializationMarker>("MqttSerialization_Core", null))
        {
            return;
        }

        services.AddCrdtJsonTypeInfoResolver(MqttJsonContext.Default);

        services.AddCrdtSerializableType<MqttPeerEndpoint>("mqtt-peer-endpoint");

        services.AddCrdtJsonModifier(ti =>
        {
            if (ti.Type == typeof(PeerEndpoint))
            {
                ti.PolymorphismOptions ??= new JsonPolymorphismOptions
                {
                    TypeDiscriminatorPropertyName = "$type",
                    IgnoreUnrecognizedTypeDiscriminators = true,
                    UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FallBackToBaseType
                };

                if (!ti.PolymorphismOptions.DerivedTypes.Any(dt => dt.DerivedType == typeof(MqttPeerEndpoint)))
                {
                    ti.PolymorphismOptions.DerivedTypes.Add(new JsonDerivedType(typeof(MqttPeerEndpoint), "mqtt-peer-endpoint"));
                }
            }
        });
    }

    /// <summary>
    /// Registers standalone MQTT transport services.
    /// </summary>
    public static IP2pMeshBuilder AddMqttTransport(
        this IP2pMeshBuilder builder,
        Action<MqttTransportOptions>? configureOptions = null)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<MqttTransportOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        TryAddMqttSerialization(builder.Services);

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var nodeOptions = sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>().Get((string)key!);
            return new MqttPeerEndpoint(nodeOptions.LocalPeerId.ToString());
        });

        builder.Services.AddKeyedSingleton<MqttClientManager>(builder.MeshId, (sp, key) =>
            new MqttClientManager(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<MqttTransportOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredService<ILogger<MqttClientManager>>()));

        builder.Services.AddKeyedSingleton<IMqttClientManager>(builder.MeshId, (sp, key) =>
            sp.GetRequiredKeyedService<MqttClientManager>(key));

        builder.Services.AddKeyedSingleton<ITransport>(builder.MeshId, (sp, key) =>
            new MqttTransport(
                (string)key!,
                sp.GetRequiredKeyedService<IMqttClientManager>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<MqttTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(builder.MeshId, (sp, key) =>
            new MqttTransportListener(
                (string)key!,
                sp.GetRequiredKeyedService<IMqttClientManager>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<MqttTransportListener>>()));

        return builder;
    }
}