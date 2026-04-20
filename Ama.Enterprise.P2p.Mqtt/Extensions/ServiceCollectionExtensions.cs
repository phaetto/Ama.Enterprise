namespace Ama.Enterprise.P2p.Mqtt.Extensions;

using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Mqtt.Services;
using Ama.Enterprise.P2p.Mqtt.Services.Discovery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering the MQTT transport mechanism into the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
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

        if (!builder.Services.Any(s => s.ServiceType == typeof(IJsonTypeInfoResolver) && (string?)s.ServiceKey == "Ama.CRDT" && s.ImplementationInstance == MqttJsonContext.Default))
        {
            builder.Services.AddKeyedSingleton<IJsonTypeInfoResolver>("Ama.CRDT", MqttJsonContext.Default);
        }

        builder.Services.AddCrdtSerializableType<MqttPeerEndpoint>("mqtt-peer-endpoint");

        builder.Services.Configure<JsonSerializerOptions>(options =>
        {
            if (options.TypeInfoResolver is not null)
            {
                options.TypeInfoResolver = options.TypeInfoResolver.WithAddedModifier(ti =>
                {
                    if (ti.Type == typeof(PeerEndpoint))
                    {
                        ti.PolymorphismOptions ??= new JsonPolymorphismOptions
                        {
                            TypeDiscriminatorPropertyName = "$type",
                            IgnoreUnrecognizedTypeDiscriminators = true,
                            UnknownDerivedTypeHandling = System.Text.Json.Serialization.JsonUnknownDerivedTypeHandling.FallBackToBaseType
                        };

                        if (!ti.PolymorphismOptions.DerivedTypes.Any(dt => dt.DerivedType == typeof(MqttPeerEndpoint)))
                        {
                            ti.PolymorphismOptions.DerivedTypes.Add(new JsonDerivedType(typeof(MqttPeerEndpoint), "mqtt-peer-endpoint"));
                        }
                    }
                });
            }
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

    /// <summary>
    /// Registers MQTT-based peer discovery mechanisms for the specified mesh.
    /// </summary>
    public static IP2pMeshBuilder AddMqttPeerDiscovery(
        this IP2pMeshBuilder builder,
        Action<MqttDiscoveryOptions>? configureOptions = null)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<MqttDiscoveryOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        builder.Services.AddKeyedSingleton<IPeerDiscovery>(builder.MeshId, (sp, key) =>
            new MqttPeerDiscovery(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<MqttTransportOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<MqttDiscoveryOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredService<ILogger<MqttPeerDiscovery>>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredKeyedService<IPeerAuthenticator>(key),
                sp.GetRequiredKeyedService<IFailureDetector>(key)));

        builder.Services.AddHostedService(sp =>
            (MqttPeerDiscovery)sp.GetRequiredKeyedService<IPeerDiscovery>(builder.MeshId));

        return builder;
    }
}