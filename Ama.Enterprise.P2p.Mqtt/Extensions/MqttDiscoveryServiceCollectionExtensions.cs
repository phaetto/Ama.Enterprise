namespace Ama.Enterprise.P2p.Mqtt.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Mqtt.Models;
using Ama.Enterprise.P2p.Mqtt.Services.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering MQTT peer discovery components tied to a specific mesh profile.
/// </summary>
public static class MqttDiscoveryServiceCollectionExtensions
{
    private sealed record MqttDiscoverySerializationMarker;

    internal static void TryAddMqttDiscoverySerialization(IServiceCollection services)
    {
        var tracker = P2pMeshRegistrationTracker.GetOrCreate(services);
        if (!tracker.TryRegister<MqttDiscoverySerializationMarker>("MqttDiscoverySerialization_Core", null))
        {
            return;
        }

        services.AddCrdtJsonTypeInfoResolver(MqttDiscoveryJsonContext.Default);
    }

    /// <summary>
    /// Registers the Phase 1 MQTT peer discovery services and options under the current mesh context.
    /// This extension requires the mesh to register a valid <see cref="IPeerHandshaker"/> Keyed service implementation to operate.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the MQTT discovery options.</param>
    /// <returns>The updated mesh builder.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IP2pMeshBuilder AddMqttPeerDiscovery(
        this IP2pMeshBuilder builder,
        Action<MqttDiscoveryOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_MqttDiscovery", configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        ServiceCollectionExtensions.TryAddMqttSerialization(builder.Services);
        TryAddMqttDiscoverySerialization(builder.Services);

        builder.Services.AddKeyedSingleton<IPeerDiscovery>(builder.MeshId, (sp, key) =>
            new MqttPeerDiscovery(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<MqttDiscoveryOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredKeyedService<IPeerHandshaker>(key),
                sp.GetRequiredService<ILogger<MqttPeerDiscovery>>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredKeyedService<IPeerAuthenticator>(key),
                sp.GetRequiredKeyedService<IFailureDetector>(key)));

        return builder;
    }

    /// <summary>
    /// Registers the Phase 2 isolated MQTT handshaker implementation under the current mesh context.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the MQTT handshake options.</param>
    /// <returns>The updated mesh builder.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IP2pMeshBuilder AddMqttPeerHandshake(
        this IP2pMeshBuilder builder,
        Action<MqttHandshakeOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_MqttHandshaker", configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        ServiceCollectionExtensions.TryAddMqttSerialization(builder.Services);
        TryAddMqttDiscoverySerialization(builder.Services);

        builder.Services.AddKeyedSingleton<IPeerHandshaker>(builder.MeshId, (sp, key) =>
            new MqttPeerHandshaker(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<MqttHandshakeOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<MqttPeerHandshaker>>()));

        return builder;
    }
}