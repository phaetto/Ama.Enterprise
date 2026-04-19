namespace Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Extensions;

using System;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Models;
using Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Services;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Provides dependency injection extension methods to configure MQTT-based WebRTC signaling capabilities.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the MQTT WebRTC out-of-band signaling mechanism utilizing persistent push channels.
    /// This background orchestrator is optimized for Azure EventGrid MQTT capabilities.
    /// </summary>
    /// <param name="builder">The P2P mesh builder pipeline.</param>
    /// <param name="configureOptions">Delegate to configure the signaling properties.</param>
    /// <returns>The original P2P mesh builder.</returns>
    public static IP2pMeshBuilder AddMqttWebRtcSignaling(
        this IP2pMeshBuilder builder,
        Action<MqttSignalingOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var meshId = builder.MeshId;

        builder.Services.Configure<MqttSignalingOptions>(meshId, configureOptions);

        builder.Services.AddHostedService(sp =>
        {
            return ActivatorUtilities.CreateInstance<MqttSignalingService>(
                sp,
                meshId);
        });

        return builder;
    }
}