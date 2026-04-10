namespace Ama.Enterprise.P2p.WebRTC.Extensions;

using System;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Ama.CRDT.Services.Serialization;

/// <summary>
/// Extension methods for securely registering cross-platform WebRTC transport components deeply integrated onto any configured P2P mesh logic explicitly.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers standalone WebRTC transport services binding actively underneath the current mesh context identifiers properly.
    /// </summary>
    /// <typeparam name="TMessage">The type of the generic message traversing via the transport bounds.</typeparam>
    /// <param name="builder">The mesh builder configuration instance pipeline.</param>
    /// <param name="configureOptions">An action specifying isolated STUN/TURN rules mapping appropriately.</param>
    /// <returns>The fully hydrated updated mesh builder.</returns>
    public static IP2pMeshBuilder AddWebRtcTransport<TMessage>(
        this IP2pMeshBuilder builder,
        Action<WebRtcOptions>? configureOptions = null)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<WebRtcOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        // Add the primary connection manager handling invitation lifecycle capabilities dynamically
        builder.Services.AddKeyedSingleton<WebRtcConnectionManager>(builder.MeshId, (sp, key) =>
            new WebRtcConnectionManager(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<WebRtcOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<WebRtcConnectionManager>>()));

        // Expose explicitly separated behavioral interfaces universally targeting the singleton reference safely
        builder.Services.AddKeyedSingleton<IWebRtcConnectionManager>(builder.MeshId, (sp, key) =>
            sp.GetRequiredKeyedService<WebRtcConnectionManager>(key));
            
        builder.Services.AddKeyedSingleton<IWebRtcInvitationService>(builder.MeshId, (sp, key) =>
            sp.GetRequiredKeyedService<WebRtcConnectionManager>(key));

        // Inject dynamic transport routers capturing WebRtc endpoint variations seamlessly
        builder.Services.AddKeyedSingleton<ITransport<TMessage>>(builder.MeshId, (sp, key) =>
            new WebRtcTransport<TMessage>(
                (string)key!,
                sp.GetRequiredKeyedService<IWebRtcConnectionManager>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<WebRtcTransport<TMessage>>>()));

        // Inject inbound transport listener tracking deeply routed RTCPeer connections continuously 
        builder.Services.AddKeyedSingleton<ITransportListener<TMessage>>(builder.MeshId, (sp, key) =>
            new WebRtcTransportListener<TMessage>(
                (string)key!,
                sp.GetRequiredKeyedService<IWebRtcConnectionManager>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<WebRtcTransportListener<TMessage>>>()));

        return builder;
    }
}