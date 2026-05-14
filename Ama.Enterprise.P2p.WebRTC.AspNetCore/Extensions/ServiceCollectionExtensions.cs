namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Extensions;

using System;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering generic ASP.NET Core out-of-band WebRTC signaling components explicitly.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers tracking explicit decoupled ASP.NET Core signaling endpoints abstracting WebRTC out-of-band handshakes.
    /// </summary>
    public static IP2pMeshBuilder AddAspNetCoreWebRtcSignaling(
        this IP2pMeshBuilder builder,
        Action<WebRtcSignalingOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        Action<WebRtcSignalingOptions> configAction = options =>
        {
            options.IsEnabled = true;
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_WebRtcSignaling", configAction))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configAction);

        builder.Services.AddHostedService(sp =>
        {
            var optionsMonitor = sp.GetRequiredService<IOptionsMonitor<WebRtcSignalingOptions>>();
            var invitationService = sp.GetRequiredKeyedService<IWebRtcInvitationService>(builder.MeshId);
            var serializer = sp.GetRequiredService<ICrdtSerializer>();
            var logger = sp.GetRequiredService<ILogger<WebRtcSignalingServer>>();

            return new WebRtcSignalingServer(
                builder.MeshId,
                optionsMonitor,
                invitationService,
                serializer,
                logger);
        });

        return builder;
    }

    /// <summary>
    /// Registers the out-of-band HTTP peer discovery mechanism connecting natively to remote WebRTC signaling endpoints.
    /// </summary>
    public static IP2pMeshBuilder AddWebRtcHttpPeerDiscovery(this IP2pMeshBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_WebRtcHttpPeerDiscovery"))
        {
            return builder;
        }

        builder.Services.AddHttpClient();
        builder.Services.TryAddSingleton<IWebRtcSignalingClient, WebRtcSignalingClient>();

        builder.Services.AddKeyedSingleton<IWebRtcHttpPeerDiscovery>(builder.MeshId, (sp, key) =>
        {
            var signalingClient = sp.GetRequiredService<IWebRtcSignalingClient>();
            var invitationService = sp.GetRequiredKeyedService<IWebRtcInvitationService>(key);
            var peerRegistry = sp.GetRequiredService<IPeerRegistry>();
            var authenticator = sp.GetRequiredKeyedService<IPeerAuthenticator>(key);
            var failureDetector = sp.GetRequiredKeyedService<IFailureDetector>(key);
            var logger = sp.GetRequiredService<ILogger<WebRtcHttpPeerDiscovery>>();
            
            return new WebRtcHttpPeerDiscovery((string)key!, signalingClient, invitationService, peerRegistry, authenticator, failureDetector, logger);
        });

        return builder;
    }
}