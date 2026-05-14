namespace Ama.Enterprise.P2p.WebRTC.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for securely registering cross-platform WebRTC transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers standalone WebRTC transport services binding actively underneath the current mesh context identifiers properly.
    /// </summary>
    public static IP2pMeshBuilder AddWebRtcTransport(
        this IP2pMeshBuilder builder,
        Action<WebRtcOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<WebRtcOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        if (!builder.Services.Any(s => s.ServiceType == typeof(IJsonTypeInfoResolver) && (string?)s.ServiceKey == "Ama.CRDT" && s.ImplementationInstance == WebRtcJsonContext.Default))
        {
            builder.Services.AddCrdtJsonTypeInfoResolver(WebRtcJsonContext.Default);
        }

        builder.Services.AddCrdtSerializableType<WebRtcPeerEndpoint>("webrtc-peer-endpoint");

        builder.Services.AddCrdtJsonModifier(ti =>
        {
            if (ti.Type == typeof(PeerEndpoint))
            {
                ti.PolymorphismOptions ??= new JsonPolymorphismOptions
                {
                    TypeDiscriminatorPropertyName = "$type",
                    IgnoreUnrecognizedTypeDiscriminators = true,
                    UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FallBackToBaseType
                };

                if (!ti.PolymorphismOptions.DerivedTypes.Any(dt => dt.DerivedType == typeof(WebRtcPeerEndpoint)))
                {
                    ti.PolymorphismOptions.DerivedTypes.Add(new JsonDerivedType(typeof(WebRtcPeerEndpoint), "webrtc-peer-endpoint"));
                }
            }
        });

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var nodeOptions = sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>().Get((string)key!);
            return new WebRtcPeerEndpoint(nodeOptions.LocalPeerId);
        });

        builder.Services.AddKeyedSingleton(builder.MeshId, (sp, key) =>
            new WebRtcConnectionManager(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<WebRtcOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<WebRtcConnectionManager>>()));

        builder.Services.AddKeyedSingleton<IWebRtcConnectionManager>(builder.MeshId, (sp, key) =>
            sp.GetRequiredKeyedService<WebRtcConnectionManager>(key));
            
        builder.Services.AddKeyedSingleton<IWebRtcInvitationService>(builder.MeshId, (sp, key) =>
            sp.GetRequiredKeyedService<WebRtcConnectionManager>(key));

        builder.Services.AddKeyedSingleton<ITransport>(builder.MeshId, (sp, key) =>
            new WebRtcTransport(
                (string)key!,
                sp.GetRequiredKeyedService<IWebRtcConnectionManager>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<WebRtcTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(builder.MeshId, (sp, key) =>
            new WebRtcTransportListener(
                (string)key!,
                sp.GetRequiredKeyedService<IWebRtcConnectionManager>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<WebRtcTransportListener>>()));

        return builder;
    }
}