namespace Ama.Enterprise.P2p.Kestrel.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Kestrel.Models;
using Ama.Enterprise.P2p.Kestrel.Services;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for securely registering the Kestrel-based HTTP transport.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the core Kestrel transport explicitly isolated securely mapped to the current mesh context structurally.
    /// </summary>
    public static IP2pMeshBuilder AddKestrelTransport(
        this IP2pMeshBuilder builder,
        Action<KestrelTransportOptions>? configureOptions = null)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        Action<KestrelTransportOptions> configAction = options => 
        {
            options.IsEnabled = true;
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configAction))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configAction);

        if (!builder.Services.Any(s => s.ServiceType == typeof(IJsonTypeInfoResolver) && (string?)s.ServiceKey == "Ama.CRDT" && s.ImplementationInstance == KestrelJsonContext.Default))
        {
            builder.Services.AddCrdtJsonTypeInfoResolver(KestrelJsonContext.Default);
        }

        builder.Services.AddCrdtSerializableType<KestrelPeerEndpoint>("kestrel-peer-endpoint");

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

                if (!ti.PolymorphismOptions.DerivedTypes.Any(dt => dt.DerivedType == typeof(KestrelPeerEndpoint)))
                {
                    ti.PolymorphismOptions.DerivedTypes.Add(new JsonDerivedType(typeof(KestrelPeerEndpoint), "kestrel-peer-endpoint"));
                }
            }
        });

        builder.Services.AddHttpClient("P2pKestrelTransport");

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<KestrelTransportOptions>>().Get((string)key!);
            var host = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? "localhost" 
                : options.ListenHost;
                
            return new KestrelPeerEndpoint(host, options.ListenPort);
        });

        builder.Services.AddKeyedSingleton<ITransport>(builder.MeshId, (sp, key) =>
            new KestrelTransport(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<KestrelTransportOptions>>(),
                sp.GetRequiredService<IHttpClientFactory>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<KestrelTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(builder.MeshId, (sp, key) =>
            new KestrelTransportListener(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<KestrelTransportOptions>>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<KestrelTransportListener>>()));

        return builder;
    }
}