namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Ama.Enterprise.P2p.Services.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Provides extension methods for registering generic P2P meshes.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Initiates the registration of a new P2P mesh network profile under the given identifier.
    /// </summary>
    public static IP2pMeshBuilder AddP2pMesh(
        this IServiceCollection services, 
        string meshId, 
        Action<P2pNodeOptions>? configureNodeOptions = null)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        if (configureNodeOptions is null)
        {
            services.Configure<P2pNodeOptions>(meshId, _ => { });
        }
        else
        {
            services.Configure(meshId, configureNodeOptions);
        }

        if (!services.Any(s => s.ImplementationType == typeof(P2pHostedService)))
        {
            services.AddHostedService<P2pHostedService>();
        }

        services.AddSingleton(new P2pMeshMetadata(meshId));

        return new P2pMeshBuilder(services, meshId);
    }

    /// <summary>
    /// Registers the core HTTP transport.
    /// </summary>
    public static IP2pMeshBuilder AddHttpTransport(
        this IP2pMeshBuilder builder,
        Action<HttpTransportOptions>? configureOptions = null)
    {
        if (configureOptions is null)
        {
            builder.Services.Configure<HttpTransportOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        builder.Services.AddHttpClient("P2pTransport");

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<HttpTransportOptions>>().Get((string)key!);
            var host = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? "localhost" 
                : options.ListenHost;
                
            return new HttpPeerEndpoint(host, options.ListenPort);
        });

        // Transport mechanisms are generic, polymorphic, and shared.
        builder.Services.TryAddSingleton<ITransport, HttpTransport>();
        builder.Services.TryAddSingleton<ITransportListener, HttpTransportListener>();

        return builder;
    }

    /// <summary>
    /// Registers the specific Gossip network protocol orchestrators.
    /// </summary>
    public static IP2pMeshBuilder AddGossipNetwork(
        this IP2pMeshBuilder builder, 
        Action<GossipOptions>? configureOptions = null)
    {
        if (configureOptions is null)
        {
            builder.Services.Configure<GossipOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        builder.Services.AddOptions<FailureDetectorOptions>(builder.MeshId)
            .Configure<IOptionsMonitor<GossipOptions>>((failureOptions, gossipOptionsMonitor) =>
            {
                var gossipOptions = gossipOptionsMonitor.Get(builder.MeshId);
                if (gossipOptions is not null)
                {
                    failureOptions.HeartbeatInterval = gossipOptions.GossipInterval;
                }
            });

        if (!builder.Services.Any(s => s.ServiceType == typeof(IJsonTypeInfoResolver) && s.ServiceKey as string == "Ama.CRDT" && s.ImplementationInstance == P2pJsonSerializerContext.Default))
        {
            builder.Services.AddKeyedSingleton<IJsonTypeInfoResolver>("Ama.CRDT", P2pJsonSerializerContext.Default);
        }

        builder.Services.AddKeyedSingleton<IInboundMessageQueue<GossipMessage>>(builder.MeshId, (sp, key) =>
            new InboundMessageQueue<GossipMessage>());

        builder.Services.TryAddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        builder.Services.AddKeyedSingleton<IPeerAuthenticator>(builder.MeshId, (sp, key) =>
            new PassThroughPeerAuthenticator(
                (string)key!, 
                sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        builder.Services.AddKeyedSingleton<IPeerSelector>(builder.MeshId, (sp, key) =>
            new RandomPeerSelector(
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<RandomPeerSelector>>()));

        builder.Services.AddKeyedSingleton<IFailureDetector>(builder.MeshId, (sp, key) =>
            new TimeBasedFailureDetector(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(),
                sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        
        builder.Services.AddKeyedSingleton<ITransportRouter>(builder.MeshId, (sp, key) =>
            new TransportRouter(sp.GetServices<ITransport>()));

        builder.Services.AddKeyedSingleton<IApplicationPayloadDispatcher>(builder.MeshId, (sp, key) =>
            new ApplicationPayloadDispatcher(
                (string)key!,
                sp.GetKeyedServices<IApplicationPayloadHandler>(key),
                sp.GetRequiredService<ILogger<ApplicationPayloadDispatcher>>()));
        
        builder.Services.TryAddSingleton<IP2pProtocol, GossipProtocol>();

        return builder;
    }
}