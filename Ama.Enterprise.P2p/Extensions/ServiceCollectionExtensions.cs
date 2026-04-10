namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Linq;
using System.Net.Http;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Ama.Enterprise.P2p.Services.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Provides extension methods for registering P2P gossip components in the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Initiates the registration of a new P2P mesh network profile under the given identifier.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="meshId">The unique identifier defining this mesh network instance.</param>
    /// <param name="configureNodeOptions">An action to configure the core node options like identity and endpoint for this mesh.</param>
    /// <returns>A builder to chain protocol and discovery configuration actions.</returns>
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

        services.AddHostedService<P2pHostedService>();
        services.AddSingleton(new P2pMeshMetadata(meshId));

        return new P2pMeshBuilder(services, meshId);
    }

    /// <summary>
    /// Registers the core HTTP transport mechanisms mapped flexibly across generalized endpoints securely.
    /// </summary>
    /// <typeparam name="TMessage">The primary network message type resolving throughout the local scope correctly.</typeparam>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action specifying HttpTransport overrides naturally explicitly.</param>
    /// <returns>The updated mesh builder.</returns>
    public static IP2pMeshBuilder AddHttpTransport<TMessage>(
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

        builder.Services.AddKeyedSingleton<ITransport<TMessage>>(builder.MeshId, (sp, key) =>
            new HttpTransport<TMessage>(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<HttpTransportOptions>>(),
                sp.GetRequiredService<IHttpClientFactory>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<HttpTransport<TMessage>>>()));

        builder.Services.AddKeyedSingleton<ITransportListener<TMessage>>(builder.MeshId, (sp, key) =>
            new HttpTransportListener<TMessage>(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<HttpTransportOptions>>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<HttpTransportListener<TMessage>>>()));

        return builder;
    }

    /// <summary>
    /// Registers the core interfaces required for the P2P Gossip protocol under the current mesh context.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the gossip options for this mesh.</param>
    /// <returns>The updated mesh builder.</returns>
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
        
        builder.Services.AddKeyedSingleton<ITransportRouter<GossipMessage>>(builder.MeshId, (sp, key) =>
            new TransportRouter<GossipMessage>(sp.GetKeyedServices<ITransport<GossipMessage>>(key)));

        builder.Services.AddKeyedSingleton<IMessageDispatcher<GossipMessage>>(builder.MeshId, (sp, key) =>
            new MessageDispatcher<GossipMessage>(
                (string)key!,
                sp.GetKeyedServices<IMessageHandler<GossipMessage>>(key),
                sp.GetRequiredService<ILogger<MessageDispatcher<GossipMessage>>>()));
        
        builder.Services.TryAddSingleton<IP2pProtocol, GossipProtocol>();

        return builder;
    }
}