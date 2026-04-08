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

        // Register the overarching hosted service orchestrator only once.
        // AddHostedService uses TryAddEnumerable internally, so it is safe and idempotent to call multiple times.
        services.AddHostedService<P2pHostedService>();

        // Register the metadata so the hosted service knows which meshes to boot.
        services.AddSingleton(new P2pMeshMetadata(meshId));

        return new P2pMeshBuilder(services, meshId);
    }

    /// <summary>
    /// Registers the core interfaces and HTTP Transport required for the P2P Gossip protocol under the current mesh context.
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

        // Map GossipInterval down to the named generic failure detector options
        builder.Services.AddOptions<FailureDetectorOptions>(builder.MeshId)
            .Configure<IOptionsMonitor<GossipOptions>>((failureOptions, gossipOptionsMonitor) =>
            {
                var gossipOptions = gossipOptionsMonitor.Get(builder.MeshId);
                if (gossipOptions is not null)
                {
                    failureOptions.HeartbeatInterval = gossipOptions.GossipInterval;
                }
            });

        // Register global HttpClient if not already present. It is idempotent.
        builder.Services.AddHttpClient("P2pTransport");

        // Conditionally register P2P JSON Context to avoid duplicate generic enumerations for System.Text.Json context merging
        if (!builder.Services.Any(s => s.ServiceType == typeof(IJsonTypeInfoResolver) && s.ServiceKey as string == "Ama.CRDT" && s.ImplementationInstance == P2pJsonSerializerContext.Default))
        {
            builder.Services.AddKeyedSingleton<IJsonTypeInfoResolver>("Ama.CRDT", P2pJsonSerializerContext.Default);
        }

        // Internal inbound queuing for isolating protocol and IO bounds
        builder.Services.AddKeyedSingleton<IInboundMessageQueue<GossipMessage>>(builder.MeshId, (sp, key) =>
            new InboundMessageQueue<GossipMessage>());

        // The Peer Registry is now a global Singleton cross-mesh to ensure node awareness regardless of where peers were discovered.
        builder.Services.TryAddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        builder.Services.AddKeyedSingleton<IPeerAuthenticator>(builder.MeshId, (sp, key) =>
            new PassThroughPeerAuthenticator(
                (string)key!, 
                sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        // Peer Selector is still Keyed per mesh, but injects the global un-keyed IPeerRegistry.
        builder.Services.AddKeyedSingleton<IPeerSelector>(builder.MeshId, (sp, key) =>
            new RandomPeerSelector(
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<RandomPeerSelector>>()));

        builder.Services.AddKeyedSingleton<IFailureDetector>(builder.MeshId, (sp, key) =>
            new TimeBasedFailureDetector(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(),
                sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        
        // Transports, Listeners, and Routing isolated per MeshId
        builder.Services.AddKeyedSingleton<ITransport<GossipMessage>>(builder.MeshId, (sp, key) =>
            new HttpTransport(
                (string)key!,
                sp.GetRequiredService<IHttpClientFactory>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<HttpTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportRouter<GossipMessage>>(builder.MeshId, (sp, key) =>
            new TransportRouter<GossipMessage>(sp.GetKeyedServices<ITransport<GossipMessage>>(key)));

        builder.Services.AddKeyedSingleton<ITransportListener<GossipMessage>>(builder.MeshId, (sp, key) =>
            new HttpTransportListener(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<GossipOptions>>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<HttpTransportListener>>()));

        builder.Services.AddKeyedSingleton<IMessageDispatcher<GossipMessage>>(builder.MeshId, (sp, key) =>
            new MessageDispatcher<GossipMessage>(
                (string)key!,
                sp.GetKeyedServices<IMessageHandler<GossipMessage>>(key),
                sp.GetRequiredService<ILogger<MessageDispatcher<GossipMessage>>>()));
        
        // Protocol orchestrator mapped globally as a single instance executing across all configured meshes
        builder.Services.TryAddSingleton<IP2pProtocol, GossipProtocol>();

        return builder;
    }
}