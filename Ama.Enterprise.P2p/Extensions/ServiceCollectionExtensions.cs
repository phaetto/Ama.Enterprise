namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Text.Json.Serialization.Metadata;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Gossip;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

/// <summary>
/// Provides extension methods for registering P2P gossip components in the dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the core interfaces and options required for the P2P Gossip protocol.
    /// </summary>
    /// <param name="services">The service collection to add the registrations to.</param>
    /// <param name="configureOptions">An action to configure the gossip options.</param>
    /// <returns>The updated service collection.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="services"/> is null.</exception>
    public static IServiceCollection AddP2pGossipNetwork(
        this IServiceCollection services, 
        Action<GossipOptions>? configureOptions = null)
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (configureOptions is null)
        {
            services.Configure<GossipOptions>(_ => { });
        }
        else
        {
            services.Configure(configureOptions);
        }

        // Map GossipInterval down to the new generic failure detector
        services.AddOptions<FailureDetectorOptions>()
            .Configure<IOptions<GossipOptions>>((failureOptions, gossipOptions) =>
            {
                if (gossipOptions?.Value is not null)
                {
                    failureOptions.HeartbeatInterval = gossipOptions.Value.GossipInterval;
                }
            });

        // Register HttpClient specific to the P2P transport
        services.AddHttpClient("P2pTransport");

        // Register P2P JSON Context to be combined by Ama.CRDT options context
        services.AddKeyedSingleton<IJsonTypeInfoResolver>("Ama.CRDT", P2pJsonSerializerContext.Default);

        // Core P2P services
        services.TryAddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.TryAddSingleton<IPeerAuthenticator, PassThroughPeerAuthenticator>();
        services.TryAddSingleton<IPeerSelector, RandomPeerSelector>();
        services.TryAddSingleton<IFailureDetector, TimeBasedFailureDetector>();
        
        // Transport and Dispatcher
        services.TryAddSingleton<ITransport<GossipMessage>, HttpTransport>();
        services.TryAddSingleton<ITransportListener<GossipMessage>, HttpTransportListener>();
        services.TryAddSingleton<IMessageDispatcher<GossipMessage>, MessageDispatcher<GossipMessage>>();
        
        // Protocol orchestrator mapped to the generic IP2pProtocol interface
        services.TryAddSingleton<IP2pProtocol, GossipProtocol>();
        
        // Hosted service to manage background lifecycle within the generic host
        services.AddHostedService<P2pHostedService>();

        return services;
    }
}