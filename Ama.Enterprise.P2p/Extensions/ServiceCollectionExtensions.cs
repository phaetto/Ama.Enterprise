using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Ama.Enterprise.P2p.Extensions;

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

        // Register HttpClient specific to the P2P transport
        services.AddHttpClient("P2pTransport");

        // Use TryAddSingleton so consumers can override default implementations if needed.
        
        // Serialization
        services.TryAddSingleton<IGossipSerializer, SystemTextJsonGossipSerializer>();

        // Core P2P services
        services.TryAddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        services.TryAddSingleton<IPeerAuthenticator, PassThroughPeerAuthenticator>();
        services.TryAddSingleton<IPeerSelector, RandomPeerSelector>();
        services.TryAddSingleton<IFailureDetector, TimeBasedFailureDetector>();
        
        // Transport and Dispatcher
        services.TryAddSingleton<ITransport, HttpTransport>();
        services.TryAddSingleton<ITransportListener, HttpTransportListener>();
        services.TryAddSingleton<IMessageDispatcher, MessageDispatcher>();
        
        // Protocol orchestrator
        services.TryAddSingleton<IGossipProtocol, GossipProtocol>();
        
        // Hosted service to manage background lifecycle within the generic host
        services.AddHostedService<P2pHostedService>();

        return services;
    }
}