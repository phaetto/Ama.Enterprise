using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.DependencyInjection;

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

        // Note: Implementations should be added by the consumer or further down the pipeline.
        // This ensures the Host can resolve the interfaces if they are injected.
        // Example:
        // services.AddSingleton<IGossipProtocol, GossipProtocolImpl>();
        // services.AddSingleton<IPeerRegistry, InMemoryPeerRegistry>();
        
        return services;
    }
}