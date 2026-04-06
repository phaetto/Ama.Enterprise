namespace Ama.Enterprise.P2p.Extensions;

using System;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Extension methods for registering UDP multicast peer discovery components.
/// </summary>
public static class UdpDiscoveryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the UDP peer discovery services and options.
    /// </summary>
    /// <param name="services">The service collection to add the registrations to.</param>
    /// <param name="configureOptions">An action to configure the UDP discovery options.</param>
    /// <returns>The updated service collection.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IServiceCollection AddUdpPeerDiscovery(
        this IServiceCollection services,
        Action<UdpDiscoveryOptions> configureOptions)
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (configureOptions is null)
        {
            throw new ArgumentNullException(nameof(configureOptions));
        }

        services.Configure(configureOptions);

        // Register the singleton instance so it can be retrieved as both the discovery interface and the hosted service.
        services.TryAddSingleton<UdpPeerDiscovery>();
        
        services.TryAddSingleton<IPeerDiscovery>(serviceProvider => 
            serviceProvider.GetRequiredService<UdpPeerDiscovery>());
            
        services.AddHostedService(serviceProvider => 
            serviceProvider.GetRequiredService<UdpPeerDiscovery>());

        return services;
    }
}