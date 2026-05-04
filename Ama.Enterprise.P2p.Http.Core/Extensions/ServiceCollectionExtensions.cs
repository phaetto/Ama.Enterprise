namespace Ama.Enterprise.P2p.Http.Core.Extensions;

using Ama.Enterprise.P2p.Http.Core.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Extension methods for registering core HTTP P2P dependencies.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the shared HTTP inbound dispatcher to the service collection.
    /// </summary>
    /// <param name="services">The service collection to mutate.</param>
    /// <returns>The mutated service collection.</returns>
    public static IServiceCollection AddP2pHttpCore(this IServiceCollection services)
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        services.TryAddSingleton<IHttpInboundDispatcher, HttpInboundDispatcher>();

        return services;
    }
}