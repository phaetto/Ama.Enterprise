namespace Ama.Enterprise.P2p.TableStorage.Extensions;

using System;
using Microsoft.Extensions.DependencyInjection;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.TableStorage.Models;
using Ama.Enterprise.P2p.TableStorage.Services;

/// <summary>
/// Extension methods for setting up P2P Table Storage services in an <see cref="IServiceCollection" />.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the <see cref="TableStoragePeerRegistry"/> as the primary <see cref="IPeerRegistry"/> implementation.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">An action to configure the required Table Storage options.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddTableStoragePeerRegistry(
        this IServiceCollection services,
        Action<TableStorageRegistryOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);
        services.AddSingleton<IPeerRegistry, TableStoragePeerRegistry>();

        return services;
    }
}