namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Extensions;

using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Models;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Services;
using Microsoft.Extensions.DependencyInjection;
using System;

/// <summary>
/// Extension methods exposing Dependency Injection routines capturing Table Storage integration models dynamically decoupled.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers distributed CRDT Table Storage mechanism hooking into existing global state trees acting as the unified scope store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">Delegate mapping Table Storage parameters.</param>
    /// <returns>The configured service collection.</returns>
    public static IServiceCollection AddDistributedCrdtTableStorage(this IServiceCollection services, Action<TableStorageCrdtOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);
        services.AddDistributedCrdtStorage<TableStorageDistributedCrdtStorage>();

        return services;
    }
}