namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Extensions;

using System;
using Microsoft.Extensions.DependencyInjection;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Models;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Services;

/// <summary>
/// Extension methods exposing Dependency Injection routines capturing Table Storage integration models.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers distributed CRDT Table Storage mechanisms hooking into existing global state trees.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">Delegate mapping Table Storage parameters.</param>
    /// <returns>The configured service collection.</returns>
    public static IServiceCollection AddDistributedCrdtTableStorage(this IServiceCollection services, Action<TableStorageCrdtOptions> configureOptions)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (configureOptions == null) throw new ArgumentNullException(nameof(configureOptions));

        services.Configure(configureOptions);
        services.AddSingleton<IDistributedCrdtStorage, TableStorageDistributedCrdtStorage>();

        return services;
    }
}