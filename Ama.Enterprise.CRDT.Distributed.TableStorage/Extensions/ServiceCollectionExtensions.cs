namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Extensions;

using System;
using Microsoft.Extensions.DependencyInjection;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Models;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Services;

/// <summary>
/// Extension methods exposing Dependency Injection routines capturing Table Storage integration models.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers distributed CRDT Table Storage mechanisms hooking into existing global state trees natively acting as the primary store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">Delegate mapping Table Storage parameters.</param>
    /// <returns>The configured service collection.</returns>
    public static IServiceCollection AddDistributedCrdtTableStorage(this IServiceCollection services, Action<TableStorageCrdtOptions> configureOptions)
    {
        return services.AddDistributedCrdtTableStorage("primary", configureOptions);
    }

    /// <summary>
    /// Registers distributed CRDT Table Storage mechanisms mapped strictly to a specific dynamic document alias natively explicitly supporting multi-database architectures safely gracefully successfully.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="storageKey">The keyed routing identifier mapping directly back to the active Document TypeAlias.</param>
    /// <param name="configureOptions">Delegate mapping Table Storage parameters.</param>
    /// <returns>The configured service collection.</returns>
    public static IServiceCollection AddDistributedCrdtTableStorage(this IServiceCollection services, string storageKey, Action<TableStorageCrdtOptions> configureOptions)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(storageKey)) throw new ArgumentException("Storage key cannot be null or empty.", nameof(storageKey));
        if (configureOptions == null) throw new ArgumentNullException(nameof(configureOptions));

        services.Configure(configureOptions);
        services.AddKeyedSingleton<IDistributedCrdtStorage, TableStorageDistributedCrdtStorage>(storageKey);

        if (storageKey != "primary")
        {
            services.AddSingleton(new CrdtStorageRegistration(storageKey));
        }

        return services;
    }
}