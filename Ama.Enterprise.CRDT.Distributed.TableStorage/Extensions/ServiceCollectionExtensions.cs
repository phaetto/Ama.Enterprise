namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Extensions;

using System;
using Microsoft.Extensions.DependencyInjection;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Models;
using Ama.Enterprise.CRDT.Distributed.TableStorage.Services;

/// <summary>
/// Extension methods exposing Dependency Injection routines capturing Table Storage integration models dynamically decoupled.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers distributed CRDT Table Storage mechanisms hooking into existing global state trees acting as the primary store.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureOptions">Delegate mapping Table Storage parameters.</param>
    /// <returns>The configured service collection.</returns>
    public static IServiceCollection AddPrimaryDistributedCrdtTableStorage(this IServiceCollection services, Action<TableStorageCrdtOptions> configureOptions)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (configureOptions == null) throw new ArgumentNullException(nameof(configureOptions));

        services.Configure(configureOptions);
        services.AddKeyedSingleton<IDistributedCrdtStorage, TableStorageDistributedCrdtStorage>("primary");

        return services;
    }

    /// <summary>
    /// Registers distributed CRDT Table Storage mechanisms mapped strictly to a specific dynamic document alias natively explicitly supporting multi-database architectures safely.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="typeAlias">The explicit target CRDT document type alias to map.</param>
    /// <param name="configureOptions">Delegate mapping Table Storage parameters.</param>
    /// <returns>The configured service collection.</returns>
    public static IServiceCollection AddDistributedCrdtTableStorageForType(this IServiceCollection services, string typeAlias, Action<TableStorageCrdtOptions> configureOptions)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(typeAlias)) throw new ArgumentException("Type alias cannot be null or empty.", nameof(typeAlias));
        if (configureOptions == null) throw new ArgumentNullException(nameof(configureOptions));

        var key = $"type:{typeAlias}";
        services.Configure(configureOptions);
        services.AddKeyedSingleton<IDistributedCrdtStorage, TableStorageDistributedCrdtStorage>(key);
        services.AddSingleton(new CrdtStorageRegistration(key, typeAlias, CrdtStorageRoutingType.DocumentType));

        return services;
    }

    /// <summary>
    /// Registers distributed CRDT Table Storage mechanisms mapped strictly to a specific unique active document ID dynamically decoupled explicitly.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="documentId">The specific Document ID that will trigger this active Table Storage mapping explicitly.</param>
    /// <param name="configureOptions">Delegate mapping Table Storage parameters.</param>
    /// <returns>The configured service collection.</returns>
    public static IServiceCollection AddDistributedCrdtTableStorageForDocument(this IServiceCollection services, string documentId, Action<TableStorageCrdtOptions> configureOptions)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));
        if (configureOptions == null) throw new ArgumentNullException(nameof(configureOptions));

        var key = $"doc:{documentId}";
        services.Configure(configureOptions);
        services.AddKeyedSingleton<IDistributedCrdtStorage, TableStorageDistributedCrdtStorage>(key);
        services.AddSingleton(new CrdtStorageRegistration(key, documentId, CrdtStorageRoutingType.DocumentId));

        return services;
    }
}