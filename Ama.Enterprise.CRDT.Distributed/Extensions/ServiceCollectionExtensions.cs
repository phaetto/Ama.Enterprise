namespace Ama.Enterprise.CRDT.Distributed.Extensions;

using System;
using Ama.CRDT.Extensions;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Decorators;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.P2p;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extension methods for registering completely generic distributed CRDT state logic.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Bootstraps the baseline services, scope providers, and journaling decorators required by distributed CRDT modules.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtCore(this IServiceCollection services, Action<DistributedCrdtOptions>? configure = null)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));

        if (configure != null)
        {
            services.Configure(configure);
        }
        else
        {
            services.Configure<DistributedCrdtOptions>(_ => { });
        }

        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(DistributedCrdtP2pJsonContext.Default);

        services.AddCrdtJournaling<MemoryJournal>();

        services.AddCrdtApplicatorDecorator<JournalingApplicatorDecorator>(DecoratorBehavior.After);
        services.AddCrdtPatcherDecorator<JournalingPatcherDecorator>(DecoratorBehavior.After);
        services.AddCrdtApplicatorDecorator<CompactingApplicatorDecorator>(DecoratorBehavior.After);

        services.AddSingleton<DistributedCrdtScopeProvider>();

        return services;
    }

    /// <summary>
    /// Registers a specific application domain model as a universally distributed CRDT document handled within the pipeline.
    /// </summary>
    public static IServiceCollection AddDistributedDocument<TState>(this IServiceCollection services, string documentId) where TState : class, new()
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        // Register the typed document resolver within the generic scope
        services.AddScoped<IDistributedCrdtDocument<TState>>(sp =>
        {
            return ActivatorUtilities.CreateInstance<DistributedCrdtDocument<TState>>(sp, documentId);
        });

        // Forward the specific registration mapping to the generic iterable pool used by backend processors
        services.AddScoped<IDistributedCrdtDocument>(sp => sp.GetRequiredService<IDistributedCrdtDocument<TState>>());

        return services;
    }

    /// <summary>
    /// Injects the generic CRDT routing dispatchers and anti-entropy background processors directly into the targeted P2P Mesh pipeline.
    /// </summary>
    public static IServiceCollection AddDistributedCrdtP2p(this IServiceCollection services, string meshId)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        if (string.IsNullOrWhiteSpace(meshId)) throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));

        services.AddKeyedSingleton<IMessageHandler<GossipMessage>, CrdtGossipHandler>(meshId);
        services.AddSingleton<IPeerTopologyObserver, CrdtTopologyObserver>();
        services.AddHostedService<CrdtAntiEntropyService>();

        return services;
    }
}