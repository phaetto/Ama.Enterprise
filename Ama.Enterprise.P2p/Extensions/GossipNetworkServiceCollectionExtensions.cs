namespace Ama.Enterprise.P2p.Extensions;

using System;
using Ama.CRDT.Services.Pooling;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Services.Algorithms;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Provides extension methods for registering generic Gossip algorithm components.
/// </summary>
public static class GossipNetworkServiceCollectionExtensions
{
    /// <summary>
    /// Registers the specific Gossip network protocol orchestrators.
    /// </summary>
    public static IP2pMeshBuilder AddGossipNetwork(
        this IP2pMeshBuilder builder, 
        Action<GossipOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var meshId = CrdtStringPool.Shared.GetOrAdd(builder.MeshId);
        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(meshId, configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<GossipOptions>(meshId, _ => { });
        }
        else
        {
            builder.Services.Configure(meshId, configureOptions);
        }
        
        builder.Services.TryAddSingleton<IP2pAlgorithm, GossipAlgorithm>();

        return builder;
    }
}