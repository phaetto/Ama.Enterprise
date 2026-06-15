namespace Ama.Enterprise.P2p.Extensions;

using System;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Services.Algorithms;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Provides extension methods for registering generic Push-Pull Gossip algorithm components.
/// </summary>
public static class PushPullGossipNetworkServiceCollectionExtensions
{
    /// <summary>
    /// Registers the specific Push-Pull Gossip network protocol orchestrators natively handling targeted anti-entropy.
    /// </summary>
    public static IP2pMeshBuilder AddPushPullGossipNetwork(
        this IP2pMeshBuilder builder, 
        Action<PushPullGossipOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<PushPullGossipOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }
        
        builder.Services.TryAddSingleton<IP2pAlgorithm, PushPullGossipAlgorithm>();

        return builder;
    }
}