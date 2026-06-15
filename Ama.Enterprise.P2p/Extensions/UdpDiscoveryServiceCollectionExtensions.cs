namespace Ama.Enterprise.P2p.Extensions;

using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Discovery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Diagnostics.Metrics;

/// <summary>
/// Extension methods for registering UDP multicast peer discovery components tied to a specific mesh profile.
/// </summary>
public static class UdpDiscoveryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Phase 1 UDP peer discovery services and options under the current mesh context.
    /// This extension requires the mesh to register a valid <see cref="IPeerHandshaker"/> Keyed service implementation to operate.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the UDP discovery options.</param>
    /// <returns>The updated mesh builder.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IP2pMeshBuilder AddUdpPeerDiscovery(
        this IP2pMeshBuilder builder,
        Action<UdpDiscoveryOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_UdpDiscovery", configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        builder.Services.AddKeyedSingleton<IPeerDiscovery>(builder.MeshId, (sp, key) =>
            new UdpPeerDiscovery(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<UdpDiscoveryOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredKeyedService<IPeerHandshaker>(key),
                sp.GetRequiredService<ILogger<UdpPeerDiscovery>>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredKeyedService<IPeerAuthenticator>(key),
                sp.GetRequiredKeyedService<IFailureDetector>(key),
                sp.GetService<IMeterFactory>()));

        return builder;
    }

    /// <summary>
    /// Registers the Phase 2 isolated UDP handshaker implementation under the current mesh context.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the UDP handshake options.</param>
    /// <returns>The updated mesh builder.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IP2pMeshBuilder AddUdpPeerHandshake(
        this IP2pMeshBuilder builder,
        Action<UdpHandshakeOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_UdpHandshaker", configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        builder.Services.AddKeyedSingleton<IPeerHandshaker>(builder.MeshId, (sp, key) =>
            new UdpPeerHandshaker(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<UdpHandshakeOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<UdpPeerHandshaker>>(),
                sp.GetRequiredKeyedService<IPeerAuthenticator>(key),
                sp.GetService<IMeterFactory>()));

        return builder;
    }
}