namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Discovery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering UDP multicast peer discovery components tied to a specific mesh profile.
/// </summary>
public static class UdpDiscoveryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the UDP peer discovery services and options under the current mesh context.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the UDP discovery options.</param>
    /// <returns>The updated mesh builder.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IP2pMeshBuilder AddUdpPeerDiscovery(
        this IP2pMeshBuilder builder,
        Action<UdpDiscoveryOptions> configureOptions)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configureOptions is null)
        {
            throw new ArgumentNullException(nameof(configureOptions));
        }

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        if (!builder.Services.Any(s => s.ServiceType == typeof(IJsonTypeInfoResolver) && s.ServiceKey as string == "Ama.CRDT" && s.ImplementationInstance == UdpDiscoveryJsonContext.Default))
        {
            builder.Services.AddKeyedSingleton<IJsonTypeInfoResolver>("Ama.CRDT", UdpDiscoveryJsonContext.Default);
        }

        builder.Services.AddKeyedSingleton<IPeerDiscovery>(builder.MeshId, (sp, key) =>
            new UdpPeerDiscovery(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<UdpDiscoveryOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredService<ILogger<UdpPeerDiscovery>>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ICrdtSerializer>()));

        return builder;
    }
}