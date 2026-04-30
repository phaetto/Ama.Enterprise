namespace Ama.Enterprise.P2p.Extensions;

using System;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Discovery;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering DNS-based peer discovery components tied to a specific mesh profile.
/// </summary>
public static class DnsDiscoveryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the DNS peer discovery services and options under the current mesh context.
    /// This extension requires the mesh to register a valid <see cref="IPeerHandshaker"/> Keyed service implementation to operate.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the DNS discovery options.</param>
    /// <returns>The updated mesh builder.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IP2pMeshBuilder AddDnsPeerDiscovery(
        this IP2pMeshBuilder builder,
        Action<DnsDiscoveryOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        builder.Services.AddKeyedSingleton<IPeerDiscovery>(builder.MeshId, (sp, key) =>
            new DnsPeerDiscovery(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<DnsDiscoveryOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredKeyedService<IPeerHandshaker>(key),
                sp.GetRequiredService<ILogger<DnsPeerDiscovery>>(),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredKeyedService<IPeerAuthenticator>(key),
                sp.GetRequiredKeyedService<IFailureDetector>(key)));

        return builder;
    }
}