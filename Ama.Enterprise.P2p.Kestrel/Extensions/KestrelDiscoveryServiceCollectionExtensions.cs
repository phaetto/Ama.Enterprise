namespace Ama.Enterprise.P2p.Kestrel.Extensions;

using System;
using System.Net.Http;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Kestrel.Models;
using Ama.Enterprise.P2p.Kestrel.Services.Discovery;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering Kestrel peer handshaker components tied to a specific mesh profile.
/// </summary>
public static class KestrelDiscoveryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Phase 2 isolated Kestrel handshaker implementation under the current mesh context.
    /// </summary>
    /// <param name="builder">The mesh builder instance.</param>
    /// <param name="configureOptions">An action to configure the Kestrel handshake options.</param>
    /// <returns>The updated mesh builder.</returns>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public static IP2pMeshBuilder AddKestrelPeerHandshake(
        this IP2pMeshBuilder builder,
        Action<KestrelHandshakeOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_KestrelHandshaker", configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        ServiceCollectionExtensions.TryAddKestrelSerialization(builder.Services);

        builder.Services.AddHttpClient("P2pKestrelHandshaker");

        builder.Services.AddKeyedSingleton<IPeerHandshaker>(builder.MeshId, (sp, key) =>
            new KestrelPeerHandshaker(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<KestrelHandshakeOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredService<IHttpClientFactory>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<KestrelPeerHandshaker>>()));

        return builder;
    }
}