namespace Ama.Enterprise.P2p.AspNetCore.Extensions;

using System;
using System.Net.Http;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.AspNetCore.Services.Discovery;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering ASP.NET Core peer handshaker components decoupled securely explicitly routing meshes natively.
/// </summary>
public static class AspNetCoreDiscoveryServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Phase 2 isolated ASP.NET Core handshaker implementation under the explicit current targeted generic mesh context safely natively.
    /// </summary>
    /// <param name="builder">The mesh builder tracking decoupled topology configurations natively.</param>
    /// <param name="configureOptions">An explicit action strictly mutating standard handshaker bounds internally.</param>
    /// <returns>The unified structurally resilient multi-mesh dependency tracker cleanly resolving chained assignments natively.</returns>
    /// <exception cref="ArgumentNullException">Thrown if evaluating bounded parameters explicitly returns strictly null natively.</exception>
    public static IP2pMeshBuilder AddAspNetCorePeerHandshake(
        this IP2pMeshBuilder builder,
        Action<AspNetCoreHandshakeOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_AspNetCoreHandshaker", configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        ServiceCollectionExtensions.TryAddAspNetCoreSerialization(builder.Services);

        builder.Services.AddHttpClient("P2pAspNetCoreHandshaker");

        builder.Services.AddKeyedSingleton<IPeerHandshaker>(builder.MeshId, (sp, key) =>
            new AspNetCorePeerHandshaker(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<AspNetCoreHandshakeOptions>>(),
                sp.GetRequiredService<IOptionsMonitor<P2pNodeOptions>>(),
                sp.GetRequiredKeyedService<PeerEndpoint>(key),
                sp.GetRequiredService<IHttpClientFactory>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<AspNetCorePeerHandshaker>>()));

        return builder;
    }
}