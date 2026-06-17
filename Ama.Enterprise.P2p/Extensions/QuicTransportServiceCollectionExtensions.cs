namespace Ama.Enterprise.P2p.Extensions;

using System;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Provides extension methods for registering QUIC TLS 1.3 multiplexed transports into the P2P mesh network.
/// </summary>
public static class QuicTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers the highly performant multiplexed QUIC TLS 1.3 transport structuring explicit decoupled capabilities locally natively.
    /// </summary>
    public static IP2pMeshBuilder AddQuicTransport(
        this IP2pMeshBuilder builder,
        Action<QuicTransportOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        Action<QuicTransportOptions> configAction = options => 
        {
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister($"{builder.MeshId}_QuicTransport", configAction))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configAction);

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<QuicTransportOptions>>().Get((string)key!);
            var host = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? "localhost" 
                : options.ListenHost;
                
            return new QuicPeerEndpoint(host, options.ListenPort);
        });

        builder.Services.AddKeyedSingleton<ITransport>(builder.MeshId, (sp, key) =>
            new QuicTransport(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<QuicTransportOptions>>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<QuicTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(builder.MeshId, (sp, key) =>
            new QuicTransportListener(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<QuicTransportOptions>>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<ILogger<QuicTransportListener>>()));

        return builder;
    }
}