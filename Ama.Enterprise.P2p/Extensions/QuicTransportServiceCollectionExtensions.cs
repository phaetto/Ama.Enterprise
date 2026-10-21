namespace Ama.Enterprise.P2p.Extensions;

using System;
using Ama.CRDT.Services.Pooling;
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

        var meshId = CrdtStringPool.Shared.GetOrAdd(builder.MeshId);
        Action<QuicTransportOptions> configAction = options => 
        {
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(CrdtStringPool.Shared.GetOrAdd($"{meshId}_QuicTransport"), configAction))
        {
            return builder;
        }

        builder.Services.Configure(meshId, configAction);

        builder.Services.AddKeyedSingleton<PeerEndpoint>(meshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<QuicTransportOptions>>().Get((string)key!);
            var host = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? "localhost" 
                : options.ListenHost;
                
            return new QuicPeerEndpoint(CrdtStringPool.Shared.GetOrAdd(host), options.ListenPort);
        });

        builder.Services.AddKeyedSingleton<ITransport>(meshId, (sp, key) =>
            new QuicTransport(
                CrdtStringPool.Shared.GetOrAdd((string)key!),
                sp.GetRequiredService<IOptionsMonitor<QuicTransportOptions>>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<QuicTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(meshId, (sp, key) =>
            new QuicTransportListener(
                CrdtStringPool.Shared.GetOrAdd((string)key!),
                sp.GetRequiredService<IOptionsMonitor<QuicTransportOptions>>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<ILogger<QuicTransportListener>>()));

        return builder;
    }
}