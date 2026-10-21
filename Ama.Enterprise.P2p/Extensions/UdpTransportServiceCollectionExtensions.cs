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
/// Provides extension methods for registering robust UDP datagram transports decoupled to multi-mesh pipelines.
/// </summary>
public static class UdpTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers the robust datagram UDP transport structuring standalone explicitly natively decoupled multi-mesh pipelines.
    /// </summary>
    public static IP2pMeshBuilder AddUdpTransport(
        this IP2pMeshBuilder builder,
        Action<UdpTransportOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var meshId = CrdtStringPool.Shared.GetOrAdd(builder.MeshId);
        Action<UdpTransportOptions> configAction = options => 
        {
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(CrdtStringPool.Shared.GetOrAdd($"{meshId}_UdpTransport"), configAction))
        {
            return builder;
        }

        builder.Services.Configure(meshId, configAction);

        builder.Services.AddKeyedSingleton<PeerEndpoint>(meshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<UdpTransportOptions>>().Get((string)key!);
            var host = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? "localhost" 
                : options.ListenHost;
                
            return new UdpPeerEndpoint(CrdtStringPool.Shared.GetOrAdd(host), options.ListenPort);
        });

        builder.Services.AddKeyedSingleton<ITransport>(meshId, (sp, key) =>
            new UdpTransport(
                CrdtStringPool.Shared.GetOrAdd((string)key!),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<UdpTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(meshId, (sp, key) =>
            new UdpTransportListener(
                CrdtStringPool.Shared.GetOrAdd((string)key!),
                sp.GetRequiredService<IOptionsMonitor<UdpTransportOptions>>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<ILogger<UdpTransportListener>>()));

        return builder;
    }
}