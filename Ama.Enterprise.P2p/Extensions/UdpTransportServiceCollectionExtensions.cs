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

        Action<UdpTransportOptions> configAction = options => 
        {
            options.IsEnabled = true;
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister($"{builder.MeshId}_UdpTransport", configAction))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configAction);

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<UdpTransportOptions>>().Get((string)key!);
            var host = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? "localhost" 
                : options.ListenHost;
                
            return new UdpPeerEndpoint(host, options.ListenPort);
        });

        builder.Services.AddKeyedSingleton<ITransport>(builder.MeshId, (sp, key) =>
            new UdpTransport(
                (string)key!,
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<UdpTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(builder.MeshId, (sp, key) =>
            new UdpTransportListener(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<UdpTransportOptions>>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<ILogger<UdpTransportListener>>()));

        return builder;
    }
}