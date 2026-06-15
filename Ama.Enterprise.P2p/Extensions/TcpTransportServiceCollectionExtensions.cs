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
/// Provides extension methods for registering TCP transports dynamically mapped to multi-mesh pipelines.
/// </summary>
public static class TcpTransportServiceCollectionExtensions
{
    /// <summary>
    /// Registers the pure TCP transport actively mapping standalone decoupled capabilities locally natively.
    /// </summary>
    public static IP2pMeshBuilder AddTcpTransport(
        this IP2pMeshBuilder builder,
        Action<TcpTransportOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        Action<TcpTransportOptions> configAction = options => 
        {
            options.IsEnabled = true;
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister($"{builder.MeshId}_TcpTransport", configAction))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configAction);

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<TcpTransportOptions>>().Get((string)key!);
            var host = string.Equals(options.ListenHost, "+", StringComparison.OrdinalIgnoreCase) 
                ? "localhost" 
                : options.ListenHost;
                
            return new TcpPeerEndpoint(host, options.ListenPort);
        });

        builder.Services.AddKeyedSingleton<ITransport>(builder.MeshId, (sp, key) =>
            new TcpTransport(
                (string)key!,
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<TcpTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(builder.MeshId, (sp, key) =>
            new TcpTransportListener(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<TcpTransportOptions>>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<ILogger<TcpTransportListener>>()));

        return builder;
    }
}