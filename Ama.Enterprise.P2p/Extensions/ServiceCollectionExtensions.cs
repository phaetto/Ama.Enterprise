namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.Licensing.Extensions;
using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Models.Algorithms;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Algorithms;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Transports;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Provides extension methods for registering generic P2P meshes.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Initiates the registration of a new P2P mesh network profile under the given identifier.
    /// </summary>
    public static IP2pMeshBuilder AddP2pMesh(
        this IServiceCollection services, 
        string meshId, 
        Action<P2pNodeOptions>? configureNodeOptions = null)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(services);
        if (!tracker.TryRegister(meshId, configureNodeOptions))
        {
            return new P2pMeshBuilder(services, meshId);
        }

        if (configureNodeOptions is null)
        {
            services.Configure<P2pNodeOptions>(meshId, _ => { });
        }
        else
        {
            services.Configure(meshId, configureNodeOptions);
        }

        services.AddAmaEnterpriseLicense();

        if (!services.Any(s => s.ImplementationType == typeof(P2pHostedService)))
        {
            services.AddHostedService<P2pHostedService>();
        }

        if (!services.Any(s => s.ImplementationType == typeof(DirectMessageSender)))
        {
            services.TryAddSingleton<IDirectMessageSender, DirectMessageSender>();
        }

        if (!services.Any(s => s.ServiceType == typeof(IJsonTypeInfoResolver) && s.ServiceKey as string == "Ama.CRDT" && s.ImplementationInstance == P2pJsonSerializerContext.Default))
        {
            services.AddKeyedSingleton<IJsonTypeInfoResolver>("Ama.CRDT", P2pJsonSerializerContext.Default);
        }

        services.TryAddSingleton<IPeerRegistry, InMemoryPeerRegistry>();

        services.TryAddKeyedSingleton<IMeshWireEncoder>(meshId, (sp, key) =>
            new MeshWireEncoder(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<WireEncoderOptions>>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<MeshWireEncoder>>()));

        services.AddKeyedSingleton<IInboundMessageQueue<IMeshMessage>>(meshId, (sp, key) =>
            new InboundMessageQueue<IMeshMessage>());

        services.AddKeyedSingleton<IPeerAuthenticator>(meshId, (sp, key) =>
            new PassThroughPeerAuthenticator(
                (string)key!, 
                sp.GetRequiredService<ILogger<PassThroughPeerAuthenticator>>()));

        services.AddKeyedSingleton<IPeerSelector>(meshId, (sp, key) =>
            new RandomPeerSelector(
                (string)key!,
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<RandomPeerSelector>>()));

        services.AddKeyedSingleton<IFailureDetector>(meshId, (sp, key) =>
            new TimeBasedFailureDetector(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<FailureDetectorOptions>>(),
                sp.GetRequiredService<ILogger<TimeBasedFailureDetector>>()));
        
        services.AddKeyedSingleton<ITransportRouter>(meshId, (sp, key) =>
            new TransportRouter(sp.GetKeyedServices<ITransport>(key)));

        services.AddKeyedSingleton<IApplicationPayloadDispatcher>(meshId, (sp, key) =>
            new ApplicationPayloadDispatcher(
                (string)key!,
                sp.GetKeyedServices<IApplicationPayloadHandler>(key),
                sp.GetRequiredService<ILogger<ApplicationPayloadDispatcher>>()));

        services.AddSingleton(new P2pMeshMetadata(meshId));

        return new P2pMeshBuilder(services, meshId);
    }

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

    /// <summary>
    /// Registers the specific Gossip network protocol orchestrators.
    /// </summary>
    public static IP2pMeshBuilder AddGossipNetwork(
        this IP2pMeshBuilder builder, 
        Action<GossipOptions>? configureOptions = null)
    {
        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<GossipOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }
        
        builder.Services.TryAddSingleton<IP2pAlgorithm, GossipAlgorithm>();

        return builder;
    }

    /// <summary>
    /// Registers the specific Push-Pull Gossip network protocol orchestrators natively handling targeted anti-entropy.
    /// </summary>
    public static IP2pMeshBuilder AddPushPullGossipNetwork(
        this IP2pMeshBuilder builder, 
        Action<PushPullGossipOptions>? configureOptions = null)
    {
        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<PushPullGossipOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }
        
        builder.Services.TryAddSingleton<IP2pAlgorithm, PushPullGossipAlgorithm>();

        return builder;
    }

    /// <summary>
    /// Configures the failure detector options explicitly allocating custom limits like HeartbeatInterval decoupled from overarching protocol constraints.
    /// </summary>
    public static IP2pMeshBuilder ConfigureFailureDetector(
        this IP2pMeshBuilder builder, 
        Action<FailureDetectorOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(configureOptions);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId, configureOptions))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configureOptions);

        return builder;
    }
}