namespace Ama.Enterprise.FeatureFlags.Extensions;

using System;
using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Extensions for registering feature flags CRDT components leveraging the distributed CRDT core.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the complete, plug-and-play feature flags system to the service collection,
    /// abstracting away all internal P2P cluster mesh configurations natively.
    /// </summary>
    public static IServiceCollection AddFeatureFlags(this IServiceCollection services, Action<FeatureFlagOptions>? configure = null)
    {
        if (services == null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        var ffOpts = new FeatureFlagOptions();
        configure?.Invoke(ffOpts);

        // Apply configuration mappings natively maintaining backwards compatibility
        services.Configure<FeatureFlagOptions>(configure ?? (_ => { }));

        Action<DistributedCrdtOptions> distConfig = dist =>
        {
            dist.ReplicaId = ffOpts.Crdt.ReplicaId;
            dist.ActiveSyncEnabled = ffOpts.Crdt.ActiveSyncEnabled;
            dist.CheckpointIntervalSeconds = ffOpts.Crdt.CheckpointIntervalSeconds;
            dist.AntiEntropyInitialDelaySeconds = ffOpts.Crdt.AntiEntropyInitialDelaySeconds;
            dist.AntiEntropyIntervalSeconds = ffOpts.Crdt.AntiEntropyIntervalSeconds;
            dist.PeerEvictionTtlSeconds = ffOpts.Crdt.PeerEvictionTtlSeconds;
        };

        // Bootstrap generic core dependencies
        services.AddDistributedCrdtCore(distConfig);

        // Register CRDT models specifically for feature flags domain
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(FeatureFlagsJsonContext.Default)
                .AddCrdtAotContext<FeatureFlagsCrdtAotContext>();

        services.AddCrdtSerializableType<FeatureFlag>("feature-flag");

        // Map domain generic types inside the centralized document pool securely via explicitly inherited constraints
        services.AddDistributedDocumentType<FeatureFlagState>("feature-flag");

        // Register the business logic domain wrapper scoped exactly to the CRDT hierarchy via transparent forwarder natively
        services.AddDistributedCrdtService<IFeatureFlagClusterManager, FeatureFlagClusterManager>();

        // Register the background initialization service to securely build the global scope natively
        services.AddHostedService<FeatureFlagBootstrapper>();

        // Delegate routing orchestrations and background services completely to the distributed core
        services.AddDistributedCrdtP2p(ffOpts.InternalMeshId);

        // Wire up the abstracted P2P network layer specifically for Feature Flags completely encapsulating internals seamlessly
        if (!string.IsNullOrWhiteSpace(ffOpts.InternalMeshId))
        {
            services.AddP2pMesh(ffOpts.InternalMeshId)
                    .AddGossipNetwork(options =>
                    {
                        options.GossipInterval = ffOpts.Gossip.GossipInterval;
                        options.Fanout = ffOpts.Gossip.Fanout;
                        options.DefaultTimeToLive = ffOpts.Gossip.DefaultTimeToLive;
                    })
                    .AddHttpTransport(options =>
                    {
                        options.ListenPort = ffOpts.Http.ListenPort;
                        options.ListenHost = ffOpts.Http.ListenHost;
                        options.PathPrefix = ffOpts.Http.PathPrefix;
                    })
                    .AddUdpPeerDiscovery(options =>
                    {
                        options.MulticastAddress = ffOpts.UdpDiscovery.MulticastAddress;
                        options.MulticastPort = ffOpts.UdpDiscovery.MulticastPort;
                        options.DiscoveryInterval = ffOpts.UdpDiscovery.DiscoveryInterval;
                        options.DiscoveryTimeout = ffOpts.UdpDiscovery.DiscoveryTimeout;
                    })
                    .AddWebRtcTransport(options => 
                    {
                        options.IceServers = ffOpts.WebRtc.IceServers;
                        options.IceGatheringTimeout = ffOpts.WebRtc.IceGatheringTimeout;
                    });
        }

        // Add the mesh that sends data to the admin panel, usually using WebRTC
        if (!string.IsNullOrWhiteSpace(ffOpts.AdminMeshId))
        {
            services.AddP2pMesh(ffOpts.AdminMeshId)
                    .AddGossipNetwork(options =>
                    {
                        options.GossipInterval = ffOpts.Gossip.GossipInterval;
                        options.Fanout = ffOpts.Gossip.Fanout;
                        options.DefaultTimeToLive = ffOpts.Gossip.DefaultTimeToLive;
                    })
                    .AddWebRtcTransport(options =>
                    {
                        options.IceServers = ffOpts.AdminWebRtc.IceServers;
                        options.IceGatheringTimeout = ffOpts.AdminWebRtc.IceGatheringTimeout;
                    });
        }

        return services;
    }
}