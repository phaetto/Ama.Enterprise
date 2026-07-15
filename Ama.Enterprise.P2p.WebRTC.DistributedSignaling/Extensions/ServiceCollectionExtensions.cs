namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Extensions;

using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;
using Ama.Enterprise.P2p.WebRTC.Models;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;
using System.Net.Http;

/// <summary>
/// Dependency injection extensions for bootstrapping the CRDT-backed WebRTC signaling hub.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the distributed WebRTC signaling manager utilizing the underlying CRDT engine for low-latency P2P state synchronization avoiding centralized datastores.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddWebRtcDistributedSignaling(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // Map AOT bindings for CRDT reflection and JSON resolution
        services.AddSharedSerializationBindings();

        // Register the typed document model in the CRDT dependency map
        services.AddDistributedDocumentType<CrdtSignalingState>(Constants.SignalingDocumentTypeAlias);
        
        // Register the specific domain logic manager scoped directly to the CRDT hierarchy bounds
        services.AddDistributedCrdtService<ICrdtSignalingManager, CrdtSignalingManager>();

        return services;
    }

    /// <summary>
    /// Registers the distributed WebRTC signaling HTTP client for developers to interact with the decentralized topology externally avoiding structural dependencies.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configureClient">Action to configure the underlying HTTP client, such as setting the base address bound to active nodes.</param>
    /// <returns>The updated service collection.</returns>
    public static IServiceCollection AddWebRtcDistributedSignalingClient(this IServiceCollection services, Action<HttpClient> configureClient)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureClient);

        // Map AOT bindings for CRDT reflection and JSON resolution required by the client models
        services.AddSharedSerializationBindings();

        // Register the named HttpClient configuration
        services.AddHttpClient(nameof(IWebRtcDistributedSignalingClient), configureClient);
        
        // Register the client wrapper utilizing IHttpClientFactory as a Singleton natively
        services.TryAddSingleton<IWebRtcDistributedSignalingClient, WebRtcDistributedSignalingClient>();
        
        return services;
    }

    private static IServiceCollection AddSharedSerializationBindings(this IServiceCollection services)
    {
        services.AddCrdt()
                .AddCrdtJsonTypeInfoResolver(DistributedSignalingJsonContext.Default)
                .AddCrdtAotContext<DistributedSignalingCrdtAotContext>();

        services.AddCrdtSerializableType<WebRtcInvitationOffer>("webrtc-offer");
        services.AddCrdtSerializableType<WebRtcInvitationAnswer>("webrtc-answer");

        return services;
    }
}