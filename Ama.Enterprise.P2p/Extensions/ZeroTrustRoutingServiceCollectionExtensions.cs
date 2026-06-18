namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Provides dependency injection extension methods to configure zero-trust routing rules for the P2P mesh.
/// </summary>
/// <example>
/// <code>
/// builder.Services.AddP2pMesh("EnterpriseMesh")
///     .EnableZeroTrustRouting()
///     .AddRoutingPolicy&lt;AdminOnlyRoutingPolicy&gt;();
/// </code>
/// </example>
public static class ZeroTrustRoutingServiceCollectionExtensions
{
    /// <summary>
    /// Enables zero-trust routing for the mesh. This wraps the standard message dispatchers and routers with policy-enforcing decorators.
    /// </summary>
    /// <param name="builder">The dependency injection builder for the P2P mesh.</param>
    /// <returns>The modified mesh builder to allow method chaining.</returns>
    public static IP2pMeshBuilder EnableZeroTrustRouting(this IP2pMeshBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var meshId = builder.MeshId;
        var services = builder.Services;

        var routerDescriptor = services.FirstOrDefault(s => s.ServiceType == typeof(ITransportRouter) && Equals(s.ServiceKey, meshId));
        if (routerDescriptor is not null && routerDescriptor.ImplementationType != typeof(PolicyEnforcingTransportRouter))
        {
            services.Remove(routerDescriptor);
            services.AddKeyedSingleton<ITransportRouter>(meshId, (sp, key) => 
            {
                var inner = new TransportRouter(sp.GetKeyedServices<ITransport>(key));
                return new PolicyEnforcingTransportRouter(
                    (string)key!,
                    inner,
                    sp.GetKeyedServices<IMeshRoutingPolicy>(key),
                    sp.GetRequiredService<IPeerRegistry>(),
                    sp.GetRequiredService<IPeerSessionRegistry>(),
                    sp.GetRequiredService<IOptionsMonitor<SessionAuthenticatorOptions>>(),
                    sp.GetRequiredService<ILogger<PolicyEnforcingTransportRouter>>());
            });
        }

        var dispatcherDescriptor = services.FirstOrDefault(s => s.ServiceType == typeof(IApplicationPayloadDispatcher) && Equals(s.ServiceKey, meshId));
        if (dispatcherDescriptor is not null && dispatcherDescriptor.ImplementationType != typeof(PolicyEnforcingPayloadDispatcher))
        {
            services.Remove(dispatcherDescriptor);
            services.AddKeyedSingleton<IApplicationPayloadDispatcher>(meshId, (sp, key) =>
            {
                var inner = new ApplicationPayloadDispatcher(
                    (string)key!,
                    sp.GetKeyedServices<IApplicationPayloadHandler>(key),
                    sp.GetRequiredService<ILogger<ApplicationPayloadDispatcher>>());
                    
                return new PolicyEnforcingPayloadDispatcher(
                    (string)key!,
                    inner,
                    sp.GetKeyedServices<IMeshRoutingPolicy>(key),
                    sp.GetRequiredService<IPeerSessionRegistry>(),
                    sp.GetRequiredService<IOptionsMonitor<SessionAuthenticatorOptions>>(),
                    sp.GetRequiredService<ILogger<PolicyEnforcingPayloadDispatcher>>());
            });
        }

        return builder;
    }

    /// <summary>
    /// Adds a specific routing policy evaluator to the decentralized mesh.
    /// </summary>
    /// <typeparam name="TPolicy">The type of the routing policy to add.</typeparam>
    /// <param name="builder">The dependency injection builder for the P2P mesh.</param>
    /// <returns>The modified mesh builder to allow method chaining.</returns>
    public static IP2pMeshBuilder AddRoutingPolicy<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TPolicy>(this IP2pMeshBuilder builder)
        where TPolicy : class, IMeshRoutingPolicy
    {
        ArgumentNullException.ThrowIfNull(builder);
        
        builder.Services.AddKeyedSingleton<IMeshRoutingPolicy, TPolicy>(builder.MeshId);
        
        return builder;
    }
}