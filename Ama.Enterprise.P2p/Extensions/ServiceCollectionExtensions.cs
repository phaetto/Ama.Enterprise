namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Linq;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.Licensing.Extensions;
using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services;
using Ama.Enterprise.P2p.Services.Core;
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

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IValidateOptions<P2pNodeOptions>, P2pNodeOptionsValidator>());

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