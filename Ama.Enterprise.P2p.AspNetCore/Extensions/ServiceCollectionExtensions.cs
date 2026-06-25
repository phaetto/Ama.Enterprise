namespace Ama.Enterprise.P2p.AspNetCore.Extensions;

using System;
using System.Linq;
using System.Net.Http;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Ama.CRDT.Extensions;
using Ama.Enterprise.Licensing.Services;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.AspNetCore.Services;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods configuring standard isolated dependencies natively mapping internal shared boundaries guaranteeing valid ASP.NET architectures decoupled securely.
/// </summary>
public static class ServiceCollectionExtensions
{
    private sealed record AspNetCoreSerializationMarker;

    internal static void TryAddAspNetCoreSerialization(IServiceCollection services)
    {
        var tracker = P2pMeshRegistrationTracker.GetOrCreate(services);
        if (!tracker.TryRegister<AspNetCoreSerializationMarker>("AspNetCoreSerialization_Core", null))
        {
            return;
        }

        services.AddCrdtJsonTypeInfoResolver(AspNetCoreJsonContext.Default);

        services.AddCrdtSerializableType<AspNetCorePeerEndpoint>("aspnetcore-peer-endpoint");

        services.AddCrdtJsonModifier(ti =>
        {
            if (ti.Type == typeof(PeerEndpoint))
            {
                ti.PolymorphismOptions ??= new JsonPolymorphismOptions
                {
                    TypeDiscriminatorPropertyName = "$type",
                    IgnoreUnrecognizedTypeDiscriminators = true,
                    UnknownDerivedTypeHandling = JsonUnknownDerivedTypeHandling.FallBackToBaseType
                };

                if (!ti.PolymorphismOptions.DerivedTypes.Any(dt => dt.DerivedType == typeof(AspNetCorePeerEndpoint)))
                {
                    ti.PolymorphismOptions.DerivedTypes.Add(new JsonDerivedType(typeof(AspNetCorePeerEndpoint), "aspnetcore-peer-endpoint"));
                }
            }
        });
    }

    internal static IServiceCollection AddP2pHttpCore(this IServiceCollection services)
    {
        services.TryAddSingleton<IHttpInboundDispatcher, HttpInboundDispatcher>();
        return services;
    }

    /// <summary>
    /// Registers tracking explicit decoupled inbound handlers abstracting internal dependencies mapping standard underlying mesh generic environments securely.
    /// </summary>
    public static IP2pMeshBuilder AddAspNetCoreTransport(
        this IP2pMeshBuilder builder,
        Action<AspNetCoreTransportOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        Action<AspNetCoreTransportOptions> configAction = options => 
        {
            configureOptions?.Invoke(options);
        };

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister(builder.MeshId + "_AspNetCoreTransport", configAction))
        {
            return builder;
        }

        builder.Services.Configure(builder.MeshId, configAction);

        TryAddAspNetCoreSerialization(builder.Services);

        // Map unified Core HTTP dispatcher mechanisms globally across explicit isolated meshes safely natively.
        builder.Services.AddP2pHttpCore();

        builder.Services.AddHttpClient($"{builder.MeshId}_P2pAspNetCoreTransport")
            .ConfigurePrimaryHttpMessageHandler(sp =>
            {
                var handler = new HttpClientHandler();
                var optionsMonitor = sp.GetRequiredService<IOptionsMonitor<AspNetCoreTransportOptions>>();
                var options = optionsMonitor.Get(builder.MeshId);
                
                if (options.IgnoreOutboundSslErrors)
                {
                    handler.ServerCertificateCustomValidationCallback = HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
                }
                
                return handler;
            });

        builder.Services.AddKeyedSingleton<PeerEndpoint>(builder.MeshId, (sp, key) =>
        {
            var options = sp.GetRequiredService<IOptionsMonitor<AspNetCoreTransportOptions>>().Get((string)key!);
            return new AspNetCorePeerEndpoint(options.AdvertisedHost, options.AdvertisedPort);
        });

        builder.Services.AddKeyedSingleton<ITransport>(builder.MeshId, (sp, key) =>
            new AspNetCoreTransport(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<AspNetCoreTransportOptions>>(),
                sp.GetRequiredService<IHttpClientFactory>(),
                sp.GetRequiredKeyedService<IMeshWireEncoder>(key),
                sp.GetRequiredService<IPeerRegistry>(),
                sp.GetRequiredService<ILogger<AspNetCoreTransport>>()));

        builder.Services.AddKeyedSingleton<ITransportListener>(builder.MeshId, (sp, key) =>
            new AspNetCoreTransportListener(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<AspNetCoreTransportOptions>>(),
                sp.GetRequiredService<IHttpInboundDispatcher>(),
                sp.GetRequiredService<ILogger<AspNetCoreTransportListener>>(),
                sp.GetService<ICertificateLoader>()));

        return builder;
    }
}