namespace Ama.Enterprise.P2p.AspNetCore.Extensions;

using System;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.AspNetCore.Services;
using Ama.Enterprise.P2p.AspNetCore.Services.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Provides extension methods for routing decentralized ASP.NET Core application HTTP traffic mapped to P2P meshes.
/// </summary>
public static class EndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps generic inbound POST endpoints handling multiple distinct mesh topology roots via standard routes.
    /// </summary>
    /// <param name="endpoints">The route builder instance tracking internal HTTP structural boundaries.</param>
    /// <param name="routePrefix">The base path prefix standardizing generic explicit routes. Defaults to "/ama-enterprise/p2p-mesh".</param>
    /// <returns>A convention builder permitting explicit mapped endpoint customization.</returns>
    public static IEndpointConventionBuilder MapP2pMeshEndpoints(this IEndpointRouteBuilder endpoints, string routePrefix = "/ama-enterprise/p2p-mesh")
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var pattern = $"{routePrefix.TrimEnd('/')}/{{meshId}}";

        return endpoints.MapPost(pattern, new RequestDelegate(async context =>
        {
            var meshId = context.GetRouteValue("meshId")?.ToString();
            if (string.IsNullOrEmpty(meshId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var dispatcher = context.RequestServices.GetRequiredService<IHttpInboundDispatcher>();
            var incomingVersion = context.Request.Headers["X-P2P-Protocol-Version"].ToString();
            
            var result = await dispatcher.ProcessPayloadAsync(meshId, incomingVersion, context.Request.Body, context.RequestAborted).ConfigureAwait(false);

            context.Response.StatusCode = result switch
            {
                HttpPayloadProcessResult.Success => StatusCodes.Status202Accepted,
                HttpPayloadProcessResult.BadRequest => StatusCodes.Status400BadRequest,
                HttpPayloadProcessResult.Forbidden => StatusCodes.Status403Forbidden,
                HttpPayloadProcessResult.UnsupportedVersion => StatusCodes.Status505HttpVersionNotsupported,
                HttpPayloadProcessResult.ServiceUnavailable => StatusCodes.Status503ServiceUnavailable,
                HttpPayloadProcessResult.InternalServerError => StatusCodes.Status500InternalServerError,
                _ => StatusCodes.Status500InternalServerError
            };
        }));
    }

    /// <summary>
    /// Maps phase 2 inbound discovery handshake endpoints tracking decoupled discovery topologies.
    /// </summary>
    /// <param name="endpoints">The explicitly constrained route builder mapping boundaries.</param>
    /// <param name="routePrefix">The base path resolving Phase 2 decoupled probes. Defaults to "/ama-enterprise/p2p-handshake".</param>
    /// <returns>A unified standard ASP.NET convention builder ensuring identical custom configurations.</returns>
    public static IEndpointConventionBuilder MapP2pMeshHandshakes(this IEndpointRouteBuilder endpoints, string routePrefix = "/ama-enterprise/p2p-handshake")
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var pattern = $"{routePrefix.TrimEnd('/')}/{{meshId}}";

        return endpoints.MapPost(pattern, new RequestDelegate(async context =>
        {
            var meshId = context.GetRouteValue("meshId")?.ToString();
            if (string.IsNullOrEmpty(meshId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var handshaker = context.RequestServices.GetKeyedService<IPeerHandshaker>(meshId) as AspNetCorePeerHandshaker;
            if (handshaker is not null)
            {
                await handshaker.HandleHandshakeRequestAsync(context).ConfigureAwait(false);
            }
            else
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
            }
        }));
    }

    /// <summary>
    /// Maps phase 1 inbound HTTP discovery endpoints tracking decoupled discovery topologies natively via ASP.NET Core integrations.
    /// </summary>
    /// <param name="endpoints">The explicitly constrained route builder mapping boundaries.</param>
    /// <param name="routePrefix">The base path resolving Phase 1 decoupled probes. Defaults to "/ama-enterprise/p2p-discovery".</param>
    /// <returns>A unified standard ASP.NET convention builder ensuring identical custom configurations.</returns>
    public static IEndpointConventionBuilder MapP2pMeshDiscovery(this IEndpointRouteBuilder endpoints, string routePrefix = "/ama-enterprise/p2p-discovery")
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var pattern = $"{routePrefix.TrimEnd('/')}/{{meshId}}";

        return endpoints.MapPost(pattern, new RequestDelegate(async context =>
        {
            var meshId = context.GetRouteValue("meshId")?.ToString();
            if (string.IsNullOrEmpty(meshId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var discovery = context.RequestServices.GetKeyedService<IPeerDiscovery>(meshId) as AspNetCorePeerDiscovery;
            if (discovery is not null)
            {
                await discovery.HandleDiscoveryRequestAsync(context).ConfigureAwait(false);
            }
            else
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
            }
        }));
    }
}