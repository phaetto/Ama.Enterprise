namespace Ama.Enterprise.P2p.AspNetCore.Extensions;

using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.AspNetCore.Services;
using Ama.Enterprise.P2p.AspNetCore.Services.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Provides extension methods for seamlessly routing decentralized ASP.NET Core application HTTP traffic mapped actively bounding P2P meshes dynamically.
/// </summary>
public static class EndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps generic inbound POST endpoints bounding decoupled payload streams handling multiple distinct mesh topology roots securely isolated mapping standard routes purely.
    /// </summary>
    /// <param name="endpoints">The route builder instance tracking internal HTTP structural boundaries cleanly.</param>
    /// <param name="routePrefix">The base path prefix standardizing generic explicit routes natively. Defaults to "/ama-enterprise/p2p-mesh".</param>
    /// <returns>A convention builder safely permitting explicit mapped dynamic endpoint customization purely.</returns>
    public static IEndpointConventionBuilder MapP2pMeshEndpoints(this IEndpointRouteBuilder endpoints, string routePrefix = "/ama-enterprise/p2p-mesh")
    {
        if (endpoints is null)
        {
            throw new ArgumentNullException(nameof(endpoints));
        }

        var pattern = $"{routePrefix.TrimEnd('/')}/{{meshId}}";

        return endpoints.MapPost(pattern, async (HttpContext context, string meshId) =>
        {
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
        });
    }

    /// <summary>
    /// Maps phase 2 inbound discovery handshake endpoints tracking actively decoupled discovery topologies routing explicitly isolated explicit integrated bounds gracefully natively.
    /// </summary>
    /// <param name="endpoints">The explicitly constrained route builder mapping boundaries.</param>
    /// <param name="routePrefix">The base structurally standard path resolving Phase 2 decoupled probes organically. Defaults to "/ama-enterprise/p2p-handshake".</param>
    /// <returns>A unified standard ASP.NET convention builder ensuring identical robust custom configurations natively cleanly.</returns>
    public static IEndpointConventionBuilder MapP2pMeshHandshakes(this IEndpointRouteBuilder endpoints, string routePrefix = "/ama-enterprise/p2p-handshake")
    {
        if (endpoints is null)
        {
            throw new ArgumentNullException(nameof(endpoints));
        }

        var pattern = $"{routePrefix.TrimEnd('/')}/{{meshId}}";

        return endpoints.MapPost(pattern, async (HttpContext context, string meshId) =>
        {
            var handshaker = context.RequestServices.GetKeyedService<IPeerHandshaker>(meshId) as AspNetCorePeerHandshaker;
            if (handshaker is not null)
            {
                await handshaker.HandleHandshakeRequestAsync(context).ConfigureAwait(false);
            }
            else
            {
                context.Response.StatusCode = StatusCodes.Status404NotFound;
            }
        });
    }
}