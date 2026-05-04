namespace Ama.Enterprise.P2p.AspNetCore.Extensions;

using System.Threading.Tasks;
using Ama.Enterprise.P2p.Http.Core.Models;
using Ama.Enterprise.P2p.Http.Core.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Provides extension methods for seamlessly routing decentralized ASP.NET Core application HTTP traffic mapped actively bounding P2P meshes.
/// </summary>
public static class EndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps generic inbound POST endpoints bounding decoupled payload streams handling multiple distinct mesh topology roots securely isolated mapping standard routes.
    /// </summary>
    /// <param name="endpoints">The route builder instance.</param>
    /// <param name="routePrefix">The base path prefix standardizing routes. Defaults to "/p2p-mesh".</param>
    /// <returns>A convention builder safely permitting endpoint customization natively.</returns>
    public static IEndpointConventionBuilder MapP2pMeshEndpoints(this IEndpointRouteBuilder endpoints, string routePrefix = "/p2p-mesh")
    {
        if (endpoints is null)
        {
            throw new System.ArgumentNullException(nameof(endpoints));
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
}