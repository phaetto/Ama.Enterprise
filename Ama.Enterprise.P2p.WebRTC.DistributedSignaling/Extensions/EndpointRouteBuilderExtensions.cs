namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Extensions;

using System;
using System.IO;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;
using Ama.Enterprise.P2p.WebRTC.Models;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Provides extension methods for routing distributed WebRTC signaling drops natively via ASP.NET Core generic pipelines.
/// </summary>
public static class EndpointRouteBuilderExtensions
{
    /// <summary>
    /// Maps generic inbound HTTP minimal API endpoints wrapping AOT WebRTC Distributed Signaling state drops resolving decentralized mesh connections.
    /// </summary>
    /// <param name="endpoints">The explicitly constrained route builder mapping boundaries.</param>
    /// <param name="routePrefix">The base path prefix standardizing generic explicit routes. Defaults to "/ama-enterprise/webrtc-signaling".</param>
    /// <returns>A unified standard ASP.NET convention builder ensuring identical custom configurations.</returns>
    public static IEndpointConventionBuilder MapWebRtcDistributedSignalingEndpoints(this IEndpointRouteBuilder endpoints, string routePrefix = "/ama-enterprise/webrtc-signaling")
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var basePath = routePrefix.TrimEnd('/');
        var intentsPattern = $"{basePath}/{{replicaId}}/intents";
        var offersPattern = $"{basePath}/{{replicaId}}/offers";
        var answersPattern = $"{basePath}/{{replicaId}}/answers";

        endpoints.MapGet(intentsPattern, new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();
            var serializer = context.RequestServices.GetRequiredService<ICrdtSerializer>();
            
            var intents = manager.GetJoinIntents(documentId);
            var payload = serializer.SerializeToBytes(intents);

            context.Response.ContentType = "application/octet-stream";
            await context.Response.Body.WriteAsync(payload, context.RequestAborted).ConfigureAwait(false);
        }));

        endpoints.MapPost($"{intentsPattern}/{{peerId:guid}}", new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var peerIdStr = context.GetRouteValue("peerId")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId) || !Guid.TryParse(peerIdStr, out var peerId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();

            await manager.SetJoinIntentAsync(peerId, documentId, context.RequestAborted).ConfigureAwait(false);
            context.Response.StatusCode = StatusCodes.Status202Accepted;
        }));

        endpoints.MapDelete($"{intentsPattern}/{{peerId:guid}}", new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var peerIdStr = context.GetRouteValue("peerId")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId) || !Guid.TryParse(peerIdStr, out var peerId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();

            await manager.RemoveJoinIntentAsync(peerId, documentId, context.RequestAborted).ConfigureAwait(false);
            context.Response.StatusCode = StatusCodes.Status204NoContent;
        }));

        endpoints.MapGet(offersPattern, new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();
            var serializer = context.RequestServices.GetRequiredService<ICrdtSerializer>();
            
            var offers = manager.GetOffers(documentId);
            var payload = serializer.SerializeToBytes(offers);

            context.Response.ContentType = "application/octet-stream";
            await context.Response.Body.WriteAsync(payload, context.RequestAborted).ConfigureAwait(false);
        }));

        endpoints.MapPost(offersPattern, new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();
            var routingKey = context.Request.Query["routingKey"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId) || string.IsNullOrWhiteSpace(routingKey))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();
            var serializer = context.RequestServices.GetRequiredService<ICrdtSerializer>();

            using var ms = new MemoryStream();
            await context.Request.Body.CopyToAsync(ms, context.RequestAborted).ConfigureAwait(false);
            var payload = ms.ToArray();

            if (payload.Length == 0)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var offer = serializer.DeserializeFromBytes<WebRtcInvitationOffer>(payload);
            if (offer == null)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            await manager.SetOfferAsync(routingKey, offer, documentId, context.RequestAborted).ConfigureAwait(false);
            context.Response.StatusCode = StatusCodes.Status202Accepted;
        }));

        endpoints.MapDelete($"{offersPattern}/{{routingKey}}", new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var routingKey = context.GetRouteValue("routingKey")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId) || string.IsNullOrWhiteSpace(routingKey))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();

            await manager.RemoveOfferAsync(routingKey, documentId, context.RequestAborted).ConfigureAwait(false);
            context.Response.StatusCode = StatusCodes.Status204NoContent;
        }));

        endpoints.MapGet(answersPattern, new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();
            var serializer = context.RequestServices.GetRequiredService<ICrdtSerializer>();
            
            var answers = manager.GetAnswers(documentId);
            var payload = serializer.SerializeToBytes(answers);

            context.Response.ContentType = "application/octet-stream";
            await context.Response.Body.WriteAsync(payload, context.RequestAborted).ConfigureAwait(false);
        }));

        endpoints.MapPost(answersPattern, new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();
            var routingKey = context.Request.Query["routingKey"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId) || string.IsNullOrWhiteSpace(routingKey))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();
            var serializer = context.RequestServices.GetRequiredService<ICrdtSerializer>();

            using var ms = new MemoryStream();
            await context.Request.Body.CopyToAsync(ms, context.RequestAborted).ConfigureAwait(false);
            var payload = ms.ToArray();

            if (payload.Length == 0)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var answer = serializer.DeserializeFromBytes<WebRtcInvitationAnswer>(payload);
            if (answer == null)
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            await manager.SetAnswerAsync(routingKey, answer, documentId, context.RequestAborted).ConfigureAwait(false);
            context.Response.StatusCode = StatusCodes.Status202Accepted;
        }));

        endpoints.MapDelete($"{answersPattern}/{{routingKey}}", new RequestDelegate(async context =>
        {
            var replicaId = context.GetRouteValue("replicaId")?.ToString();
            var routingKey = context.GetRouteValue("routingKey")?.ToString();
            var documentId = context.Request.Query["documentId"].ToString();

            if (string.IsNullOrWhiteSpace(replicaId) || string.IsNullOrWhiteSpace(documentId) || string.IsNullOrWhiteSpace(routingKey))
            {
                context.Response.StatusCode = StatusCodes.Status400BadRequest;
                return;
            }

            var scopeManager = context.RequestServices.GetRequiredService<DistributedCrdtScopeManager>();
            var scope = scopeManager.GetOrCreateScope(replicaId);
            var manager = scope.ServiceProvider.GetRequiredService<ICrdtSignalingManager>();

            await manager.RemoveAnswerAsync(routingKey, documentId, context.RequestAborted).ConfigureAwait(false);
            context.Response.StatusCode = StatusCodes.Status204NoContent;
        }));

        // Return the core convention builder honoring generic mappings.
        return endpoints.MapGet($"{basePath}/ping", () => "pong");
    }
}