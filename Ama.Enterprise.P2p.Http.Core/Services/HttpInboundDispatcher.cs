namespace Ama.Enterprise.P2p.Http.Core.Services;

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Http.Core.Models;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Singleton orchestrator managing decoupling of inbound HTTP frameworks mapping raw payloads into decoupled P2P processing delegates.
/// </summary>
public sealed class HttpInboundDispatcher(
    ICrdtSerializer serializer,
    ILogger<HttpInboundDispatcher> logger) : IHttpInboundDispatcher
{
    private readonly ICrdtSerializer serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    private readonly ILogger<HttpInboundDispatcher> logger = logger ?? throw new ArgumentNullException(nameof(logger));
    
    private readonly ConcurrentDictionary<string, Func<IMeshMessage, Task>> listeners = new(StringComparer.Ordinal);

    /// <inheritdoc />
    public void RegisterListener(string meshId, Func<IMeshMessage, Task> onMessageReceived)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }
        
        if (onMessageReceived is null)
        {
            throw new ArgumentNullException(nameof(onMessageReceived));
        }

        listeners.AddOrUpdate(meshId, onMessageReceived, (_, _) => onMessageReceived);
        logger.LogInformation("[{MeshId}] HTTP inbound dispatcher registered listener.", meshId);
    }

    /// <inheritdoc />
    public void UnregisterListener(string meshId)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            return;
        }

        if (listeners.TryRemove(meshId, out _))
        {
            logger.LogInformation("[{MeshId}] HTTP inbound dispatcher unregistered listener.", meshId);
        }
    }

    /// <inheritdoc />
    public async Task<HttpPayloadProcessResult> ProcessPayloadAsync(string targetMeshId, string? protocolVersionHeader, Stream bodyStream, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(targetMeshId))
        {
            return HttpPayloadProcessResult.BadRequest;
        }

        try
        {
            if (!string.IsNullOrWhiteSpace(protocolVersionHeader) && !IsMajorVersionCompatible(protocolVersionHeader, Constants.ProtocolVersion))
            {
                logger.LogWarning("[{MeshId}] Header protocol version {IncomingVersion} is not compatible with local version {LocalVersion}.", targetMeshId, protocolVersionHeader, Constants.ProtocolVersion);
                return HttpPayloadProcessResult.UnsupportedVersion;
            }

            using var memoryStream = new MemoryStream();
            await bodyStream.CopyToAsync(memoryStream, cancellationToken).ConfigureAwait(false);
            
            var payload = memoryStream.ToArray();
            var message = serializer.DeserializeFromBytes<IMeshMessage>(payload);

            if (message is null) 
            {
                logger.LogWarning("[{MeshId}] Failed to deserialize incoming HTTP message. Invalid format.", targetMeshId);
                return HttpPayloadProcessResult.BadRequest;
            }

            if (!IsMajorVersionCompatible(message.ProtocolVersion, Constants.ProtocolVersion))
            {
                logger.LogWarning("[{MeshId}] Internal message protocol version {MessageVersion} is not compatible with local version {LocalVersion}.", targetMeshId, message.ProtocolVersion, Constants.ProtocolVersion);
                return HttpPayloadProcessResult.UnsupportedVersion;
            }

            if (!string.Equals(message.MeshId, targetMeshId, StringComparison.Ordinal))
            {
                logger.LogWarning("[{MeshId}] Rejected HTTP message targeting foreign mesh ID {ForeignMeshId}.", targetMeshId, message.MeshId);
                return HttpPayloadProcessResult.Forbidden;
            }

            if (listeners.TryGetValue(targetMeshId, out var listener))
            {
                try
                {
                    await listener(message).ConfigureAwait(false);
                    return HttpPayloadProcessResult.Success;
                }
                catch (NotSupportedException ex)
                {
                    logger.LogWarning(ex, "[{MeshId}] Message rejected by inner payload handler natively: Protocol version not supported.", targetMeshId);
                    return HttpPayloadProcessResult.UnsupportedVersion;
                }
            }

            logger.LogWarning("[{MeshId}] No active listener registered to process the HTTP payload.", targetMeshId);
            return HttpPayloadProcessResult.ServiceUnavailable;
        }
        catch (NotSupportedException ex)
        {
            logger.LogWarning(ex, "[{MeshId}] HTTP message rejected: Protocol version not supported.", targetMeshId);
            return HttpPayloadProcessResult.UnsupportedVersion;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Error processing incoming HTTP request generically.", targetMeshId);
            return HttpPayloadProcessResult.InternalServerError;
        }
    }

    private static bool IsMajorVersionCompatible(string? version1, string? version2)
    {
        if (string.IsNullOrWhiteSpace(version1) || string.IsNullOrWhiteSpace(version2)) 
        {
            return true;
        }

        var v1Major = version1.Split('.')[0];
        var v2Major = version2.Split('.')[0];

        return string.Equals(v1Major, v2Major, StringComparison.Ordinal);
    }
}