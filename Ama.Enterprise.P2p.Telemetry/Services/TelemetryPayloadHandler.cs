namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Telemetry.Models;
using Microsoft.Extensions.Logging;

/// <summary>
/// Application payload handler dedicated to processing incoming telemetry network payloads 
/// and routing them into the centralized aggregator.
/// </summary>
public sealed class TelemetryPayloadHandler : IApplicationPayloadHandler
{
    private readonly ITelemetryAggregator aggregator;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<TelemetryPayloadHandler> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="TelemetryPayloadHandler"/> class.
    /// </summary>
    public TelemetryPayloadHandler(
        ITelemetryAggregator aggregator,
        ICrdtSerializer serializer,
        ILogger<TelemetryPayloadHandler> logger)
    {
        this.aggregator = aggregator ?? throw new ArgumentNullException(nameof(aggregator));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task HandlePayloadAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (payload.IsEmpty)
        {
            return Task.CompletedTask;
        }

        try
        {
            var dto = serializer.DeserializeFromBytes<TelemetryPayloadDto>(payload.ToArray());
            
            if (dto != null && dto.NodeId != Guid.Empty)
            {
                logger.LogTrace("Received telemetry payload mapping {Count} metrics from node {NodeId}.", dto.Metrics?.Count ?? 0, dto.NodeId);
                aggregator.UpdateNodeTelemetry(dto);
            }
        }
        catch (Exception ex)
        {
            // Ignoring errors since payloads might belong to other application domains sharing the generic mesh
            logger.LogTrace(ex, "Payload could not be parsed as explicit telemetry structures.");
        }

        return Task.CompletedTask;
    }
}