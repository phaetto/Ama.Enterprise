namespace Ama.Enterprise.P2p.Telemetry.Services;

using System;
using System.Buffers.Binary;
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
        if (payload.Length < 4)
        {
            return Task.CompletedTask;
        }

        var header = BinaryPrimitives.ReadInt32LittleEndian(payload.Span);
        if (header != TelemetryPayloadDto.MagicHeader)
        {
            // Drop it instantly without throwing exceptions, payload belongs to another domain sharing the mesh
            return Task.CompletedTask;
        }

        var payloadData = payload.Slice(4);

        try
        {
            var dto = serializer.DeserializeFromBytes<TelemetryPayloadDto>(payloadData.ToArray());
            
            if (dto != null && dto.NodeId != Guid.Empty)
            {
                logger.LogTrace("Received telemetry payload mapping {Count} metrics from node {NodeId}.", dto.Metrics?.Count ?? 0, dto.NodeId);
                aggregator.UpdateNodeTelemetry(dto);
            }
        }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "Payload could not be parsed as explicit telemetry structures.");
        }

        return Task.CompletedTask;
    }
}