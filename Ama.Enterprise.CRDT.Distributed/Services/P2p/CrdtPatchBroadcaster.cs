namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// Decoupled observer explicitly handling the P2P broadcast propagation of locally generated document patches.
/// </summary>
public sealed class CrdtPatchBroadcaster : IDocumentPatchObserver, IDisposable
{
    private readonly IP2pAlgorithm p2pAlgorithm;
    private readonly ReplicaContext replicaContext;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtPatchBroadcaster> logger;

    private readonly Meter meter;
    private readonly Counter<long> broadcastBytesCounter;

    public CrdtPatchBroadcaster(
        IP2pAlgorithm p2pAlgorithm,
        ReplicaContext replicaContext,
        ICrdtSerializer serializer,
        ILogger<CrdtPatchBroadcaster> logger,
        IMeterFactory? meterFactory = null)
    {
        this.p2pAlgorithm = p2pAlgorithm ?? throw new ArgumentNullException(nameof(p2pAlgorithm));
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtPatchBroadcaster") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtPatchBroadcaster");
        this.broadcastBytesCounter = this.meter.CreateCounter<long>("crdt.broadcaster.broadcast_bytes", "bytes", "Total bytes broadcasted across real-time patch syncs");
    }

    /// <inheritdoc />
    public async Task OnPatchGeneratedAsync(string documentId, CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        try
        {
            var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, patch);
            var payload = serializer.SerializeToBytes(patchMsg);
            
            var wrapper = new CrdtMessageWrapper(documentId, "CrdtPatch", payload);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            broadcastBytesCounter.Add(finalBytes.Length, new KeyValuePair<string, object?>("document_id", documentId));

            await p2pAlgorithm.BroadcastAsync(finalBytes, cancellationToken).ConfigureAwait(false);
            
            logger.LogTrace("Broadcasted active sync patch for document {DocumentId}.", documentId);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to broadcast active sync patch for document {DocumentId}.", documentId);
        }
    }

    public void Dispose()
    {
        meter.Dispose();
    }
}