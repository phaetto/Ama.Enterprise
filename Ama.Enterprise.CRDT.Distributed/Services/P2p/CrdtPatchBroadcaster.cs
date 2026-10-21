namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Decoupled observer handling the P2P broadcast propagation of locally generated document patches via an asynchronous background channel.
/// </summary>
public sealed class CrdtPatchBroadcaster : IDocumentPatchObserver, IDisposable
{
    private readonly record struct OutboundPatchItem(string DocumentId, string? DocumentTypeAlias, CrdtPatch Patch);

    private readonly IServiceProvider serviceProvider;
    private readonly IP2pAlgorithm p2pAlgorithm;
    private readonly ReplicaContext replicaContext;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtPatchBroadcaster> logger;

    private readonly Channel<OutboundPatchItem> broadcastChannel;
    private readonly CancellationTokenSource disposeCts = new();

    private readonly Meter meter;
    private readonly Counter<long> broadcastBytesCounter;
    private readonly Counter<long> patchesBroadcastedCounter;
    private readonly Counter<long> broadcastFailuresCounter;

    public CrdtPatchBroadcaster(
        IServiceProvider serviceProvider,
        IP2pAlgorithm p2pAlgorithm,
        ReplicaContext replicaContext,
        ICrdtSerializer serializer,
        ILogger<CrdtPatchBroadcaster> logger,
        IMeterFactory? meterFactory = null,
        IOptions<DistributedCrdtOptions>? options = null)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.p2pAlgorithm = p2pAlgorithm ?? throw new ArgumentNullException(nameof(p2pAlgorithm));
        this.replicaContext = replicaContext ?? throw new ArgumentNullException(nameof(replicaContext));
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var capacity = options?.Value.ChannelCapacity ?? 0;
        if (capacity > 0)
        {
            this.broadcastChannel = Channel.CreateBounded<OutboundPatchItem>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
        }
        else
        {
            this.broadcastChannel = Channel.CreateUnbounded<OutboundPatchItem>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
        }

        this.meter = meterFactory?.Create("Ama.Enterprise.CRDT.Distributed.CrdtPatchBroadcaster") ?? new Meter("Ama.Enterprise.CRDT.Distributed.CrdtPatchBroadcaster");
        this.broadcastBytesCounter = this.meter.CreateCounter<long>("crdt.broadcaster.broadcast_bytes", "bytes", "Total bytes broadcasted for patches");
        this.patchesBroadcastedCounter = this.meter.CreateCounter<long>("crdt.broadcaster.patches_broadcasted", "patches", "Total patch payloads broadcasted");
        this.broadcastFailuresCounter = this.meter.CreateCounter<long>("crdt.broadcaster.broadcast_failures", "errors", "Total errors during patch broadcasting");

        _ = Task.Run(ProcessBroadcastChannelAsync);
    }

    /// <inheritdoc />
    public Task OnPatchGeneratedAsync(string documentId, string? documentTypeAlias, CrdtPatch patch, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(documentId)) throw new ArgumentException("Document ID cannot be null or empty.", nameof(documentId));

        var item = new OutboundPatchItem(documentId, documentTypeAlias, patch);
        if (broadcastChannel.Writer.TryWrite(item))
        {
            return Task.CompletedTask;
        }

        return broadcastChannel.Writer.WriteAsync(item, cancellationToken).AsTask();
    }

    public void Dispose()
    {
        disposeCts.Cancel();
        broadcastChannel.Writer.TryComplete();
        disposeCts.Dispose();
        meter.Dispose();
    }

    private async Task ProcessBroadcastChannelAsync()
    {
        try
        {
            await foreach (var item in broadcastChannel.Reader.ReadAllAsync(disposeCts.Token).ConfigureAwait(false))
            {
                try
                {
                    var documentTypeAlias = item.DocumentTypeAlias;
                    if (string.IsNullOrWhiteSpace(documentTypeAlias))
                    {
                        var orchestrator = serviceProvider.GetService<ICrdtDocumentOrchestrator>();
                        if (orchestrator?.Registry != null && !string.Equals(item.DocumentId, orchestrator.Registry.DocumentId, StringComparison.Ordinal))
                        {
                            var registryDoc = await orchestrator.Registry.GetDocumentAsync(disposeCts.Token).ConfigureAwait(false);
                            foreach (var entry in registryDoc.Data.Documents.Values)
                            {
                                if (string.Equals(entry.DocumentId, item.DocumentId, StringComparison.Ordinal) && !entry.IsDeleted)
                                {
                                    documentTypeAlias = entry.TypeAlias;
                                    break;
                                }
                            }
                        }
                    }

                    var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, item.Patch);
                    var payload = serializer.SerializeToBytes(patchMsg);
                    
                    var wrapper = new CrdtMessageWrapper(item.DocumentId, "CrdtPatch", payload, documentTypeAlias);
                    var finalBytes = serializer.SerializeToBytes(wrapper);

                    broadcastBytesCounter.Add(finalBytes.Length, new KeyValuePair<string, object?>("document_id", item.DocumentId));

                    await p2pAlgorithm.BroadcastAsync(finalBytes, disposeCts.Token).ConfigureAwait(false);
                    
                    patchesBroadcastedCounter.Add(1, new KeyValuePair<string, object?>("document_id", item.DocumentId));
                    logger.LogTrace("Broadcasted patch for document {DocumentId}.", item.DocumentId);
                }
                catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    broadcastFailuresCounter.Add(1, new KeyValuePair<string, object?>("document_id", item.DocumentId));
                    logger.LogError(ex, "Failed to broadcast patch for document {DocumentId}.", item.DocumentId);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Broadcast background channel failed.");
        }
    }
}