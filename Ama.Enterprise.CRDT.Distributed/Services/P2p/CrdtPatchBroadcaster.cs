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
using Ama.Enterprise.CRDT.Distributed.Services.Sharding;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Decoupled observer handling the P2P broadcast propagation of locally generated document patches via an asynchronous background channel.
/// </summary>
public sealed class CrdtPatchBroadcaster : IDocumentPatchObserver, IDisposable
{
    private readonly record struct OutboundPatchItem(string DocumentId, string? DocumentTypeAlias, CrdtPatch Patch, TaskCompletionSource? FlushBarrier = null);

    private readonly IServiceProvider serviceProvider;
    private readonly IP2pAlgorithm p2pAlgorithm;
    private readonly ReplicaContext replicaContext;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtPatchBroadcaster> logger;

    private readonly Channel<OutboundPatchItem> broadcastChannel;
    private readonly CancellationTokenSource disposeCts = new();
    private readonly Task processingTask;
    private readonly Dictionary<(string DocumentId, string? DocumentTypeAlias), List<CrdtOperation>> coalesceBuffer = new();
    private readonly List<TaskCompletionSource> pendingFlushBarriers = new();

    private readonly Meter meter;
    private readonly Counter<long> broadcastBytesCounter;
    private readonly Counter<long> patchesBroadcastedCounter;
    private readonly Counter<long> broadcastFailuresCounter;
    private int disposed;

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

        var capacity = options?.Value.ChannelCapacity is > 0 ? options.Value.ChannelCapacity : 32_768;
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

        this.processingTask = Task.Run(ProcessBroadcastChannelAsync);
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

    /// <inheritdoc />
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        if (disposeCts.IsCancellationRequested)
        {
            return;
        }

        var barrierTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var barrierItem = new OutboundPatchItem(string.Empty, null, default, barrierTcs);

        try
        {
            if (!broadcastChannel.Writer.TryWrite(barrierItem))
            {
                await broadcastChannel.Writer.WriteAsync(barrierItem, cancellationToken).ConfigureAwait(false);
            }

            await barrierTcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        try
        {
            disposeCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }

        broadcastChannel.Writer.TryComplete();
        disposeCts.Dispose();
        meter.Dispose();
    }

    private async Task ProcessBroadcastChannelAsync()
    {
        var reader = broadcastChannel.Reader;
        try
        {
            while (await reader.WaitToReadAsync(disposeCts.Token).ConfigureAwait(false))
            {
                coalesceBuffer.Clear();
                pendingFlushBarriers.Clear();

                int drainedCount = 0;
                while (drainedCount < 4096 && reader.TryRead(out var item))
                {
                    if (item.FlushBarrier is not null)
                    {
                        pendingFlushBarriers.Add(item.FlushBarrier);
                        break;
                    }

                    drainedCount++;
                    var ops = item.Patch.Operations;
                    if (ops == null || ops.Count == 0)
                    {
                        continue;
                    }

                    var key = (item.DocumentId, item.DocumentTypeAlias);
                    if (!coalesceBuffer.TryGetValue(key, out var list))
                    {
                        list = new List<CrdtOperation>(ops.Count);
                        coalesceBuffer[key] = list;
                    }

                    list.AddRange(ops);
                }

                if (coalesceBuffer.Count > 0)
                {
                    foreach (var kvp in coalesceBuffer)
                    {
                        var docId = kvp.Key.DocumentId;
                        var docTypeAlias = kvp.Key.DocumentTypeAlias;
                        var coalescedOps = kvp.Value;

                        await BroadcastSinglePatchAsync(docId, docTypeAlias, new CrdtPatch(coalescedOps)).ConfigureAwait(false);
                    }
                }

                for (int i = 0; i < pendingFlushBarriers.Count; i++)
                {
                    pendingFlushBarriers[i].TrySetResult();
                }

                if (coalesceBuffer.Count > 256)
                {
                    coalesceBuffer.Clear();
                    coalesceBuffer.TrimExcess();
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

    private async Task BroadcastSinglePatchAsync(string documentId, string? documentTypeAlias, CrdtPatch patch)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(documentTypeAlias))
            {
                var orchestrator = serviceProvider.GetService<ICrdtDocumentOrchestrator>();
                if (orchestrator?.Registry != null && !string.Equals(documentId, orchestrator.Registry.DocumentId, StringComparison.Ordinal))
                {
                    var registryData = orchestrator.Registry.ReadData();
                    foreach (var entry in registryData.Documents.Values)
                    {
                        if (string.Equals(entry.DocumentId, documentId, StringComparison.Ordinal) && !entry.IsDeleted)
                        {
                            documentTypeAlias = entry.TypeAlias;
                            break;
                        }
                    }
                }
            }

            var patchMsg = new CrdtPatchMessage(replicaContext.ReplicaId, patch);
            var payload = serializer.SerializeToBytes(patchMsg);
            var scopeContext = serviceProvider.GetService<CrdtScopeContext>();
            var hasExplicitScope = !string.IsNullOrWhiteSpace(scopeContext?.ScopeId) &&
                                   !string.Equals(scopeContext.ScopeId, scopeContext.HostReplicaId, StringComparison.Ordinal);
            var logicalScopeId = !string.IsNullOrWhiteSpace(scopeContext?.ScopeId)
                ? scopeContext.ScopeId
                : replicaContext.ReplicaId;

            var wrapper = new CrdtMessageWrapper(
                documentId,
                "CrdtPatch",
                payload,
                replicaContext.ReplicaId,
                documentTypeAlias,
                hasExplicitScope ? logicalScopeId : null);
            var finalBytes = serializer.SerializeToBytes(wrapper);

            broadcastBytesCounter.Add(finalBytes.Length, new KeyValuePair<string, object?>("document_id", documentId));

            var sentViaDirect = await TryDispatchToScopeAuthorizedPeersAsync(logicalScopeId, finalBytes, disposeCts.Token).ConfigureAwait(false);
            if (!sentViaDirect)
            {
                await p2pAlgorithm.BroadcastAsync(finalBytes, disposeCts.Token).ConfigureAwait(false);
            }

            patchesBroadcastedCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
            logger.LogTrace("Broadcasted patch for document {DocumentId} in scope {ScopeId}.", documentId, logicalScopeId);
        }
        catch (OperationCanceledException) when (disposeCts.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            broadcastFailuresCounter.Add(1, new KeyValuePair<string, object?>("document_id", documentId));
            logger.LogError(ex, "Failed to broadcast patch for document {DocumentId}.", documentId);
        }
    }

    private async ValueTask<bool> TryDispatchToScopeAuthorizedPeersAsync(
        string logicalScopeId,
        byte[] finalBytes,
        CancellationToken cancellationToken)
    {
        var peerRegistry = serviceProvider.GetService<IPeerRegistry>();
        var directSender = serviceProvider.GetService<IDirectMessageSender>();
        if (peerRegistry == null || directSender == null)
        {
            return false;
        }

        var scopeContext = serviceProvider.GetService<CrdtScopeContext>();
        var hostReplicaId = !string.IsNullOrWhiteSpace(scopeContext?.HostReplicaId)
            ? scopeContext.HostReplicaId
            : replicaContext.ReplicaId;

        var boundMeshes = new HashSet<string>(StringComparer.Ordinal);
        foreach (var reg in serviceProvider.GetServices<DistributedCrdtP2pRegistration>())
        {
            if ((string.Equals(reg.ReplicaId, replicaContext.ReplicaId, StringComparison.Ordinal) ||
                 string.Equals(reg.ReplicaId, hostReplicaId, StringComparison.Ordinal)) &&
                !string.IsNullOrWhiteSpace(reg.MeshId))
            {
                boundMeshes.Add(reg.MeshId);
            }
        }

        if (boundMeshes.Count == 0)
        {
            foreach (var mesh in serviceProvider.GetServices<P2pMeshMetadata>())
            {
                if (!string.IsNullOrWhiteSpace(mesh.MeshId))
                {
                    boundMeshes.Add(mesh.MeshId);
                }
            }
        }

        if (boundMeshes.Count == 0)
        {
            return false;
        }

        var topologyProvider = serviceProvider.GetService<IScopeTopologyProvider>();
        var seenPeers = new HashSet<Guid>();
        var hasAnyPeersInRegistry = false;

        foreach (var meshId in boundMeshes)
        {
            var activePeers = await peerRegistry.GetPeersByStatusAsync(meshId, PeerStatus.Active, cancellationToken).ConfigureAwait(false);
            foreach (var peer in activePeers)
            {
                hasAnyPeersInRegistry = true;
                if (!seenPeers.Add(peer.Id.Value))
                {
                    continue;
                }

                var peerIdStr = peer.Id.Value.ToString();
                var isAllowed = true;
                if (topologyProvider != null)
                {
                    isAllowed = await topologyProvider.CanReplicateScopeToPeerAsync(
                        new ScopeAccessContext(logicalScopeId, peerIdStr, meshId, "CrdtPatch"),
                        cancellationToken).ConfigureAwait(false);
                }

                if (isAllowed)
                {
                    if (boundMeshes.Count > 1)
                    {
                        await directSender.SendDirectAsync(meshId, peer.Id, finalBytes, cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        await directSender.SendDirectAsync(peer.Id, finalBytes, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
        }

        return hasAnyPeersInRegistry;
    }
}