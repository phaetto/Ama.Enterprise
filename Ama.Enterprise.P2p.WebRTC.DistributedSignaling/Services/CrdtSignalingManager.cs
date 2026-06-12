namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Implementation managing the WebRTC out-of-band signaling drop-box avoiding centralized datastores.
/// </summary>
public sealed class CrdtSignalingManager : ICrdtSignalingManager, IDisposable
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly IAsyncCrdtPatcher patcher;
    private readonly object syncRoot = new();
    private bool disposed;

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtSignalingManager"/> class.
    /// </summary>
    /// <param name="orchestrator">The document orchestrator mapping internal CRDT lifecycles.</param>
    /// <param name="patcher">The patcher facilitating local state mutations.</param>
    public CrdtSignalingManager(
        ICrdtDocumentOrchestrator orchestrator,
        IAsyncCrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));

        this.orchestrator.DocumentsChanged += OnOrchestratorChanged;
        SubscribeToActiveDocuments();
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, long> GetJoinIntents(string? documentId = null)
    {
        lock (syncRoot)
        {
            var doc = orchestrator.GetDocument<CrdtSignalingState>(ResolveDocumentId(documentId));
            return doc != null 
                ? new ReadOnlyDictionary<string, long>(doc.Document.Data.JoinIntents) 
                : new ReadOnlyDictionary<string, long>(new Dictionary<string, long>());
        }
    }

    /// <inheritdoc />
    public async Task SetJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docId = ResolveDocumentId(documentId);
        var doc = await GetOrCreateDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
        if (doc != null)
        {
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.JoinIntents, new MapSetIntent(peerId.ToString(), timestamp), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
            await orchestrator.BroadcastPatchAsync(docId, patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docId = ResolveDocumentId(documentId);
        var doc = orchestrator.GetDocument<CrdtSignalingState>(docId);
        if (doc != null)
        {
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.JoinIntents, new MapRemoveIntent(peerId.ToString()), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
            await orchestrator.BroadcastPatchAsync(docId, patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, WebRtcInvitationOffer> GetOffers(string? documentId = null)
    {
        lock (syncRoot)
        {
            var doc = orchestrator.GetDocument<CrdtSignalingState>(ResolveDocumentId(documentId));
            return doc != null 
                ? new ReadOnlyDictionary<string, WebRtcInvitationOffer>(doc.Document.Data.Offers) 
                : new ReadOnlyDictionary<string, WebRtcInvitationOffer>(new Dictionary<string, WebRtcInvitationOffer>());
        }
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, WebRtcInvitationAnswer> GetAnswers(string? documentId = null)
    {
        lock (syncRoot)
        {
            var doc = orchestrator.GetDocument<CrdtSignalingState>(ResolveDocumentId(documentId));
            return doc != null 
                ? new ReadOnlyDictionary<string, WebRtcInvitationAnswer>(doc.Document.Data.Answers) 
                : new ReadOnlyDictionary<string, WebRtcInvitationAnswer>(new Dictionary<string, WebRtcInvitationAnswer>());
        }
    }

    /// <inheritdoc />
    public async Task SetOfferAsync(string routingKey, WebRtcInvitationOffer offer, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        
        var docId = ResolveDocumentId(documentId);
        var doc = await GetOrCreateDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
        if (doc != null)
        {
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Offers, new MapSetIntent(routingKey, offer), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
            
            // Instantly push this specific change across the network bypassing typical background intervals
            await orchestrator.BroadcastPatchAsync(docId, patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveOfferAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        
        var docId = ResolveDocumentId(documentId);
        var doc = orchestrator.GetDocument<CrdtSignalingState>(docId);
        if (doc != null)
        {
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Offers, new MapRemoveIntent(routingKey), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
            await orchestrator.BroadcastPatchAsync(docId, patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task SetAnswerAsync(string routingKey, WebRtcInvitationAnswer answer, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);

        var docId = ResolveDocumentId(documentId);
        var doc = await GetOrCreateDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
        if (doc != null)
        {
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Answers, new MapSetIntent(routingKey, answer), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
            
            // Instantly push this specific change across the network bypassing typical background intervals
            await orchestrator.BroadcastPatchAsync(docId, patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveAnswerAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);

        var docId = ResolveDocumentId(documentId);
        var doc = orchestrator.GetDocument<CrdtSignalingState>(docId);
        if (doc != null)
        {
            var operation = await patcher.GenerateOperationAsync(doc.Document, x => x.Answers, new MapRemoveIntent(routingKey), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await doc.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
            await orchestrator.BroadcastPatchAsync(docId, patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (disposed) return;
        disposed = true;

        orchestrator.DocumentsChanged -= OnOrchestratorChanged;
        var docs = orchestrator.GetActiveDocuments();
        foreach (var d in docs)
        {
            if (d is IDistributedCrdtDocument<CrdtSignalingState> typedDoc)
            {
                typedDoc.StateChanged -= OnDocumentStateChanged;
            }
        }
    }

    private void OnOrchestratorChanged(object? sender, EventArgs e)
    {
        SubscribeToActiveDocuments();
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    private void SubscribeToActiveDocuments()
    {
        lock (syncRoot)
        {
            var docs = orchestrator.GetActiveDocuments();
            foreach (var d in docs)
            {
                if (d is IDistributedCrdtDocument<CrdtSignalingState> typedDoc)
                {
                    typedDoc.StateChanged -= OnDocumentStateChanged;
                    typedDoc.StateChanged += OnDocumentStateChanged;
                }
            }
        }
    }

    private void OnDocumentStateChanged(object? sender, EventArgs e)
    {
        StateChanged?.Invoke(this, EventArgs.Empty);
    }

    private static string ResolveDocumentId(string? documentId)
    {
        return string.IsNullOrWhiteSpace(documentId) ? Constants.DefaultSignalingDocumentId : documentId;
    }

    private async Task<IDistributedCrdtDocument<CrdtSignalingState>?> GetOrCreateDocumentAsync(string documentId, CancellationToken cancellationToken)
    {
        var doc = orchestrator.GetDocument<CrdtSignalingState>(documentId);
        if (doc != null)
        {
            return doc;
        }

        await orchestrator.CreateDocumentAsync(documentId, Constants.SignalingDocumentTypeAlias, cancellationToken).ConfigureAwait(false);
        return orchestrator.GetDocument<CrdtSignalingState>(documentId);
    }
}