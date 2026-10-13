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
/// Implementation managing the WebRTC out-of-band signaling drop-box avoiding centralized datastores enforcing explicit decoupled behavior mappings explicitly intelligently reliably perfectly seamlessly cleanly dynamically structurally completely.
/// </summary>
public sealed class CrdtSignalingManager : ICrdtSignalingManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly IAsyncCrdtPatcher patcher;

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
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, long>> GetJoinIntentsAsync(string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docManager = orchestrator.GetDocument<CrdtSignalingState>(ResolveDocumentId(documentId));
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            return new ReadOnlyDictionary<string, long>(crdtDoc.Data.JoinIntents);
        }
        
        return new ReadOnlyDictionary<string, long>(new Dictionary<string, long>());
    }

    /// <inheritdoc />
    public async Task SetJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docId = ResolveDocumentId(documentId);
        var docManager = await GetOrCreateDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.JoinIntents, new MapSetIntent(peerId.ToString(), timestamp), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docId = ResolveDocumentId(documentId);
        var docManager = orchestrator.GetDocument<CrdtSignalingState>(docId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.JoinIntents, new MapRemoveIntent(peerId.ToString()), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, WebRtcInvitationOffer>> GetOffersAsync(string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docManager = orchestrator.GetDocument<CrdtSignalingState>(ResolveDocumentId(documentId));
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            return new ReadOnlyDictionary<string, WebRtcInvitationOffer>(crdtDoc.Data.Offers);
        }
        
        return new ReadOnlyDictionary<string, WebRtcInvitationOffer>(new Dictionary<string, WebRtcInvitationOffer>());
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, WebRtcInvitationAnswer>> GetAnswersAsync(string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docManager = orchestrator.GetDocument<CrdtSignalingState>(ResolveDocumentId(documentId));
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            return new ReadOnlyDictionary<string, WebRtcInvitationAnswer>(crdtDoc.Data.Answers);
        }
        
        return new ReadOnlyDictionary<string, WebRtcInvitationAnswer>(new Dictionary<string, WebRtcInvitationAnswer>());
    }

    /// <inheritdoc />
    public async Task SetOfferAsync(string routingKey, WebRtcInvitationOffer offer, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        
        var docId = ResolveDocumentId(documentId);
        var docManager = await GetOrCreateDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Offers, new MapSetIntent(routingKey, offer), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveOfferAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        
        var docId = ResolveDocumentId(documentId);
        var docManager = orchestrator.GetDocument<CrdtSignalingState>(docId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Offers, new MapRemoveIntent(routingKey), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task SetAnswerAsync(string routingKey, WebRtcInvitationAnswer answer, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);

        var docId = ResolveDocumentId(documentId);
        var docManager = await GetOrCreateDocumentAsync(docId, cancellationToken).ConfigureAwait(false);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Answers, new MapSetIntent(routingKey, answer), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public async Task RemoveAnswerAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);

        var docId = ResolveDocumentId(documentId);
        var docManager = orchestrator.GetDocument<CrdtSignalingState>(docId);
        if (docManager != null)
        {
            var crdtDoc = await docManager.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
            var operation = await patcher.GenerateOperationAsync(crdtDoc, x => x.Answers, new MapRemoveIntent(routingKey), cancellationToken).ConfigureAwait(false);
            var patch = new CrdtPatch(new[] { operation });

            await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
        }
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