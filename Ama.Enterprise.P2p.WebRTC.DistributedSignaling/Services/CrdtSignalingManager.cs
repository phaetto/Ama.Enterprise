namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Models;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Implementation managing the WebRTC out-of-band signaling drop-box avoiding centralized datastores enforcing explicit decoupled behavior mappings via the Stateless Orchestrator Gateway directly.
/// </summary>
public sealed class CrdtSignalingManager : ICrdtSignalingManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtSignalingManager"/> class.
    /// </summary>
    /// <param name="orchestrator">The document orchestrator mapping internal CRDT lifecycles.</param>
    public CrdtSignalingManager(ICrdtDocumentOrchestrator orchestrator)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, long>> GetJoinIntentsAsync(string? documentId = null, CancellationToken cancellationToken = default)
    {
        var crdtDoc = await orchestrator.GetDocumentStateAsync<CrdtSignalingState>(ResolveDocumentId(documentId), cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data != null && crdtDoc.Data.JoinIntents != null)
        {
            return new ReadOnlyDictionary<string, long>(crdtDoc.Data.JoinIntents);
        }
        
        return new ReadOnlyDictionary<string, long>(new Dictionary<string, long>());
    }

    /// <inheritdoc />
    public async Task SetJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docId = ResolveDocumentId(documentId);
        await EnsureDocumentExistsAsync(docId, cancellationToken).ConfigureAwait(false);
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var intent = new MapSetIntent(peerId.ToString(), timestamp);
        await orchestrator.ForwardIntentAsync<CrdtSignalingState>(docId, intent, "JoinIntents", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveJoinIntentAsync(Guid peerId, string? documentId = null, CancellationToken cancellationToken = default)
    {
        var docId = ResolveDocumentId(documentId);
        var intent = new MapRemoveIntent(peerId.ToString());
        await orchestrator.ForwardIntentAsync<CrdtSignalingState>(docId, intent, "JoinIntents", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, WebRtcInvitationOffer>> GetOffersAsync(string? documentId = null, CancellationToken cancellationToken = default)
    {
        var crdtDoc = await orchestrator.GetDocumentStateAsync<CrdtSignalingState>(ResolveDocumentId(documentId), cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data != null && crdtDoc.Data.Offers != null)
        {
            return new ReadOnlyDictionary<string, WebRtcInvitationOffer>(crdtDoc.Data.Offers);
        }
        
        return new ReadOnlyDictionary<string, WebRtcInvitationOffer>(new Dictionary<string, WebRtcInvitationOffer>());
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, WebRtcInvitationAnswer>> GetAnswersAsync(string? documentId = null, CancellationToken cancellationToken = default)
    {
        var crdtDoc = await orchestrator.GetDocumentStateAsync<CrdtSignalingState>(ResolveDocumentId(documentId), cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data != null && crdtDoc.Data.Answers != null)
        {
            return new ReadOnlyDictionary<string, WebRtcInvitationAnswer>(crdtDoc.Data.Answers);
        }
        
        return new ReadOnlyDictionary<string, WebRtcInvitationAnswer>(new Dictionary<string, WebRtcInvitationAnswer>());
    }

    /// <inheritdoc />
    public async Task SetOfferAsync(string routingKey, WebRtcInvitationOffer offer, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        
        var docId = ResolveDocumentId(documentId);
        await EnsureDocumentExistsAsync(docId, cancellationToken).ConfigureAwait(false);
        var intent = new MapSetIntent(routingKey, offer);
        await orchestrator.ForwardIntentAsync<CrdtSignalingState>(docId, intent, "Offers", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveOfferAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);
        
        var docId = ResolveDocumentId(documentId);
        var intent = new MapRemoveIntent(routingKey);
        await orchestrator.ForwardIntentAsync<CrdtSignalingState>(docId, intent, "Offers", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SetAnswerAsync(string routingKey, WebRtcInvitationAnswer answer, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);

        var docId = ResolveDocumentId(documentId);
        await EnsureDocumentExistsAsync(docId, cancellationToken).ConfigureAwait(false);
        var intent = new MapSetIntent(routingKey, answer);
        await orchestrator.ForwardIntentAsync<CrdtSignalingState>(docId, intent, "Answers", cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveAnswerAsync(string routingKey, string? documentId = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(routingKey);

        var docId = ResolveDocumentId(documentId);
        var intent = new MapRemoveIntent(routingKey);
        await orchestrator.ForwardIntentAsync<CrdtSignalingState>(docId, intent, "Answers", cancellationToken).ConfigureAwait(false);
    }

    private static string ResolveDocumentId(string? documentId)
    {
        return string.IsNullOrWhiteSpace(documentId) ? Constants.DefaultSignalingDocumentId : documentId;
    }

    private async Task EnsureDocumentExistsAsync(string documentId, CancellationToken cancellationToken)
    {
        var doc = orchestrator.TryGetLocalDocument<CrdtSignalingState>(documentId);
        if (doc == null)
        {
            await orchestrator.CreateDocumentAsync(documentId, Constants.SignalingDocumentTypeAlias, cancellationToken).ConfigureAwait(false);
        }
    }
}