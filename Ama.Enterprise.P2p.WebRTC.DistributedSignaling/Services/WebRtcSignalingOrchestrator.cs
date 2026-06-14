namespace Ama.Enterprise.P2p.WebRTC.DistributedSignaling.Services;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of the high-level orchestrator coordinating WebRTC signaling components actively monitoring logical mesh presence.
/// </summary>
public sealed class WebRtcSignalingOrchestrator : IWebRtcSignalingOrchestrator
{
    private readonly IServiceProvider serviceProvider;
    private readonly IWebRtcDistributedSignalingClient signalingClient;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptions;
    private readonly ILogger<WebRtcSignalingOrchestrator> logger;

    private readonly ConcurrentDictionary<string, bool> processedIntents = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, Guid> publishedOffers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> processedOffers = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, bool> processedAnswers = new(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcSignalingOrchestrator"/> class.
    /// </summary>
    /// <param name="serviceProvider">The service provider to resolve keyed mesh services.</param>
    /// <param name="signalingClient">The HTTP signaling client mapped to the CRDT mesh.</param>
    /// <param name="nodeOptions">The options representing underlying mapped identities.</param>
    /// <param name="logger">The system logger instance.</param>
    public WebRtcSignalingOrchestrator(
        IServiceProvider serviceProvider,
        IWebRtcDistributedSignalingClient signalingClient,
        IOptionsMonitor<P2pNodeOptions> nodeOptions,
        ILogger<WebRtcSignalingOrchestrator> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.signalingClient = signalingClient ?? throw new ArgumentNullException(nameof(signalingClient));
        this.nodeOptions = nodeOptions ?? throw new ArgumentNullException(nameof(nodeOptions));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task PublishJoinIntentAsync(string meshId, string replicaId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(replicaId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var localPeerId = nodeOptions.Get(meshId).LocalPeerId;
        await signalingClient.SetJoinIntentAsync(replicaId, localPeerId, documentId, cancellationToken).ConfigureAwait(false);
        logger.LogInformation("[{MeshId}] Published WebRTC join intent for local peer {LocalPeerId} in signaling document {DocumentId} against replica {ReplicaId}.", meshId, localPeerId, documentId, replicaId);
    }

    /// <inheritdoc />
    public async Task ProcessJoinIntentsAsync(string meshId, string replicaId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(replicaId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var intents = await signalingClient.GetJoinIntentsAsync(replicaId, documentId, cancellationToken).ConfigureAwait(false);
        var localPeerId = nodeOptions.Get(meshId).LocalPeerId;
        var invitationService = serviceProvider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);

        foreach (var kvp in intents)
        {
            if (Guid.TryParse(kvp.Key, out var targetPeerId) && targetPeerId != localPeerId)
            {
                // Lexicographical ordering to prevent WebRTC offer glare (both nodes offering each other).
                // Only the logically "smaller" ID initiates the specific offer, the other node awaits internally.
                if (localPeerId.CompareTo(targetPeerId) < 0)
                {
                    var routingKey = $"{targetPeerId}:{localPeerId}";

                    // Check if we haven't already processed an intent sending an offer toward this specific peer
                    if (processedIntents.TryAdd(routingKey, true))
                    {
                        try
                        {
                            var offer = await invitationService.CreateInvitationAsync(cancellationToken).ConfigureAwait(false);
                            publishedOffers[routingKey] = offer.ConnectionId;

                            await signalingClient.SetOfferAsync(replicaId, routingKey, offer, documentId, cancellationToken).ConfigureAwait(false);
                            logger.LogInformation("[{MeshId}] Published WebRTC offer directed to {TargetPeerId} in signaling document {DocumentId} targeting replica {ReplicaId}.", meshId, targetPeerId, documentId, replicaId);
                        }
                        catch (Exception ex)
                        {
                            logger.LogError(ex, "[{MeshId}] Failed to process join intent generating offer for peer {TargetPeerId} in signaling document {DocumentId} via replica {ReplicaId}.", meshId, targetPeerId, documentId, replicaId);
                            processedIntents.TryRemove(routingKey, out _); // Allow subsequent polling execution retry
                            publishedOffers.TryRemove(routingKey, out _); // Clean up the orphaned mapping
                        }
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task ProcessPendingOffersAsync(string meshId, string replicaId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(replicaId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var offers = await signalingClient.GetOffersAsync(replicaId, documentId, cancellationToken).ConfigureAwait(false);
        var localPeerId = nodeOptions.Get(meshId).LocalPeerId;
        var invitationService = serviceProvider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);
        
        var targetPrefix = $"{localPeerId}:";

        foreach (var kvp in offers)
        {
            var routingKey = kvp.Key;
            
            if (routingKey.StartsWith(targetPrefix, StringComparison.OrdinalIgnoreCase))
            {
                if (processedOffers.TryAdd(routingKey, true))
                {
                    try
                    {
                        var offererPeerIdStr = routingKey.Substring(targetPrefix.Length);
                        var answerRoutingKey = $"{offererPeerIdStr}:{localPeerId}";

                        var answer = await invitationService.AcceptInvitationAsync(kvp.Value.SdpOffer, cancellationToken).ConfigureAwait(false);
                        await signalingClient.SetAnswerAsync(replicaId, answerRoutingKey, answer, documentId, cancellationToken).ConfigureAwait(false);
                        
                        logger.LogInformation("[{MeshId}] Answered WebRTC offer natively targeted from {OffererPeerId} in signaling document {DocumentId} via replica {ReplicaId}.", meshId, offererPeerIdStr, documentId, replicaId);

                        // Extract and clean up the consumed offer tracking minimal remote CRDT structural constraints internally.
                        await signalingClient.RemoveOfferAsync(replicaId, routingKey, documentId, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "[{MeshId}] Failed to properly process targeted mapped offer {RoutingKey} in signaling document {DocumentId} tracking replica {ReplicaId}.", meshId, routingKey, documentId, replicaId);
                        processedOffers.TryRemove(routingKey, out _);
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public async Task ProcessPendingAnswersAsync(string meshId, string replicaId, string documentId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(replicaId);
        ArgumentException.ThrowIfNullOrWhiteSpace(documentId);

        var answers = await signalingClient.GetAnswersAsync(replicaId, documentId, cancellationToken).ConfigureAwait(false);
        var localPeerId = nodeOptions.Get(meshId).LocalPeerId;
        var invitationService = serviceProvider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);

        var targetPrefix = $"{localPeerId}:";

        foreach (var kvp in answers)
        {
            var routingKey = kvp.Key;

            if (routingKey.StartsWith(targetPrefix, StringComparison.OrdinalIgnoreCase))
            {
                var answererPeerIdStr = routingKey.Substring(targetPrefix.Length);
                var originalOfferRoutingKey = $"{answererPeerIdStr}:{localPeerId}";

                // Ensure strict boundary constraints executing exclusively against generated internally originating targeted bounds
                if (publishedOffers.TryGetValue(originalOfferRoutingKey, out var localConnectionId) && processedAnswers.TryAdd(routingKey, true))
                {
                    try
                    {
                        await invitationService.FinalizeInvitationAsync(localConnectionId, kvp.Value.SdpAnswer, cancellationToken).ConfigureAwait(false);
                        logger.LogInformation("[{MeshId}] Finalized WebRTC connection targeting {AnswererPeerId} in signaling document {DocumentId} bound to replica {ReplicaId}.", meshId, answererPeerIdStr, documentId, replicaId);

                        // Eliminate consumed tracking dependencies optimizing state constraints across the shared drop-box bounds
                        await signalingClient.RemoveAnswerAsync(replicaId, routingKey, documentId, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "[{MeshId}] Failed to correctly finalize native connection bounds mapping answer from {AnswererPeerId} in signaling document {DocumentId} via replica {ReplicaId}.", meshId, answererPeerIdStr, documentId, replicaId);
                        processedAnswers.TryRemove(routingKey, out _);
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public void ClearLocalState()
    {
        processedIntents.Clear();
        publishedOffers.Clear();
        processedOffers.Clear();
        processedAnswers.Clear();
        logger.LogInformation("Cleared logically tracked local dictionary boundaries for WebRTC targeted signaling orchestrator.");
    }
}