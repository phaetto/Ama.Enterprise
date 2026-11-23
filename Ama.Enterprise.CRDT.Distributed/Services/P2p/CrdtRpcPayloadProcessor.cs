namespace Ama.Enterprise.CRDT.Distributed.Services.P2p;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Default implementation of <see cref="ICrdtRpcPayloadProcessor"/> handling inbound P2P RPC envelopes.
/// </summary>
public sealed class CrdtRpcPayloadProcessor : ICrdtRpcPayloadProcessor
{
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<CrdtRpcPayloadProcessor> logger;

    public CrdtRpcPayloadProcessor(
        ICrdtSerializer serializer,
        ILogger<CrdtRpcPayloadProcessor> logger)
    {
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task ProcessRpcPayloadAsync(
        IDistributedCrdtScope scope,
        string meshId,
        CrdtMessageWrapper wrapper,
        PeerId senderId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        ArgumentNullException.ThrowIfNull(wrapper);

        var wireScopeId = !string.IsNullOrWhiteSpace(wrapper.ScopeId)
            ? wrapper.ScopeId
            : (!string.IsNullOrWhiteSpace(scope.ScopeId) && !string.Equals(scope.ScopeId, scope.HostReplicaId, StringComparison.Ordinal)
                ? scope.ScopeId
                : null);
        var senderReplicaId = scope.ServiceProvider.GetService<ReplicaContext>()?.ReplicaId ?? scope.ReplicaId;

        try
        {
            if (wrapper.MessageType == "CrdtReadReq")
            {
                var req = serializer.DeserializeFromBytes<CrdtReadRequestDto>(wrapper.Payload!);
                var typeAlias = !string.IsNullOrWhiteSpace(req.DocumentTypeAlias) ? req.DocumentTypeAlias : wrapper.DocumentTypeAlias;
                IDistributedCrdtDocument? targetDoc = null;

                if (!string.IsNullOrWhiteSpace(req.DocumentId) &&
                    scope.Orchestrator.InitializationStage == OrchestratorInitializationStage.Initialized)
                {
                    var activeDocs = scope.Orchestrator.GetActiveDocuments();
                    for (int i = 0; i < activeDocs.Count; i++)
                    {
                        var doc = activeDocs[i];
                        if (string.Equals(doc.DocumentId, req.DocumentId, StringComparison.Ordinal) &&
                            (string.IsNullOrWhiteSpace(typeAlias) || string.Equals(doc.TypeAlias, typeAlias, StringComparison.Ordinal)))
                        {
                            targetDoc = doc;
                            break;
                        }
                    }
                }

                byte[] statePayload = [];
                if (targetDoc != null)
                {
                    var snapshot = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                    statePayload = snapshot.SnapshotData;
                }
                else if (!string.IsNullOrWhiteSpace(req.DocumentId))
                {
                    statePayload = await scope.Orchestrator.TryLoadStoredDocumentSnapshotBytesAsync(req.DocumentId, typeAlias, cancellationToken).ConfigureAwait(false);
                    if (statePayload.Length > 0)
                    {
                        targetDoc = await scope.Orchestrator.ResolveOrPromoteLocalDocumentAsync(req.DocumentId, typeAlias, cancellationToken).ConfigureAwait(false);
                        if (targetDoc != null)
                        {
                            var snapshot = await targetDoc.GetSnapshotDataAsync(cancellationToken).ConfigureAwait(false);
                            statePayload = snapshot.SnapshotData;
                        }
                    }
                }

                var res = new CrdtReadResponseDto { CorrelationId = req.CorrelationId, StatePayload = statePayload };
                var resPayload = serializer.SerializeToBytes(res);
                var resWrapper = new CrdtMessageWrapper(req.DocumentId, "CrdtReadRes", resPayload, senderReplicaId, typeAlias, wireScopeId);
                var directSender = scope.ServiceProvider.GetRequiredService<IDirectMessageSender>();
                await directSender.SendDirectAsync(senderId, serializer.SerializeToBytes(resWrapper), cancellationToken).ConfigureAwait(false);
            }
            else if (wrapper.MessageType == "CrdtReadRes")
            {
                var res = serializer.DeserializeFromBytes<CrdtReadResponseDto>(wrapper.Payload!);
                var rpcCorrelator = scope.ServiceProvider.GetRequiredService<IP2pRpcCorrelator>();
                rpcCorrelator.ResolveRequest(res.CorrelationId, wrapper.Payload!);
            }
            else if (wrapper.MessageType == "CrdtIntentReq")
            {
                var req = serializer.DeserializeFromBytes<CrdtIntentForwardRequestDto>(wrapper.Payload!);
                var typeAlias = !string.IsNullOrWhiteSpace(req.DocumentTypeAlias) ? req.DocumentTypeAlias : wrapper.DocumentTypeAlias;
                var targetDoc = string.IsNullOrWhiteSpace(req.DocumentId)
                    ? null
                    : await scope.Orchestrator.ResolveOrPromoteLocalDocumentAsync(req.DocumentId, typeAlias, cancellationToken).ConfigureAwait(false);
                var isFireAndForget = req.CorrelationId == Guid.Empty;
                var deliveryMode = isFireAndForget ? IntentDeliveryMode.FireAndForget : IntentDeliveryMode.Acknowledged;

                var isSuccess = false;
                var errorMessage = string.Empty;
                if (targetDoc != null)
                {
                    try
                    {
                        var intent = serializer.DeserializeFromBytes<IOperationIntent>(req.SerializedIntent);
                        var patcher = scope.ServiceProvider.GetRequiredService<IAsyncCrdtPatcher>();

                        isSuccess = await targetDoc.ApplyRemoteIntentAsync(intent, req.TargetProperty, patcher, deliveryMode, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        errorMessage = ex.Message;
                        logger.LogError(ex, "Failed to apply forwarded intent on target document {DocumentId}.", req.DocumentId);
                    }
                }
                else
                {
                    errorMessage = $"Target document '{req.DocumentId}' is not active on this node.";
                }

                if (!isFireAndForget)
                {
                    var res = new CrdtIntentForwardResponseDto { CorrelationId = req.CorrelationId, IsSuccess = isSuccess, ErrorMessage = errorMessage };
                    var resPayload = serializer.SerializeToBytes(res);
                    var resWrapper = new CrdtMessageWrapper(req.DocumentId, "CrdtIntentRes", resPayload, senderReplicaId, ScopeId: wireScopeId);
                    var directSender = scope.ServiceProvider.GetRequiredService<IDirectMessageSender>();
                    await directSender.SendDirectAsync(senderId, serializer.SerializeToBytes(resWrapper), cancellationToken).ConfigureAwait(false);
                }
            }
            else if (wrapper.MessageType == "CrdtIntentRes")
            {
                var res = serializer.DeserializeFromBytes<CrdtIntentForwardResponseDto>(wrapper.Payload!);
                var rpcCorrelator = scope.ServiceProvider.GetRequiredService<IP2pRpcCorrelator>();
                rpcCorrelator.ResolveRequest(res.CorrelationId, wrapper.Payload!);
            }
            else if (wrapper.MessageType == "CrdtMapReduceReq")
            {
                var req = serializer.DeserializeFromBytes<CrdtMapReduceRequestDto>(wrapper.Payload!);
                var typeAlias = !string.IsNullOrWhiteSpace(req.DocumentTypeAlias) ? req.DocumentTypeAlias : wrapper.DocumentTypeAlias;
                CrdtMapReduceResponseDto responseDto;

                var executor = scope.ServiceProvider.GetKeyedService<ICrdtMapReduceExecutor>(req.JobAlias);
                if (executor is null)
                {
                    responseDto = new CrdtMapReduceResponseDto
                    {
                        CorrelationId = req.CorrelationId,
                        IsSuccess = false,
                        HasAccumulator = false,
                        EvaluatedDocumentCount = 0,
                        SerializedAccumulator = Array.Empty<byte>(),
                        ErrorMessage = $"Map/Reduce job alias '{req.JobAlias}' is not registered on replica '{scope.ReplicaId}'."
                    };
                }
                else
                {
                    try
                    {
                        var docList = req.DocumentIds as IReadOnlyList<string> ?? req.DocumentIds.ToList();
                        var outcome = await executor.ExecuteSerializedLocalBatchAsync(
                            scope.ServiceProvider,
                            docList,
                            req.SerializedInput,
                            cancellationToken).ConfigureAwait(false);

                        responseDto = new CrdtMapReduceResponseDto
                        {
                            CorrelationId = req.CorrelationId,
                            IsSuccess = outcome.IsSuccess,
                            HasAccumulator = outcome.HasAccumulator,
                            EvaluatedDocumentCount = outcome.EvaluatedDocumentCount,
                            SerializedAccumulator = outcome.SerializedAccumulator,
                            MissingDocumentIds = outcome.MissingDocumentIds,
                            ErrorMessage = outcome.ErrorMessage
                        };
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Failed to execute local Map/Reduce batch for job {JobAlias}.", req.JobAlias);
                        responseDto = new CrdtMapReduceResponseDto
                        {
                            CorrelationId = req.CorrelationId,
                            IsSuccess = false,
                            HasAccumulator = false,
                            EvaluatedDocumentCount = 0,
                            SerializedAccumulator = Array.Empty<byte>(),
                            ErrorMessage = ex.Message
                        };
                    }
                }

                var resPayload = serializer.SerializeToBytes(responseDto);
                var resWrapper = new CrdtMessageWrapper(wrapper.DocumentId, "CrdtMapReduceRes", resPayload, senderReplicaId, typeAlias, wireScopeId);
                var directSender = scope.ServiceProvider.GetRequiredService<IDirectMessageSender>();
                await directSender.SendDirectAsync(senderId, serializer.SerializeToBytes(resWrapper), cancellationToken).ConfigureAwait(false);
            }
            else if (wrapper.MessageType == "CrdtMapReduceRes")
            {
                var res = serializer.DeserializeFromBytes<CrdtMapReduceResponseDto>(wrapper.Payload!);
                var rpcCorrelator = scope.ServiceProvider.GetRequiredService<IP2pRpcCorrelator>();
                rpcCorrelator.ResolveRequest(res.CorrelationId, wrapper.Payload!);
            }
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to process asynchronous RPC payload.");
        }
    }
}