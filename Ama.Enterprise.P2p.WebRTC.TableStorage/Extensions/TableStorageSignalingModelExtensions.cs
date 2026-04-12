namespace Ama.Enterprise.P2p.WebRTC.TableStorage.Extensions;

using System;
using Ama.Enterprise.P2p.WebRTC.TableStorage.Models;
using Azure.Data.Tables;

/// <summary>
/// Extension methods explicitly mapping strongly typed signaling models to TableEntities avoiding reflection completely safely.
/// </summary>
public static class TableStorageSignalingModelExtensions
{
    /// <summary>
    /// Converts a native TableEntity to a strongly typed WebRtcSignalingModel safely.
    /// </summary>
    /// <param name="entity">The Azure Table Storage entity.</param>
    /// <returns>The strongly typed signaling model.</returns>
    /// <exception cref="ArgumentNullException">Thrown when the entity is null.</exception>
    public static WebRtcSignalingModel ToModel(this TableEntity entity)
    {
        if (entity is null)
        {
            throw new ArgumentNullException(nameof(entity));
        }

        Guid creatorId = Guid.Empty;
        var creatorIdString = entity.GetString("CreatorPeerId");
        if (!string.IsNullOrWhiteSpace(creatorIdString))
        {
            _ = Guid.TryParse(creatorIdString, out creatorId);
        }

        Guid? responderId = null;
        var responderIdString = entity.GetString("ResponderPeerId");
        if (!string.IsNullOrWhiteSpace(responderIdString) && Guid.TryParse(responderIdString, out var parsedResponderId))
        {
            responderId = parsedResponderId;
        }

        return new WebRtcSignalingModel(
            MeshId: entity.PartitionKey,
            ConnectionId: entity.RowKey,
            CreatorPeerId: creatorId,
            OfferSdp: entity.GetString("OfferSdp") ?? string.Empty,
            CreatedAt: entity.GetDateTimeOffset("CreatedAt") ?? DateTimeOffset.UtcNow,
            AnswerSdp: entity.GetString("AnswerSdp"),
            ResponderPeerId: responderId,
            ETag: entity.ETag
        );
    }

    /// <summary>
    /// Converts a strongly typed WebRtcSignalingModel into a native Azure TableEntity natively.
    /// </summary>
    /// <param name="model">The strongly typed signaling model.</param>
    /// <returns>The mapped TableEntity.</returns>
    public static TableEntity ToTableEntity(this WebRtcSignalingModel model)
    {
        var entity = new TableEntity(model.MeshId, model.ConnectionId)
        {
            { "CreatorPeerId", model.CreatorPeerId.ToString() },
            { "OfferSdp", model.OfferSdp },
            { "CreatedAt", model.CreatedAt }
        };

        if (!string.IsNullOrWhiteSpace(model.AnswerSdp))
        {
            entity["AnswerSdp"] = model.AnswerSdp;
        }

        if (model.ResponderPeerId.HasValue)
        {
            entity["ResponderPeerId"] = model.ResponderPeerId.Value.ToString();
        }

        if (model.ETag != default)
        {
            entity.ETag = model.ETag;
        }

        return entity;
    }
}