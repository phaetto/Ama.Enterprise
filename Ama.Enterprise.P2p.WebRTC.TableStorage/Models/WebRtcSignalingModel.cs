namespace Ama.Enterprise.P2p.WebRTC.TableStorage.Models;

using System;
using Azure;

/// <summary>
/// Strongly typed representation of a WebRTC signaling table entity decoupled from reflection natively.
/// </summary>
public readonly record struct WebRtcSignalingModel(
    string MeshId,
    string ConnectionId,
    Guid CreatorPeerId,
    string OfferSdp,
    DateTimeOffset CreatedAt,
    string? AnswerSdp,
    Guid? ResponderPeerId,
    ETag ETag) : IEquatable<WebRtcSignalingModel>;