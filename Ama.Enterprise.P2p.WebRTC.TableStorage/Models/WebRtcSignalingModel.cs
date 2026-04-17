namespace Ama.Enterprise.P2p.WebRTC.TableStorage.Models;

using System;
using Azure;

/// <summary>
/// Strongly typed representation of a WebRTC signaling table entity decoupled from reflection.
/// Represents an SDP offer or answer used to negotiate a direct peer-to-peer connection 
/// between two nodes, establishing the links required for a full mesh network.
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