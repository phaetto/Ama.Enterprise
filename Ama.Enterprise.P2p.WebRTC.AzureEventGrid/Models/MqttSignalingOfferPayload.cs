namespace Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Models;

using System;

/// <summary>
/// Data structure representing an outbound WebRTC offer published over MQTT.
/// </summary>
public readonly record struct MqttSignalingOfferPayload(
    string ConnectionId,
    string CreatorPeerId,
    string OfferSdp,
    DateTimeOffset CreatedAt);