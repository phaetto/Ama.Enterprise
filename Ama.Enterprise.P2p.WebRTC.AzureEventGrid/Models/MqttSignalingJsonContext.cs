namespace Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Models;

using System.Text.Json.Serialization;

/// <summary>
/// JSON serialization context ensuring AOT compatibility for MQTT signaling payloads.
/// </summary>
[JsonSerializable(typeof(MqttSignalingOfferPayload))]
[JsonSerializable(typeof(MqttSignalingAnswerPayload))]
public sealed partial class MqttSignalingJsonContext : JsonSerializerContext
{
}