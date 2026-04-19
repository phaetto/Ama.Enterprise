namespace Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Models;
/// <summary>
/// Data structure representing a WebRTC answer payload published over MQTT.
/// </summary>
public readonly record struct MqttSignalingAnswerPayload(
    string ConnectionId,
    string ResponderPeerId,
    string AnswerSdp);