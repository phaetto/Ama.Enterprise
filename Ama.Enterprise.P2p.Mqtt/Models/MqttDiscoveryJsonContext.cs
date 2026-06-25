namespace Ama.Enterprise.P2p.Mqtt.Models;

using System.Text.Json.Serialization;

/// <summary>
/// Source-generated JSON serialization context resolving Phase 1 and Phase 2 MQTT generic primitive structures.
/// </summary>
[JsonSerializable(typeof(MqttDiscoveryMessage))]
[JsonSerializable(typeof(MqttHandshakeMessage))]
public sealed partial class MqttDiscoveryJsonContext : JsonSerializerContext
{
}