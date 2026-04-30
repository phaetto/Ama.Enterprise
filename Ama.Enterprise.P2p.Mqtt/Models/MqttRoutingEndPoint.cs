namespace Ama.Enterprise.P2p.Mqtt.Models;

using System;
using System.Net;

/// <summary>
/// Custom EndPoint representing an MQTT routing target mapped by its unique Client ID.
/// </summary>
public sealed class MqttRoutingEndPoint : EndPoint
{
    /// <summary>
    /// Gets the MQTT client identifier utilized for direct payload routing.
    /// </summary>
    public string ClientId { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="MqttRoutingEndPoint"/> class.
    /// </summary>
    /// <param name="clientId">The client identifier targeting the specific peer node.</param>
    /// <exception cref="ArgumentNullException">Thrown if the client identifier is null.</exception>
    public MqttRoutingEndPoint(string clientId)
    {
        ClientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
    }

    /// <inheritdoc />
    public override string ToString() => $"MqttRoutingEndPoint:{ClientId}";
}