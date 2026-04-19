namespace Ama.Enterprise.P2p.WebRTC.AzureEventGrid.Models;

using System;

/// <summary>
/// Configuration options for the MQTT-based WebRTC signaling mechanism.
/// </summary>
public sealed class MqttSignalingOptions
{
    /// <summary>
    /// Gets or sets the MQTT broker hostname (e.g., Azure Event Grid MQTT namespace).
    /// </summary>
    public string Host { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the MQTT broker port. Defaults to 8883 for TLS.
    /// </summary>
    public int Port { get; set; } = 8883;

    /// <summary>
    /// Gets or sets a value indicating whether TLS is enabled. Recommended for Event Grid.
    /// </summary>
    public bool UseTls { get; set; } = true;

    /// <summary>
    /// Gets or sets the MQTT Client ID.
    /// </summary>
    public string ClientId { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// Gets or sets the username for MQTT authentication.
    /// </summary>
    public string Username { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the password for MQTT authentication.
    /// </summary>
    public string Password { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets a value indicating whether this node should actively generate outbound WebRTC offers.
    /// </summary>
    public bool EnableOfferGeneration { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether this node should accept inbound WebRTC offers.
    /// </summary>
    public bool EnableOfferAcceptance { get; set; } = true;

    /// <summary>
    /// Gets or sets the interval at which the node generates new offers if disconnected or searching.
    /// </summary>
    public TimeSpan OfferInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    /// Gets or sets the maximum age of an offer before it is considered expired and discarded.
    /// </summary>
    public TimeSpan OfferExpiration { get; set; } = TimeSpan.FromSeconds(60);
}