namespace Ama.Enterprise.P2p.Mqtt.Services;

using System;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Internally manages the active MQTT connection and topic subscriptions for the local peer.
/// </summary>
public interface IMqttClientManager
{
    /// <summary>
    /// Event triggered when an inbound payload is received from the subscribed MQTT topic.
    /// </summary>
    event Func<byte[], Task>? OnMessageReceived;

    /// <summary>
    /// Starts the MQTT client and establishes subscriptions.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task StartAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Stops the MQTT client and severs connections.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task StopAsync(CancellationToken cancellationToken);

    /// <summary>
    /// Publishes a payload to a specific target peer via their assigned MQTT topic.
    /// </summary>
    /// <param name="targetClientId">The remote peer's client ID.</param>
    /// <param name="payload">The message payload to publish.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous publish operation.</returns>
    Task PublishAsync(string targetClientId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}