namespace Ama.Enterprise.P2p.Kestrel.Models;

using System;

/// <summary>
/// Configuration structure for isolated Kestrel handshaking parameters.
/// </summary>
public sealed record KestrelHandshakeOptions
{
    /// <summary>
    /// The host or IP address to bind the Kestrel handshaker to. Defaults to "+" (Any IP).
    /// </summary>
    public string ListenHost { get; set; } = "+";

    /// <summary>
    /// The port to listen on for incoming Kestrel handshakes.
    /// </summary>
    public int ListenPort { get; set; } = 8081;

    /// <summary>
    /// The timeout duration for an outbound Kestrel handshake.
    /// </summary>
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(5);
}