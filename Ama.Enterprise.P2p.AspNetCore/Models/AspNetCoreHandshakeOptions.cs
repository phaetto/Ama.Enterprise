namespace Ama.Enterprise.P2p.AspNetCore.Models;

using System;

/// <summary>
/// Configuration structure for isolated ASP.NET Core handshaking parameters explicitly tracking topologies dynamically.
/// </summary>
public sealed record AspNetCoreHandshakeOptions
{
    /// <summary>
    /// Gets or sets the decoupled hosting mode establishing Phase 2 handshake receivers.
    /// </summary>
    public AspNetCoreHostingMode HostingMode { get; set; } = AspNetCoreHostingMode.Standalone;

    /// <summary>
    /// The port broadcasted globally to remote peers during Phase 1 discovery explicitly identifying routing topology natively.
    /// </summary>
    public int AdvertisedHandshakePort { get; set; } = 8081;

    /// <summary>
    /// The internal host or IP address binding isolated Kestrel handshakers when explicitly evaluated in Standalone mode. Defaults to "+" (Any IP).
    /// </summary>
    public string StandaloneListenHost { get; set; } = "+";

    /// <summary>
    /// The locally assigned port executing underlying inbound isolated handshakes executing strictly during Standalone boundaries natively.
    /// </summary>
    public int StandaloneListenPort { get; set; } = 8081;

    /// <summary>
    /// The HTTP base route path prefix mapping distinct decoupled handshakes cleanly natively. Defaults to "/ama-enterprise/p2p-handshake".
    /// </summary>
    public string PathPrefix { get; set; } = "/ama-enterprise/p2p-handshake";

    /// <summary>
    /// The timeout duration safely isolating unstable decoupled outbound handshakes explicitly.
    /// </summary>
    public TimeSpan HandshakeTimeout { get; set; } = TimeSpan.FromSeconds(5);
}