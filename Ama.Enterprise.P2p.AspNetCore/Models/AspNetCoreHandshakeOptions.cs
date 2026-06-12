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

    /// <summary>
    /// Gets or sets a value indicating whether to strictly broadcast HTTPS schemes mapping generic outbound clients for Phase 2 handshakes.
    /// </summary>
    public bool UseHttps { get; set; } = false;

    /// <summary>
    /// Gets or sets a value indicating whether the Standalone hosting mode should bind using HTTPS.
    /// </summary>
    public bool UseHttpsStandalone { get; set; } = false;

    /// <summary>
    /// Gets or sets the file path to the X.509 certificate used for Standalone HTTPS bindings.
    /// </summary>
    public string? CertificateFilePath { get; set; }

    /// <summary>
    /// Gets or sets the password for the certificate file if it is encrypted.
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Gets or sets the certificate thumbprint to load from the local Certificate Store for HTTPS bindings.
    /// </summary>
    public string? CertificateThumbprint { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether outbound HTTP requests should ignore SSL validation errors (e.g., self-signed certificates).
    /// </summary>
    public bool IgnoreOutboundSslErrors { get; set; } = false;
}