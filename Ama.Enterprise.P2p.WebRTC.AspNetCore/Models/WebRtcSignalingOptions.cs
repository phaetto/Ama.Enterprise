namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;

using Ama.Enterprise.P2p.AspNetCore.Models;

/// <summary>
/// Configuration structure for isolated ASP.NET Core WebRTC signaling HTTP parameters tracking decoupled bounds.
/// </summary>
public sealed record WebRtcSignalingOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether out-of-band WebRTC HTTP signaling is actively evaluated.
    /// </summary>
    public bool IsEnabled { get; set; }

    /// <summary>
    /// Determines whether the ASP.NET Core signaling relies on the host application's HTTP pipeline explicitly routed via MapP2pWebRtcSignalingEndpoints() or spins up an isolated Standalone Kestrel web server.
    /// </summary>
    public AspNetCoreHostingMode HostingMode { get; set; } = AspNetCoreHostingMode.Integrated;

    /// <summary>
    /// Gets or sets the HTTP URL route path prefix bound explicitly tracking HTTP probes dynamically.
    /// </summary>
    public string PathPrefix { get; set; } = "/ama-enterprise/webrtc-signaling";

    /// <summary>
    /// The network interface IP host dynamically capturing traffic mapped exclusively for decoupled Standalone scenarios.
    /// </summary>
    public string StandaloneListenHost { get; set; } = "+";

    /// <summary>
    /// The active network port tracking standard listener bindings explicitly assigned.
    /// </summary>
    public int StandaloneListenPort { get; set; } = 8080;

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
    /// Gets or sets a value indicating whether outbound WebSockets requests should ignore SSL validation errors (e.g., self-signed certificates).
    /// </summary>
    public bool IgnoreOutboundSslErrors { get; set; } = false;
}