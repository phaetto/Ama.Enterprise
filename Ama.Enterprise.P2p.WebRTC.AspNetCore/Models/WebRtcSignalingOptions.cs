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
}