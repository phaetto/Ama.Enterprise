namespace Ama.Enterprise.FeatureFlags.Models;

using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.WebRTC.Models;

/// <summary>
/// Configuration options for the feature flags module.
/// </summary>
public sealed class FeatureFlagOptions
{
    /// <summary>
    /// Gets or sets the distributed CRDT options configuration.
    /// </summary>
    public DistributedCrdtOptions Crdt { get; set; } = new();

    /// <summary>
    /// Gets or sets the generic HTTP transport layer options configuration.
    /// </summary>
    public HttpTransportOptions Http { get; set; } = new();

    /// <summary>
    /// Gets or sets the UDP multicast peer discovery options configuration.
    /// </summary>
    public UdpDiscoveryOptions UdpDiscovery { get; set; } = new();

    /// <summary>
    /// Gets or sets the gossip protocol options configuration.
    /// </summary>
    public GossipOptions Gossip { get; set; } = new();

    /// <summary>
    /// Gets or sets the WebRTC transport options configuration.
    /// </summary>
    public WebRtcOptions WebRtc { get; set; } = new();
}