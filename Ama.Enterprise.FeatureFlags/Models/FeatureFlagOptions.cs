namespace Ama.Enterprise.FeatureFlags.Models;

using System;

/// <summary>
/// Configuration options for the feature flags module.
/// </summary>
public sealed class FeatureFlagOptions
{
    /// <summary>
    /// Gets or sets the unique identifier for this replica in the CRDT cluster.
    /// </summary>
    public string ReplicaId { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// Gets or sets a value indicating whether active mode is enabled.
    /// When enabled, local state changes trigger an immediate network sync broadcast,
    /// bypassing the regular anti-entropy delay.
    /// </summary>
    public bool ActiveSyncEnabled { get; set; }

    /// <summary>
    /// Gets or sets the host address to listen for incoming P2P connections.
    /// </summary>
    public string ListenHost { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the port to listen for incoming P2P connections.
    /// </summary>
    public int ListenPort { get; set; } = 8080;

    /// <summary>
    /// Gets or sets the multicast group address for UDP peer discovery.
    /// </summary>
    public string MulticastAddress { get; set; } = "239.255.0.1";

    /// <summary>
    /// Gets or sets the multicast port for UDP peer discovery.
    /// </summary>
    public int MulticastPort { get; set; } = 8035;

    /// <summary>
    /// Gets or sets the interval between UDP discovery broadcasts.
    /// </summary>
    public TimeSpan DiscoveryInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the timeout for dropping inactive peers from discovery.
    /// </summary>
    public TimeSpan DiscoveryTimeout { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Gets or sets the interval for the gossip protocol anti-entropy runs.
    /// </summary>
    public TimeSpan GossipInterval { get; set; } = TimeSpan.FromMilliseconds(500);
}