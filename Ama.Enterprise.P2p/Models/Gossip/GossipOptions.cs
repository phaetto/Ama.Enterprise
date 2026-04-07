namespace Ama.Enterprise.P2p.Models.Gossip;
/// <summary>
/// Configuration options for tuning the behavior of the gossip protocol.
/// </summary>
public sealed class GossipOptions : IEquatable<GossipOptions>
{
    /// <summary>
    /// Gets or sets the interval at which the node will initiate a gossip round.
    /// </summary>
    public TimeSpan GossipInterval { get; set; } = TimeSpan.FromMilliseconds(500);

    /// <summary>
    /// Gets or sets the number of peers to select for each gossip round (the fanout).
    /// </summary>
    public int Fanout { get; set; } = 3;

    /// <summary>
    /// Gets or sets the default Time-To-Live for newly created gossip messages.
    /// </summary>
    public int DefaultTimeToLive { get; set; } = 10;

    /// <summary>
    /// Gets or sets the host address to bind to for incoming connections. 
    /// Defaults to "+" (all interfaces). Use "localhost" to avoid requiring Admin rights on Windows during local testing.
    /// </summary>
    public string ListenHost { get; set; } = "+";

    /// <summary>
    /// Gets or sets the network port to listen on for incoming connections.
    /// </summary>
    public int ListenPort { get; set; } = 8080;

    /// <inheritdoc />
    public bool Equals(GossipOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return GossipInterval.Equals(other.GossipInterval) && 
               Fanout == other.Fanout && 
               DefaultTimeToLive == other.DefaultTimeToLive && 
               ListenPort == other.ListenPort &&
               string.Equals(ListenHost, other.ListenHost, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(
            GossipInterval, 
            Fanout, 
            DefaultTimeToLive, 
            ListenPort, 
            StringComparer.OrdinalIgnoreCase.GetHashCode(ListenHost ?? string.Empty));
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as GossipOptions);
}