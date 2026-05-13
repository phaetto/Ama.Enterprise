namespace Ama.Enterprise.P2p.Models.Transports;

/// <summary>
/// Configuration options explicitly bound for configuring active UDP datagram connectivity.
/// </summary>
public sealed class UdpTransportOptions : IEquatable<UdpTransportOptions>
{
    /// <summary>
    /// Gets or sets a value indicating whether the UDP transport is enabled globally for this mesh.
    /// </summary>
    public bool IsEnabled { get; set; }
    
    /// <summary>
    /// Gets or sets the target listening hostname or IP address mapping. 
    /// Explicitly supports '+' to bind across all available local interfaces natively.
    /// </summary>
    public string ListenHost { get; set; } = "+";
    
    /// <summary>
    /// Gets or sets the local listening port bounding incoming decentralized P2P payload allocations.
    /// </summary>
    public int ListenPort { get; set; } = 0;

    /// <inheritdoc />
    public bool Equals(UdpTransportOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return IsEnabled == other.IsEnabled &&
               ListenHost == other.ListenHost &&
               ListenPort == other.ListenPort;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as UdpTransportOptions);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(IsEnabled, ListenHost, ListenPort);
}