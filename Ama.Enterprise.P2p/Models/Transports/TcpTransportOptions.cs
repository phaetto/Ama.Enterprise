namespace Ama.Enterprise.P2p.Models.Transports;

/// <summary>
/// Configuration options explicitly bound for configuring active TCP transport connectivity.
/// </summary>
public sealed class TcpTransportOptions : IEquatable<TcpTransportOptions>
{
    /// <summary>
    /// Gets or sets a value indicating whether the TCP transport is enabled globally for this mesh.
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
    
    /// <summary>
    /// Maximum structural length constraints preventing unbounded malicious payload streams dynamically. Default is 10MB.
    /// </summary>
    public int MaxMessageSize { get; set; } = 10 * 1024 * 1024;

    /// <inheritdoc />
    public bool Equals(TcpTransportOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return IsEnabled == other.IsEnabled &&
               ListenHost == other.ListenHost &&
               ListenPort == other.ListenPort &&
               MaxMessageSize == other.MaxMessageSize;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as TcpTransportOptions);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(IsEnabled, ListenHost, ListenPort, MaxMessageSize);
}