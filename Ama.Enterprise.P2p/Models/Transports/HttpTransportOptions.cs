namespace Ama.Enterprise.P2p.Models.Transports;

using System;

/// <summary>
/// Configuration options for the generic HTTP transport layer, decoupled from protocol-specific behavior.
/// </summary>
public sealed class HttpTransportOptions : IEquatable<HttpTransportOptions>
{
    /// <summary>
    /// Gets or sets a value indicating whether the HTTP transport is explicitly enabled for a specific mesh.
    /// </summary>
    public bool IsEnabled { get; set; } = false;

    /// <summary>
    /// Gets or sets the host address to bind to for incoming connections. 
    /// Defaults to "+" (all interfaces). Use "localhost" to avoid requiring Admin rights on Windows during local testing.
    /// </summary>
    public string ListenHost { get; set; } = "+";

    /// <summary>
    /// Gets or sets the network port to listen on for incoming connections.
    /// </summary>
    public int ListenPort { get; set; } = 8080;

    /// <summary>
    /// Gets or sets the HTTP path prefix for receiving messages.
    /// </summary>
    public string PathPrefix { get; set; } = "/p2p/messages/";

    /// <inheritdoc />
    public bool Equals(HttpTransportOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return IsEnabled == other.IsEnabled &&
               ListenPort == other.ListenPort &&
               string.Equals(ListenHost, other.ListenHost, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(PathPrefix, other.PathPrefix, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(
            IsEnabled,
            ListenPort,
            StringComparer.OrdinalIgnoreCase.GetHashCode(ListenHost ?? string.Empty),
            StringComparer.OrdinalIgnoreCase.GetHashCode(PathPrefix ?? string.Empty));
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as HttpTransportOptions);
}