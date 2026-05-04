namespace Ama.Enterprise.P2p.AspNetCore.Models;

using System;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Endpoint model distinguishing standard ASP.NET Core routed nodes mapping remote destination bounds.
/// </summary>
public sealed record AspNetCorePeerEndpoint : PeerEndpoint, IEquatable<AspNetCorePeerEndpoint>
{
    /// <summary>
    /// Gets the routable remote host mapping.
    /// </summary>
    public string Host { get; init; }

    /// <summary>
    /// Gets the explicit target port boundary.
    /// </summary>
    public int Port { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="AspNetCorePeerEndpoint"/> class.
    /// </summary>
    /// <param name="host">The routable host identity.</param>
    /// <param name="port">The destination port.</param>
    public AspNetCorePeerEndpoint(string host, int port)
    {
        if (string.IsNullOrWhiteSpace(host))
        {
            throw new ArgumentException("Host cannot be null or empty.", nameof(host));
        }

        Host = host;
        Port = port;
    }

    /// <inheritdoc />
    public bool Equals(AspNetCorePeerEndpoint? other)
    {
        if (other is null) return false;
        return string.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase) && Port == other.Port;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        return HashCode.Combine(StringComparer.OrdinalIgnoreCase.GetHashCode(Host), Port);
    }
}