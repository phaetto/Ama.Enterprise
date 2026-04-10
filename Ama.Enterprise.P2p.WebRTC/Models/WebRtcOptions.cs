namespace Ama.Enterprise.P2p.WebRTC.Models;

using System;
using System.Collections.Generic;
using System.Linq;

/// <summary>
/// Configuration options for WebRTC transport mapping ICE servers and timeouts.
/// </summary>
public sealed class WebRtcOptions : IEquatable<WebRtcOptions>
{
    /// <summary>
    /// Gets or sets the list of STUN/TURN server URLs.
    /// </summary>
    public IList<string> IceServers { get; set; } = new List<string> { "stun:stun.l.google.com:19302" };

    /// <summary>
    /// Gets or sets the timeout for ICE gathering completion when generating invitations.
    /// </summary>
    public TimeSpan IceGatheringTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <inheritdoc />
    public bool Equals(WebRtcOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return IceServers.SequenceEqual(other.IceServers) &&
               IceGatheringTimeout.Equals(other.IceGatheringTimeout);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as WebRtcOptions);

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var server in IceServers)
        {
            hash.Add(server);
        }
        hash.Add(IceGatheringTimeout);
        return hash.ToHashCode();
    }
}