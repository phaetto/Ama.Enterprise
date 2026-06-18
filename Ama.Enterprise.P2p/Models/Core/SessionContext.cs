namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Collections.Generic;

/// <summary>
/// Data structure representing an authenticated multi-mesh session context extracted from handshake tokens.
/// </summary>
public readonly record struct SessionContext : IEquatable<SessionContext>
{
    /// <summary>
    /// Gets the unique identifier for the established session.
    /// </summary>
    public string SessionId { get; init; }

    /// <summary>
    /// Gets the generic collection of authorization claims bridging strict localized partition rules.
    /// </summary>
    public IReadOnlyDictionary<string, string> Claims { get; init; }

    /// <summary>
    /// Gets the explicit timestamp when the session is considered expired natively.
    /// </summary>
    public DateTimeOffset ExpiresAt { get; init; }

    /// <summary>
    /// Initializes a new instance of the <see cref="SessionContext"/> struct.
    /// </summary>
    /// <param name="sessionId">The unique session identifier.</param>
    /// <param name="claims">The dictionary of claims.</param>
    /// <param name="expiresAt">The expiration timestamp.</param>
    public SessionContext(string sessionId, IReadOnlyDictionary<string, string> claims, DateTimeOffset expiresAt)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        ArgumentNullException.ThrowIfNull(claims);

        SessionId = sessionId;
        Claims = claims;
        ExpiresAt = expiresAt;
    }

    /// <inheritdoc />
    public bool Equals(SessionContext other)
    {
        if (!string.Equals(SessionId, other.SessionId, StringComparison.Ordinal))
        {
            return false;
        }

        if (ExpiresAt != other.ExpiresAt)
        {
            return false;
        }

        if (Claims.Count != other.Claims.Count)
        {
            return false;
        }

        foreach (var kvp in Claims)
        {
            if (!other.Claims.TryGetValue(kvp.Key, out var otherValue) || !string.Equals(kvp.Value, otherValue, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(SessionId);
        hash.Add(ExpiresAt);
        
        foreach (var kvp in Claims)
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value);
        }

        return hash.ToHashCode();
    }
}