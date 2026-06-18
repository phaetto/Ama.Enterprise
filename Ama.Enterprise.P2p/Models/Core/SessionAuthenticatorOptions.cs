namespace Ama.Enterprise.P2p.Models.Core;

using System;

/// <summary>
/// Configuration options mapping decoupled bounds tracking zero-trust session authentication limits natively.
/// </summary>
public sealed class SessionAuthenticatorOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether strict session tracking is mandated explicitly.
    /// </summary>
    public bool RequireSession { get; set; } = true;

    /// <summary>
    /// Gets or sets the explicit duration before an inactive session gets purged gracefully.
    /// </summary>
    public TimeSpan SessionEvictionTimeout { get; set; } = TimeSpan.FromHours(1);
}