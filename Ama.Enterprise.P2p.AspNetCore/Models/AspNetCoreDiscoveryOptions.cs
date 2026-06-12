namespace Ama.Enterprise.P2p.AspNetCore.Models;

using System;

/// <summary>
/// Configuration options for routing ASP.NET Core-based peer discovery probes globally.
/// </summary>
public sealed record AspNetCoreDiscoveryOptions : IEquatable<AspNetCoreDiscoveryOptions>
{
    /// <summary>
    /// Gets or sets the target HTTP URI to connect to for discovering peers.
    /// This is typically a load balancer routing to active nodes in the cluster.
    /// </summary>
    public string DiscoveryUrl { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the interval at which HTTP discovery requests are dispatched.
    /// </summary>
    public TimeSpan DiscoveryInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets the maximum duration to wait for an HTTP discovery request to respond.
    /// </summary>
    public TimeSpan DiscoveryTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Gets or sets the explicit hosting mode isolating Standalone WebHost deployments natively.
    /// </summary>
    public AspNetCoreHostingMode HostingMode { get; set; } = AspNetCoreHostingMode.Integrated;

    /// <summary>
    /// Gets or sets the host address bound when operating in Standalone hosting mode natively.
    /// </summary>
    public string StandaloneListenHost { get; set; } = "+";

    /// <summary>
    /// Gets or sets the explicit local port binding standard inbound bounds when operating in Standalone mode.
    /// </summary>
    public int StandaloneListenPort { get; set; } = 8081;

    /// <summary>
    /// Gets or sets the prefix route mapped distinctly capturing HTTP discovery POST bounds natively.
    /// </summary>
    public string PathPrefix { get; set; } = "/ama-enterprise/p2p-discovery";

    /// <summary>
    /// Gets or sets a value indicating whether the Standalone hosting mode should bind using HTTPS.
    /// </summary>
    public bool UseHttpsStandalone { get; set; } = false;

    /// <summary>
    /// Gets or sets the file path to the X.509 certificate used for Standalone HTTPS bindings.
    /// </summary>
    public string? CertificateFilePath { get; set; }

    /// <summary>
    /// Gets or sets the password for the certificate file if it is encrypted.
    /// </summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// Gets or sets the certificate thumbprint to load from the local Certificate Store for HTTPS bindings.
    /// </summary>
    public string? CertificateThumbprint { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether outbound HTTP requests should ignore SSL validation errors (e.g., self-signed certificates).
    /// </summary>
    public bool IgnoreOutboundSslErrors { get; set; } = false;
}