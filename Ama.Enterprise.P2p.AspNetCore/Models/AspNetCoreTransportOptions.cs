namespace Ama.Enterprise.P2p.AspNetCore.Models;

/// <summary>
/// Configuration bounds mapping ASP.NET Core shared networking topologies decoupled from native port bindings.
/// </summary>
public sealed record AspNetCoreTransportOptions
{
    /// <summary>
    /// Gets or sets a value indicating whether this underlying transport is dynamically enabled.
    /// </summary>
    public bool IsEnabled { get; set; }

    /// <summary>
    /// Gets or sets the hosting mode determining if the P2P transport relies on the host pipeline or an isolated server.
    /// </summary>
    public AspNetCoreHostingMode HostingMode { get; set; } = AspNetCoreHostingMode.Integrated;

    /// <summary>
    /// Gets or sets the HTTP URL route path prefix where the host application maps the P2P receiver endpoints.
    /// Defaults to "/ama-enterprise/p2p-mesh".
    /// </summary>
    public string PathPrefix { get; set; } = "/ama-enterprise/p2p-mesh";

    /// <summary>
    /// Gets or sets the publicly routable Host name or IP address this node advertises strictly to remote peers.
    /// </summary>
    public string AdvertisedHost { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the publicly routable Port this node advertises securely to external topologies.
    /// </summary>
    public int AdvertisedPort { get; set; } = 80;

    /// <summary>
    /// Gets or sets a value indicating whether to strictly broadcast HTTPS schemes mapping generic outbound clients.
    /// </summary>
    public bool UseHttps { get; set; } = false;

    /// <summary>
    /// Gets or sets the local host address to bind the internal web server exclusively when operating in Standalone mode. Defaults to "+".
    /// </summary>
    public string StandaloneListenHost { get; set; } = "+";

    /// <summary>
    /// Gets or sets the localized network port bound implicitly for inbound traffic when evaluating Standalone topologies natively.
    /// </summary>
    public int StandaloneListenPort { get; set; } = 8080;

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