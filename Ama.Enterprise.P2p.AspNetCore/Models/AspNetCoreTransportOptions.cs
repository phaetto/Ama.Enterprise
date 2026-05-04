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
    /// Gets or sets the HTTP URL route path prefix where the host application maps the P2P receiver endpoints.
    /// Defaults to "/p2p-mesh".
    /// </summary>
    public string PathPrefix { get; set; } = "/p2p-mesh";

    /// <summary>
    /// Gets or sets the publicly routable Host name or IP address this node advertises to peers.
    /// </summary>
    public string AdvertisedHost { get; set; } = "localhost";

    /// <summary>
    /// Gets or sets the publicly routable Port this node advertises to peers.
    /// </summary>
    public int AdvertisedPort { get; set; } = 80;

    /// <summary>
    /// Gets or sets a value indicating whether to strictly broadcast HTTPS schemes mapping generic outbound clients.
    /// </summary>
    public bool UseHttps { get; set; } = false;
}