namespace Ama.Enterprise.P2p.AspNetCore.Models;

/// <summary>
/// Determines how the ASP.NET Core P2P transport manages its inbound network listening bindings natively.
/// </summary>
public enum AspNetCoreHostingMode
{
    /// <summary>
    /// Relies on the host application's standard HTTP pipeline explicitly routed via MapP2pMeshEndpoints().
    /// </summary>
    Integrated,

    /// <summary>
    /// Spins up an isolated lightweight Kestrel web server internally to strictly decouple P2P traffic dynamically.
    /// </summary>
    Standalone
}