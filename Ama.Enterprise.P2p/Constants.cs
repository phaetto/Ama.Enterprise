namespace Ama.Enterprise.P2p;

/// <summary>
/// Global constants for the P2P networking module.
/// </summary>
public static class Constants
{
    /// <summary>
    /// The current version of the gossip protocol to ensure backwards compatibility and prevent cross-version pollution.
    /// Derived from the assembly version at runtime.
    /// </summary>
    public static readonly string ProtocolVersion = typeof(Constants).Assembly.GetName().Version?.ToString() ?? "0.0.0";
}