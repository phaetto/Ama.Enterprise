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

    /// <summary>
    /// The maximum allowable size for a gossip message payload in bytes (1MB).
    /// Prevents network flooding and memory exhaustion attacks.
    /// </summary>
    public const int MaximumPayloadSizeBytes = 1048576;

    /// <summary>
    /// The default network port used if no configuration is provided.
    /// </summary>
    public const int DefaultGossipPort = 8080;
}