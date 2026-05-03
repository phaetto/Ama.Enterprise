namespace Ama.Enterprise.P2p.Telemetry;

/// <summary>
/// Global constants utilized by the Telemetry networking pipeline evaluating strictly decoupled telemetry bounds.
/// </summary>
public static class Constants
{
    /// <summary>
    /// Baseline meter name allocated for the standard active gossip algorithms bounds.
    /// </summary>
    public const string CoreGossipMeterName = "Ama.Enterprise.P2p.Gossip";

    /// <summary>
    /// Baseline meter name targeting explicitly the active Push-Pull gossip parameters.
    /// </summary>
    public const string PushPullGossipMeterName = "Ama.Enterprise.P2p.PushPullGossip";
}