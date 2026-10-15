namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Specifies the execution and delivery model when forwarding operation intents to distributed CRDT shards.
/// </summary>
public enum IntentDeliveryMode
{
    /// <summary>
    /// Waits for the authoritative shard to apply and journal the intent (with built-in retries and replica failover).
    /// Default mode. Best for Minimal APIs and sequential workflows.
    /// </summary>
    Acknowledged = 0,

    /// <summary>
    /// Dispatches the intent to the local queue or network without waiting for remote RPC confirmation.
    /// Best for high-throughput background ingestion, telemetry, or benchmark hammer modes.
    /// </summary>
    FireAndForget = 1
}