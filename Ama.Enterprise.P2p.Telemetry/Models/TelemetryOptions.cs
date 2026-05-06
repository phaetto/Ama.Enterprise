namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;

/// <summary>
/// Configurations specifically setting explicitly tracked telemetry network bounds isolating active generic payloads seamlessly.
/// </summary>
public sealed record TelemetryOptions
{
    /// <summary>
    /// Explicit target mesh identifying isolated network topology channels allocating native telemetry boundaries strictly decoupled minimizing recursive feedback streams.
    /// </summary>
    public string TargetMeshId { get; set; } = "TelemetryMesh";

    /// <summary>
    /// Timer frequency defining periodic aggregated flushes broadcasting batched native configurations locally.
    /// </summary>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Toggle dynamically resolving explicit telemetry forwarding bounds tracking metrics efficiently natively.
    /// </summary>
    public bool IsEnabled { get; set; } = true;
}