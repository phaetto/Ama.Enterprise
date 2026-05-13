namespace Ama.Enterprise.P2p.Telemetry.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using Ama.Enterprise.P2p.Telemetry;

/// <summary>
/// Configurations specifically setting explicitly tracked telemetry network bounds isolating active generic payloads.
/// </summary>
public sealed record TelemetryOptions : IEquatable<TelemetryOptions>
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

    /// <summary>
    /// A set of meter names that are permitted to be collected and forwarded natively by the telemetry aggregator.
    /// Defaults to the core P2P protocol meter names.
    /// </summary>
    public ISet<string> IncludedMeterNames { get; set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        Constants.AmaEnterpriseBaseMeterPrefixName,
    };

    /// <inheritdoc />
    public bool Equals(TelemetryOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        if (!string.Equals(TargetMeshId, other.TargetMeshId, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (FlushInterval != other.FlushInterval)
        {
            return false;
        }

        if (IsEnabled != other.IsEnabled)
        {
            return false;
        }

        if (IncludedMeterNames is null && other.IncludedMeterNames is null)
        {
            return true;
        }

        if (IncludedMeterNames is null || other.IncludedMeterNames is null)
        {
            return false;
        }

        if (IncludedMeterNames.Count != other.IncludedMeterNames.Count)
        {
            return false;
        }

        return !IncludedMeterNames.Except(other.IncludedMeterNames, StringComparer.OrdinalIgnoreCase).Any();
    }

    /// <inheritdoc />
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(TargetMeshId, StringComparer.OrdinalIgnoreCase);
        hash.Add(FlushInterval);
        hash.Add(IsEnabled);
        
        if (IncludedMeterNames is not null)
        {
            foreach (var meter in IncludedMeterNames.OrderBy(m => m, StringComparer.OrdinalIgnoreCase))
            {
                hash.Add(meter, StringComparer.OrdinalIgnoreCase);
            }
        }
        
        return hash.ToHashCode();
    }
}