namespace Ama.Enterprise.P2p.WebRTC.TableStorage.Models;

using System;

/// <summary>
/// Configuration options for the out-of-band WebRTC signaling service utilizing Azure Table Storage seamlessly.
/// </summary>
public sealed class TableStorageSignalingOptions : IEquatable<TableStorageSignalingOptions>
{
    /// <summary>
    /// Gets or sets the Azure Table Storage connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the name of the table to use for signaling records securely.
    /// </summary>
    public string TableName { get; set; } = "WebRtcSignaling";

    /// <summary>
    /// Gets or sets the interval at which the background service polls the table for new or answered invitations.
    /// </summary>
    public TimeSpan PollingInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Gets or sets the duration after which an unanswered SDP offer is considered expired and safely removed.
    /// </summary>
    public TimeSpan OfferExpiration { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>
    /// Gets or sets a value indicating whether the node should explicitly generate outgoing SDP offers dynamically.
    /// </summary>
    public bool EnableOfferGeneration { get; set; } = true;

    /// <summary>
    /// Gets or sets a value indicating whether the node should continuously poll and accept remote peer SDP offers securely.
    /// </summary>
    public bool EnableOfferAcceptance { get; set; } = true;

    /// <inheritdoc />
    public bool Equals(TableStorageSignalingOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        return string.Equals(ConnectionString, other.ConnectionString, StringComparison.Ordinal) &&
               string.Equals(TableName, other.TableName, StringComparison.Ordinal) &&
               PollingInterval.Equals(other.PollingInterval) &&
               OfferExpiration.Equals(other.OfferExpiration) &&
               EnableOfferGeneration == other.EnableOfferGeneration &&
               EnableOfferAcceptance == other.EnableOfferAcceptance;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as TableStorageSignalingOptions);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ConnectionString, TableName, PollingInterval, OfferExpiration, EnableOfferGeneration, EnableOfferAcceptance);
}