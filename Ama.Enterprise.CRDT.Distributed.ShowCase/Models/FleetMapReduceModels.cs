namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;

/// <summary>
/// Query DTO for searching low-battery devices across sharded fleet documents.
/// </summary>
public readonly record struct FleetBatterySearchQuery : IEquatable<FleetBatterySearchQuery>
{
    public FleetBatterySearchQuery(int maxBatteryThreshold, bool onlineOnly)
    {
        MaxBatteryThreshold = maxBatteryThreshold;
        OnlineOnly = onlineOnly;
    }

    [JsonPropertyName("m")]
    public int MaxBatteryThreshold { get; init; }

    [JsonPropertyName("o")]
    public bool OnlineOnly { get; init; }

    /// <inheritdoc />
    public bool Equals(FleetBatterySearchQuery other)
        => MaxBatteryThreshold == other.MaxBatteryThreshold && OnlineOnly == other.OnlineOnly;

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(MaxBatteryThreshold, OnlineOnly);
}

/// <summary>
/// Represents a single matching device found during a cluster-wide low-battery search.
/// </summary>
public readonly record struct LowBatteryDeviceMatchDto : IEquatable<LowBatteryDeviceMatchDto>
{
    public LowBatteryDeviceMatchDto(string fleetDocumentId, string deviceId, int batteryLevel, bool isOnline)
    {
        FleetDocumentId = fleetDocumentId;
        DeviceId = deviceId;
        BatteryLevel = batteryLevel;
        IsOnline = isOnline;
    }

    [JsonPropertyName("f")]
    public string FleetDocumentId { get; init; }

    [JsonPropertyName("d")]
    public string DeviceId { get; init; }

    [JsonPropertyName("b")]
    public int BatteryLevel { get; init; }

    [JsonPropertyName("o")]
    public bool IsOnline { get; init; }

    /// <inheritdoc />
    public bool Equals(LowBatteryDeviceMatchDto other)
        => string.Equals(FleetDocumentId, other.FleetDocumentId, StringComparison.Ordinal) &&
           string.Equals(DeviceId, other.DeviceId, StringComparison.Ordinal) &&
           BatteryLevel == other.BatteryLevel &&
           IsOnline == other.IsOnline;

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(FleetDocumentId, DeviceId, BatteryLevel, IsOnline);
}

/// <summary>
/// Result DTO containing all matching low-battery devices across evaluated fleet shards.
/// </summary>
public readonly record struct FleetBatterySearchResultDto : IEquatable<FleetBatterySearchResultDto>
{
    public FleetBatterySearchResultDto(IList<LowBatteryDeviceMatchDto> matches)
    {
        Matches = matches;
    }

    [JsonPropertyName("m")]
    public IList<LowBatteryDeviceMatchDto> Matches { get; init; }

    /// <inheritdoc />
    public bool Equals(FleetBatterySearchResultDto other)
    {
        if (Matches is null && other.Matches is null)
        {
            return true;
        }

        if (Matches is null || other.Matches is null)
        {
            return false;
        }

        return Matches.SequenceEqual(other.Matches);
    }

    /// <inheritdoc />
    public override int GetHashCode()
        => Matches?.Count ?? 0;
}

/// <summary>
/// Input DTO for computing statistical battery health metrics across all fleet shards.
/// </summary>
public readonly record struct FleetBatteryStatsInput : IEquatable<FleetBatteryStatsInput>
{
    public FleetBatteryStatsInput(bool onlineOnly)
    {
        OnlineOnly = onlineOnly;
    }

    [JsonPropertyName("o")]
    public bool OnlineOnly { get; init; }

    /// <inheritdoc />
    public bool Equals(FleetBatteryStatsInput other)
        => OnlineOnly == other.OnlineOnly;

    /// <inheritdoc />
    public override int GetHashCode()
        => OnlineOnly.GetHashCode();
}

/// <summary>
/// Intermediate wire accumulator DTO for computing global battery statistics across fleet shards.
/// </summary>
public readonly record struct FleetBatteryStatsAccumulatorDto : IEquatable<FleetBatteryStatsAccumulatorDto>
{
    public FleetBatteryStatsAccumulatorDto(long totalDevices, long batterySum, long batterySumOfSquares, int minBattery, int maxBattery)
    {
        TotalDevices = totalDevices;
        BatterySum = batterySum;
        BatterySumOfSquares = batterySumOfSquares;
        MinBattery = minBattery;
        MaxBattery = maxBattery;
    }

    [JsonPropertyName("c")]
    public long TotalDevices { get; init; }

    [JsonPropertyName("s")]
    public long BatterySum { get; init; }

    [JsonPropertyName("sq")]
    public long BatterySumOfSquares { get; init; }

    [JsonPropertyName("min")]
    public int MinBattery { get; init; }

    [JsonPropertyName("max")]
    public int MaxBattery { get; init; }

    /// <inheritdoc />
    public bool Equals(FleetBatteryStatsAccumulatorDto other)
        => TotalDevices == other.TotalDevices &&
           BatterySum == other.BatterySum &&
           BatterySumOfSquares == other.BatterySumOfSquares &&
           MinBattery == other.MinBattery &&
           MaxBattery == other.MaxBattery;

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(TotalDevices, BatterySum, BatterySumOfSquares, MinBattery, MaxBattery);
}

/// <summary>
/// Final reduced battery health report computed on the coordinator node.
/// </summary>
public readonly record struct FleetBatteryHealthReportDto(
    long TotalDevices,
    double MeanBatteryLevel,
    double StandardDeviation,
    int MinBatteryLevel,
    int MaxBatteryLevel);

/// <summary>
/// Command DTO for purging offline devices below a battery threshold in-place across fleet shards.
/// </summary>
public readonly record struct PurgeOfflineDevicesCommand : IEquatable<PurgeOfflineDevicesCommand>
{
    public PurgeOfflineDevicesCommand(int maxBatteryThreshold)
    {
        MaxBatteryThreshold = maxBatteryThreshold;
    }

    [JsonPropertyName("m")]
    public int MaxBatteryThreshold { get; init; }

    /// <inheritdoc />
    public bool Equals(PurgeOfflineDevicesCommand other)
        => MaxBatteryThreshold == other.MaxBatteryThreshold;

    /// <inheritdoc />
    public override int GetHashCode()
        => MaxBatteryThreshold;
}

/// <summary>
/// Summary DTO returned after an in-place distributed purge of offline low-battery devices.
/// </summary>
public readonly record struct PurgeOfflineDevicesSummaryDto : IEquatable<PurgeOfflineDevicesSummaryDto>
{
    public PurgeOfflineDevicesSummaryDto(int purgedDeviceCount, int modifiedDocumentCount)
    {
        PurgedDeviceCount = purgedDeviceCount;
        ModifiedDocumentCount = modifiedDocumentCount;
    }

    [JsonPropertyName("p")]
    public int PurgedDeviceCount { get; init; }

    [JsonPropertyName("d")]
    public int ModifiedDocumentCount { get; init; }

    /// <inheritdoc />
    public bool Equals(PurgeOfflineDevicesSummaryDto other)
        => PurgedDeviceCount == other.PurgedDeviceCount &&
           ModifiedDocumentCount == other.ModifiedDocumentCount;

    /// <inheritdoc />
    public override int GetHashCode()
        => HashCode.Combine(PurgedDeviceCount, ModifiedDocumentCount);
}