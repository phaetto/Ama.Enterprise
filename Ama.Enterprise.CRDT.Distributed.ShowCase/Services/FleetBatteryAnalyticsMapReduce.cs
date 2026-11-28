namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services.MapReduce;
using Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Multi-stage Map/Reduce handler that computes global mean, standard deviation, min, and max battery levels across sharded Fleet documents.
/// </summary>
public sealed class FleetBatteryAnalyticsMapReduce
    : ICrdtMapReduce<FleetState, FleetBatteryStatsInput, FleetBatteryStatsAccumulatorDto, FleetBatteryHealthReportDto>
{
    /// <inheritdoc />
    public ValueTask<FleetBatteryStatsAccumulatorDto> MapDocumentAsync(
        CrdtMapContext<FleetState> context,
        FleetBatteryStatsInput input,
        CancellationToken cancellationToken = default)
    {
        var devices = context.Data?.Devices;
        if (devices is null || devices.Count == 0)
        {
            return new ValueTask<FleetBatteryStatsAccumulatorDto>(
                new FleetBatteryStatsAccumulatorDto(0, 0, 0, int.MaxValue, int.MinValue));
        }

        long count = 0;
        long sum = 0;
        long sumSq = 0;
        var min = int.MaxValue;
        var max = int.MinValue;

        foreach (var kvp in devices)
        {
            var device = kvp.Value;
            if (input.OnlineOnly && !device.IsOnline)
            {
                continue;
            }

            var level = device.BatteryLevel;
            count++;
            sum += level;
            sumSq += (long)level * level;
            if (level < min)
            {
                min = level;
            }

            if (level > max)
            {
                max = level;
            }
        }

        return new ValueTask<FleetBatteryStatsAccumulatorDto>(
            new FleetBatteryStatsAccumulatorDto(count, sum, sumSq, min, max));
    }

    /// <inheritdoc />
    public FleetBatteryStatsAccumulatorDto Combine(
        FleetBatteryStatsInput input,
        IReadOnlyList<FleetBatteryStatsAccumulatorDto> accumulators)
    {
        ArgumentNullException.ThrowIfNull(accumulators);

        long count = 0;
        long sum = 0;
        long sumSq = 0;
        var min = int.MaxValue;
        var max = int.MinValue;

        for (var i = 0; i < accumulators.Count; i++)
        {
            var acc = accumulators[i];
            if (acc.TotalDevices == 0)
            {
                continue;
            }

            count += acc.TotalDevices;
            sum += acc.BatterySum;
            sumSq += acc.BatterySumOfSquares;
            if (acc.MinBattery < min)
            {
                min = acc.MinBattery;
            }

            if (acc.MaxBattery > max)
            {
                max = acc.MaxBattery;
            }
        }

        return new FleetBatteryStatsAccumulatorDto(count, sum, sumSq, min, max);
    }

    /// <inheritdoc />
    public FleetBatteryHealthReportDto Reduce(
        FleetBatteryStatsInput input,
        FleetBatteryStatsAccumulatorDto finalAccumulator)
    {
        if (finalAccumulator.TotalDevices == 0)
        {
            return new FleetBatteryHealthReportDto(0, 0d, 0d, 0, 0);
        }

        var mean = (double)finalAccumulator.BatterySum / finalAccumulator.TotalDevices;
        var variance = ((double)finalAccumulator.BatterySumOfSquares / finalAccumulator.TotalDevices) - (mean * mean);
        var stdDev = variance > 0 ? Math.Sqrt(variance) : 0d;

        return new FleetBatteryHealthReportDto(
            finalAccumulator.TotalDevices,
            mean,
            stdDev,
            finalAccumulator.MinBattery,
            finalAccumulator.MaxBattery);
    }
}