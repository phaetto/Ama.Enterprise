namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Distributed cluster query aggregating feature flags across sharded product and tenant documents.
/// </summary>
public sealed class FeatureFlagClusterQuery : ICrdtClusterQuery<FeatureFlagState, FeatureFlagQueryDto, FeatureFlagQueryResultDto>
{
    /// <inheritdoc />
    public ValueTask<FeatureFlagQueryResultDto> MapDocumentAsync(
        CrdtMapContext<FeatureFlagState> context,
        FeatureFlagQueryDto input,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(input);

        var result = new FeatureFlagQueryResultDto();
        var data = context.Data;
        if (data?.Flags == null || data.Flags.Count == 0)
        {
            return new ValueTask<FeatureFlagQueryResultDto>(result);
        }

        foreach (var kvp in data.Flags)
        {
            var flag = kvp.Value;
            if (!string.IsNullOrWhiteSpace(input.ProductId))
            {
                var flagProduct = flag.Metadata?.ProductId;
                if (!string.Equals(flagProduct, input.ProductId, StringComparison.Ordinal))
                {
                    continue;
                }
            }

            if (!string.IsNullOrWhiteSpace(input.TenantId))
            {
                var flagTenant = flag.Metadata?.TenantId;
                if (!string.Equals(flagTenant, input.TenantId, StringComparison.Ordinal))
                {
                    continue;
                }
            }

            var compositeKey = $"{flag.Metadata?.ProductId}:{flag.Metadata?.TenantId}:{kvp.Key}";
            result.Flags[compositeKey] = flag;
        }

        return new ValueTask<FeatureFlagQueryResultDto>(result);
    }

    /// <inheritdoc />
    public FeatureFlagQueryResultDto Combine(
        FeatureFlagQueryDto input,
        IReadOnlyList<FeatureFlagQueryResultDto> accumulators)
    {
        ArgumentNullException.ThrowIfNull(input);
        ArgumentNullException.ThrowIfNull(accumulators);

        var combined = new FeatureFlagQueryResultDto();
        for (int i = 0; i < accumulators.Count; i++)
        {
            var acc = accumulators[i];
            if (acc?.Flags == null)
            {
                continue;
            }

            foreach (var kvp in acc.Flags)
            {
                if (!combined.Flags.TryGetValue(kvp.Key, out var existing) ||
                    (kvp.Value.Audit != null && existing.Audit != null && kvp.Value.Audit.UpdatedAt >= existing.Audit.UpdatedAt))
                {
                    combined.Flags[kvp.Key] = kvp.Value;
                }
            }
        }

        return combined;
    }
}