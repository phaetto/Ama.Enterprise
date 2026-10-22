namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models.Intents;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Implementation of the feature flag cluster manager focused on mutations and retrievals routing via the orchestrator gateway.
/// </summary>
public sealed class FeatureFlagCrdtManager : IFeatureFlagCrdtManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;

    public FeatureFlagCrdtManager(ICrdtDocumentOrchestrator orchestrator)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetFlagsAsync(CancellationToken cancellationToken = default)
    {
        if (!await orchestrator.DocumentExistsAsync<FeatureFlagState>(Constants.GlobalDocumentId, cancellationToken).ConfigureAwait(false))
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(new Dictionary<string, FeatureFlag>());
        }

        var crdtDoc = await orchestrator.GetDocumentStateAsync<FeatureFlagState>(Constants.GlobalDocumentId, cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data != null && crdtDoc.Data.Flags != null)
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(crdtDoc.Data.Flags);
        }

        return new ReadOnlyDictionary<string, FeatureFlag>(new Dictionary<string, FeatureFlag>());
    }

    /// <inheritdoc />
    public async Task SetFlagAsync(
        string name, 
        bool isEnabled, 
        string? modifiedBy = null,
        FeatureFlagMetadata? metadata = null,
        FeatureFlagOwnership? ownership = null,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var now = DateTimeOffset.UtcNow;

        var hasExisting = false;
        FeatureFlag existingFlag = default!;

        if (await orchestrator.DocumentExistsAsync<FeatureFlagState>(Constants.GlobalDocumentId, cancellationToken).ConfigureAwait(false))
        {
            var crdtDoc = await orchestrator.GetDocumentStateAsync<FeatureFlagState>(Constants.GlobalDocumentId, cancellationToken).ConfigureAwait(false);
            if (crdtDoc.Data?.Flags != null)
            {
                hasExisting = crdtDoc.Data.Flags.TryGetValue(name, out existingFlag);
            }
        }
        else
        {
            await orchestrator.CreateDocumentAsync(Constants.GlobalDocumentId, Constants.FeatureFlagDocumentType, deliveryMode, cancellationToken).ConfigureAwait(false);
        }

        var createdAt = hasExisting ? existingFlag.Audit.CreatedAt : now;

        var audit = new FeatureFlagAudit
        {
            LastModifiedBy = modifiedBy,
            CreatedAt = createdAt,
            UpdatedAt = now
        };
        
        var actualMetadata = metadata ?? (hasExisting ? existingFlag.Metadata : default);
        var actualOwnership = ownership ?? (hasExisting ? existingFlag.Ownership : default);

        var flag = new FeatureFlag
        {
            Name = name,
            IsEnabled = isEnabled,
            Metadata = actualMetadata!,
            Audit = audit,
            Ownership = actualOwnership!
        };
        
        var intent = new MapSetIntent(name, flag);
        await orchestrator.ApplyIntentAsync<FeatureFlagState>(Constants.GlobalDocumentId, intent, "Flags", deliveryMode, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default)
    {
        return RemoveFlagAsync(name, IntentDeliveryMode.Acknowledged, cancellationToken);
    }

    /// <inheritdoc />
    public async Task RemoveFlagAsync(string name, IntentDeliveryMode deliveryMode, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var intent = new MapRemoveIntent(name);
        await orchestrator.ApplyIntentAsync<FeatureFlagState>(Constants.GlobalDocumentId, intent, "Flags", deliveryMode, cancellationToken).ConfigureAwait(false);
    }
}