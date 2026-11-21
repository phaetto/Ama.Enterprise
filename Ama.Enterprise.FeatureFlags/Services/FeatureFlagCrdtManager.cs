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
/// Each product and tenant combination is stored in its own document for shard distribution.
/// </summary>
public sealed class FeatureFlagCrdtManager : IFeatureFlagCrdtManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;

    public FeatureFlagCrdtManager(ICrdtDocumentOrchestrator orchestrator)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetAllFlagsAsync(CancellationToken cancellationToken = default)
    {
        var documentIds = await GetRegisteredFeatureFlagDocumentIdsAsync(cancellationToken).ConfigureAwait(false);
        if (documentIds.Count == 0)
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(new Dictionary<string, FeatureFlag>());
        }

        var queryResult = await orchestrator.QueryClusterAsync<FeatureFlagState, FeatureFlagQueryDto, FeatureFlagQueryResultDto>(
            new FeatureFlagQueryDto(),
            cancellationToken).ConfigureAwait(false);

        return new ReadOnlyDictionary<string, FeatureFlag>(queryResult.Flags ?? new Dictionary<string, FeatureFlag>());
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetFlagsAsync(
        string productId,
        string tenantId,
        CancellationToken cancellationToken = default)
    {
        var documentId = Constants.GetDocumentId(productId, tenantId);
        return await GetFlagsFromDocumentAsync(documentId, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task SetFlagAsync(
        string productId,
        string tenantId,
        string name, 
        bool isEnabled, 
        string? modifiedBy = null,
        FeatureFlagOwnership? ownership = null,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default)
    {
        var targetDocumentId = Constants.GetDocumentId(productId, tenantId);
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var now = DateTimeOffset.UtcNow;
        var hasExisting = false;
        FeatureFlag existingFlag = default!;

        if (await orchestrator.DocumentExistsAsync<FeatureFlagState>(targetDocumentId, cancellationToken).ConfigureAwait(false))
        {
            var crdtDoc = await orchestrator.GetDocumentStateAsync<FeatureFlagState>(targetDocumentId, cancellationToken).ConfigureAwait(false);
            if (crdtDoc.Data?.Flags != null)
            {
                hasExisting = crdtDoc.Data.Flags.TryGetValue(name, out existingFlag!);
            }
        }
        else
        {
            await orchestrator.CreateDocumentAsync(targetDocumentId, Constants.FeatureFlagDocumentType, deliveryMode, cancellationToken).ConfigureAwait(false);
        }

        var createdAt = hasExisting && existingFlag.Audit != null ? existingFlag.Audit.CreatedAt : now;

        var audit = new FeatureFlagAudit
        {
            LastModifiedBy = modifiedBy,
            CreatedAt = createdAt,
            UpdatedAt = now
        };
        
        var metadata = new FeatureFlagMetadata
        {
            ProductId = productId.Trim(),
            TenantId = tenantId.Trim()
        };

        var actualOwnership = ownership ?? (hasExisting ? existingFlag.Ownership : default);

        var flag = new FeatureFlag
        {
            Name = name,
            IsEnabled = isEnabled,
            Metadata = metadata,
            Audit = audit,
            Ownership = actualOwnership!
        };
        
        var intent = new MapSetIntent(name, flag);
        await orchestrator.ApplyIntentAsync<FeatureFlagState>(targetDocumentId, intent, "Flags", deliveryMode, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveFlagAsync(
        string productId,
        string tenantId,
        string name,
        IntentDeliveryMode deliveryMode = IntentDeliveryMode.Acknowledged,
        CancellationToken cancellationToken = default)
    {
        var targetDocumentId = Constants.GetDocumentId(productId, tenantId);
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var intent = new MapRemoveIntent(name);
        await orchestrator.ApplyIntentAsync<FeatureFlagState>(targetDocumentId, intent, "Flags", deliveryMode, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<IReadOnlyDictionary<string, FeatureFlag>> GetFlagsFromDocumentAsync(
        string documentId,
        CancellationToken cancellationToken)
    {
        if (!await orchestrator.DocumentExistsAsync<FeatureFlagState>(documentId, cancellationToken).ConfigureAwait(false))
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(new Dictionary<string, FeatureFlag>());
        }

        var crdtDoc = await orchestrator.GetDocumentStateAsync<FeatureFlagState>(documentId, cancellationToken).ConfigureAwait(false);
        if (crdtDoc.Data?.Flags != null)
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(crdtDoc.Data.Flags);
        }

        return new ReadOnlyDictionary<string, FeatureFlag>(new Dictionary<string, FeatureFlag>());
    }

    private async ValueTask<IReadOnlyList<string>> GetRegisteredFeatureFlagDocumentIdsAsync(CancellationToken cancellationToken)
    {
        if (orchestrator.Registry is null)
        {
            return Array.Empty<string>();
        }

        var registryDocument = await orchestrator.Registry.GetDocumentAsync(cancellationToken).ConfigureAwait(false);
        var registryData = registryDocument.Data;
        if (registryData?.Documents == null || registryData.Documents.Count == 0)
        {
            return Array.Empty<string>();
        }

        var documentIds = new List<string>();
        foreach (var entry in registryData.Documents.Values)
        {
            if (!entry.IsDeleted &&
                string.Equals(entry.TypeAlias, Constants.FeatureFlagDocumentType, StringComparison.Ordinal))
            {
                documentIds.Add(entry.DocumentId);
            }
        }

        return documentIds;
    }
}