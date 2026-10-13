namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Models.Intents;
using Ama.CRDT.Services;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.FeatureFlags.Models;

/// <summary>
/// Implementation of the feature flag cluster manager explicitly focused on generic structural mutations and retrievals.
/// </summary>
public sealed class FeatureFlagClusterManager : IFeatureFlagClusterManager
{
    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly IAsyncCrdtPatcher patcher;

    public FeatureFlagClusterManager(
        ICrdtDocumentOrchestrator orchestrator,
        IAsyncCrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, FeatureFlag> GetFlags()
    {
        var docManager = orchestrator.GetDocument<FeatureFlagState>(Constants.GlobalDocumentId);
        
        if (docManager != null)
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(docManager.Document.Data.Flags);
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
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var docManager = GetRequiredDocument();
        var now = DateTimeOffset.UtcNow;

        // Preserve existing metadata, ownership, and creation date if they are not explicitly updated during an upsert
        var hasExisting = docManager.Document.Data.Flags.TryGetValue(name, out var existingFlag);
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
        
        var operation = await patcher.GenerateOperationAsync(docManager.Document, x => x.Flags, new MapSetIntent(name, flag), cancellationToken).ConfigureAwait(false);
        var patch = new CrdtPatch(new[] { operation });

        await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var docManager = GetRequiredDocument();
        var operation = await patcher.GenerateOperationAsync(docManager.Document, x => x.Flags, new MapRemoveIntent(name), cancellationToken).ConfigureAwait(false);
        var patch = new CrdtPatch(new[] { operation });

        await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }

    private IDistributedCrdtDocument<FeatureFlagState> GetRequiredDocument()
    {
        var doc = orchestrator.GetDocument<FeatureFlagState>(Constants.GlobalDocumentId);
        if (doc == null)
        {
            throw new InvalidOperationException("The global feature flag document has not been initialized yet by the application bootstrapper.");
        }
        return doc;
    }
}