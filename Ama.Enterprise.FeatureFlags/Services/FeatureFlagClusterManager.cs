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
/// Implementation of the cluster manager logic interacting tightly with the internal typed distributed pipeline.
/// </summary>
public sealed class FeatureFlagClusterManager : IFeatureFlagClusterManager
{
    private readonly IDistributedCrdtDocument<FeatureFlagState> documentManager;
    private readonly ICrdtPatcher patcher;
    private readonly object syncRoot = new();

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public FeatureFlagClusterManager(
        IDistributedCrdtDocument<FeatureFlagState> documentManager,
        ICrdtPatcher patcher)
    {
        this.documentManager = documentManager ?? throw new ArgumentNullException(nameof(documentManager));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));

        this.documentManager.StateChanged += (sender, args) => StateChanged?.Invoke(this, args);
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, FeatureFlag> GetFlags()
    {
        lock (syncRoot)
        {
            return new ReadOnlyDictionary<string, FeatureFlag>(documentManager.Document.Data.Flags);
        }
    }

    /// <inheritdoc />
    public async Task SetFlagAsync(string name, bool isEnabled, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var flag = new FeatureFlag(name, isEnabled);
        
        var operation = patcher.GenerateOperation(documentManager.Document, x => x.Flags, new MapSetIntent(name, flag));
        var patch = new CrdtPatch(new[] { operation });

        await documentManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemoveFlagAsync(string name, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var operation = patcher.GenerateOperation(documentManager.Document, x => x.Flags, new MapRemoveIntent(name));
        var patch = new CrdtPatch(new[] { operation });

        await documentManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }
}