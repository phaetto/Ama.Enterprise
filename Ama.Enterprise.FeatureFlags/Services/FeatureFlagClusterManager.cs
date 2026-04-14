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
public sealed class FeatureFlagClusterManager : IFeatureFlagClusterManager, IDisposable
{
    private const string GlobalDocumentId = "feature-flags-singleton";

    private readonly ICrdtDocumentOrchestrator orchestrator;
    private readonly ICrdtPatcher patcher;
    private readonly object syncRoot = new();

    private IDistributedCrdtDocument<FeatureFlagState>? globalDocument;

    /// <inheritdoc />
    public event EventHandler? StateChanged;

    public FeatureFlagClusterManager(
        ICrdtDocumentOrchestrator orchestrator,
        ICrdtPatcher patcher)
    {
        this.orchestrator = orchestrator ?? throw new ArgumentNullException(nameof(orchestrator));
        this.patcher = patcher ?? throw new ArgumentNullException(nameof(patcher));

        this.orchestrator.DocumentsChanged += OnOrchestratorDocumentsChanged;
        
        TryAttachDocument();
    }

    private void OnOrchestratorDocumentsChanged(object? sender, EventArgs e)
    {
        if (TryAttachDocument())
        {
            StateChanged?.Invoke(this, EventArgs.Empty);
        }
    }

    private bool TryAttachDocument()
    {
        lock (syncRoot)
        {
            if (globalDocument != null)
            {
                return false;
            }

            globalDocument = orchestrator.GetDocument<FeatureFlagState>(GlobalDocumentId);

            if (globalDocument != null)
            {
                globalDocument.StateChanged += (sender, args) => StateChanged?.Invoke(this, args);
                return true;
            }
        }

        return false;
    }

    private IDistributedCrdtDocument<FeatureFlagState> GetRequiredDocument()
    {
        TryAttachDocument();
        
        lock (syncRoot)
        {
            if (globalDocument == null)
            {
                throw new InvalidOperationException("The global feature flag document has not been initialized yet by the application bootstrapper.");
            }
            return globalDocument;
        }
    }

    /// <inheritdoc />
    public IReadOnlyDictionary<string, FeatureFlag> GetFlags()
    {
        TryAttachDocument();

        lock (syncRoot)
        {
            if (globalDocument != null)
            {
                return new ReadOnlyDictionary<string, FeatureFlag>(globalDocument.Document.Data.Flags);
            }
        }

        return new ReadOnlyDictionary<string, FeatureFlag>(new Dictionary<string, FeatureFlag>());
    }

    /// <inheritdoc />
    public async Task SetFlagAsync(string name, bool isEnabled, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("Flag name cannot be null or empty.", nameof(name));
        }

        var docManager = GetRequiredDocument();
        var flag = new FeatureFlag(name, isEnabled);
        
        var operation = patcher.GenerateOperation(docManager.Document, x => x.Flags, new MapSetIntent(name, flag));
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
        var operation = patcher.GenerateOperation(docManager.Document, x => x.Flags, new MapRemoveIntent(name));
        var patch = new CrdtPatch(new[] { operation });

        await docManager.ApplyPatchAsync(patch, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose()
    {
        orchestrator.DocumentsChanged -= OnOrchestratorDocumentsChanged;
    }
}