namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Showcase implementation of a document storage mechanism mapping the entire CRDT state tree (data + metadata) to local JSON files.
/// </summary>
public sealed class ShowCaseDocumentStorage<TState> : IDistributedCrdtStorage<TState> where TState : class, new()
{
    private readonly string replicaId;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<ShowCaseDocumentStorage<TState>> logger;

    public ShowCaseDocumentStorage(
        IOptions<DistributedCrdtOptions> options,
        ICrdtSerializer serializer,
        ILogger<ShowCaseDocumentStorage<TState>> logger)
    {
        if (options == null) throw new ArgumentNullException(nameof(options));
        this.replicaId = options.Value.ReplicaId;
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<CrdtDocument<TState>?> LoadAsync(string documentId, CancellationToken cancellationToken = default)
    {
        var filePath = GetFilePath(documentId);
        if (!File.Exists(filePath))
        {
            return null;
        }

        try
        {
            var bytes = await File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
            return serializer.DeserializeFromBytes<CrdtDocument<TState>>(bytes);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load document from {FilePath}", filePath);
            return null;
        }
    }

    /// <inheritdoc />
    public async Task SaveAsync(string documentId, CrdtDocument<TState> document, CancellationToken cancellationToken = default)
    {
        var filePath = GetFilePath(documentId);
        try
        {
            var bytes = serializer.SerializeToBytes(document);
            await File.WriteAllBytesAsync(filePath, bytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to save document to {FilePath}", filePath);
        }
    }

    private string GetFilePath(string documentId) => $"{replicaId}_{documentId}_state.json";
}