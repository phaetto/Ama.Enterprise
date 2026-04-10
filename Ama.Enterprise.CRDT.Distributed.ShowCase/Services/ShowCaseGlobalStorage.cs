namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Services;

using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.CRDT.Distributed.Services;
using Microsoft.Extensions.Logging;

/// <summary>
/// Showcase implementation of the global DVV storage mapping to local JSON files on disk.
/// This allows cluster nodes to completely remember their synchronization boundaries between process restarts.
/// </summary>
public sealed class ShowCaseGlobalStorage : IDistributedCrdtGlobalStorage
{
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<ShowCaseGlobalStorage> logger;

    public ShowCaseGlobalStorage(ICrdtSerializer serializer, ILogger<ShowCaseGlobalStorage> logger)
    {
        this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task<DottedVersionVector?> LoadGlobalVersionVectorAsync(string replicaId, CancellationToken cancellationToken = default)
    {
        var filePath = GetFilePath(replicaId);
        if (!File.Exists(filePath))
        {
            return null;
        }

        try
        {
            var bytes = await File.ReadAllBytesAsync(filePath, cancellationToken).ConfigureAwait(false);
            return serializer.DeserializeFromBytes<DottedVersionVector>(bytes);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load global DVV from {FilePath}", filePath);
            return null;
        }
    }

    /// <inheritdoc />
    public async Task SaveGlobalVersionVectorAsync(string replicaId, DottedVersionVector globalVersionVector, CancellationToken cancellationToken = default)
    {
        var filePath = GetFilePath(replicaId);
        try
        {
            var bytes = serializer.SerializeToBytes(globalVersionVector);
            await File.WriteAllBytesAsync(filePath, bytes, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to save global DVV to {FilePath}", filePath);
        }
    }

    private static string GetFilePath(string replicaId) => $"{replicaId}_global_dvv.json";
}