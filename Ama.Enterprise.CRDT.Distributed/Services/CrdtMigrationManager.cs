namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Default implementation of <see cref="ICrdtMigrationManager"/> coordinating all registered <see cref="ICrdtMigrationBridge"/> instances.
/// </summary>
public sealed class CrdtMigrationManager : ICrdtMigrationManager
{
    private readonly IReadOnlyList<ICrdtMigrationBridge> bridges;

    public CrdtMigrationManager(IEnumerable<ICrdtMigrationBridge> bridges)
    {
        ArgumentNullException.ThrowIfNull(bridges);
        this.bridges = bridges.ToList();
    }

    /// <inheritdoc />
    public IReadOnlyList<CrdtMigrationStatusDto> GetAllStatuses()
    {
        if (bridges.Count == 0)
        {
            return Array.Empty<CrdtMigrationStatusDto>();
        }

        var list = new List<CrdtMigrationStatusDto>(bridges.Count);
        for (int i = 0; i < bridges.Count; i++)
        {
            list.Add(bridges[i].GetStatus());
        }

        return list;
    }

    /// <inheritdoc />
    public bool TryGetStatus(string migrationId, out CrdtMigrationStatusDto status)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(migrationId);

        for (int i = 0; i < bridges.Count; i++)
        {
            var bridge = bridges[i];
            if (string.Equals(bridge.Options.MigrationId, migrationId, StringComparison.Ordinal))
            {
                status = bridge.GetStatus();
                return true;
            }
        }

        status = default;
        return false;
    }

    /// <inheritdoc />
    public bool AreAllConverged()
    {
        if (bridges.Count == 0)
        {
            return true;
        }

        for (int i = 0; i < bridges.Count; i++)
        {
            if (!bridges[i].GetStatus().IsConverged)
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public async Task DrainAllAsync(CancellationToken cancellationToken = default)
    {
        for (int i = 0; i < bridges.Count; i++)
        {
            await bridges[i].DrainAndFlushAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}