namespace Ama.Enterprise.P2p.TableStorage.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Options;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.TableStorage.Models;
using Ama.Enterprise.P2p.Models.Transports;

/// <summary>
/// Implements an AOT-friendly, durable peer registry backed by Azure Table Storage.
/// Ideal for serverless/ephemeral compute nodes that cannot maintain an in-memory topology.
/// </summary>
public sealed class TableStoragePeerRegistry : IPeerRegistry
{
    private readonly TableClient tableClient;
    private readonly string partitionKey;
    private int isTableEnsured;

    /// <summary>
    /// Initializes a new instance of the <see cref="TableStoragePeerRegistry"/> class.
    /// </summary>
    /// <param name="options">The configuration options for table storage.</param>
    public TableStoragePeerRegistry(IOptions<TableStorageRegistryOptions> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(options.Value);
        ArgumentException.ThrowIfNullOrEmpty(options.Value.ConnectionString);
        ArgumentException.ThrowIfNullOrEmpty(options.Value.TableName);
        ArgumentException.ThrowIfNullOrEmpty(options.Value.PartitionKey);

        tableClient = new TableClient(options.Value.ConnectionString, options.Value.TableName);
        partitionKey = options.Value.PartitionKey;
    }

    /// <inheritdoc />
    public async Task AddOrUpdatePeerAsync(PeerNode node, PeerStatus status, CancellationToken cancellationToken)
    {
        await EnsureTableExistsAsync(cancellationToken).ConfigureAwait(false);

        string host = string.Empty;
        int port = 0;

        if (node.Endpoint is HttpPeerEndpoint httpEndpoint)
        {
            host = httpEndpoint.Host;
            port = httpEndpoint.Port;
        }

        // Using standard TableEntity (dictionary) avoids reflection and maintains strict AOT compatibility.
        var entity = new TableEntity(partitionKey, node.Id.Value.ToString("N"))
        {
            { "Host", host },
            { "Port", port },
            { "EndpointType", node.Endpoint?.GetType().Name ?? "Unknown" },
            { "Status", status.ToString() },
            { "LastSeen", DateTimeOffset.UtcNow }
        };

        await tableClient.UpsertEntityAsync(entity, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task RemovePeerAsync(PeerId peerId, CancellationToken cancellationToken)
    {
        await EnsureTableExistsAsync(cancellationToken).ConfigureAwait(false);

        await tableClient.DeleteEntityAsync(
            partitionKey, 
            peerId.Value.ToString("N"), 
            ETag.All, 
            cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> GetAllPeersAsync(CancellationToken cancellationToken)
    {
        await EnsureTableExistsAsync(cancellationToken).ConfigureAwait(false);

        var nodes = new List<PeerNode>();
        var query = tableClient.QueryAsync<TableEntity>(
            $"PartitionKey eq '{partitionKey}'", 
            cancellationToken: cancellationToken);

        await foreach (var entity in query.ConfigureAwait(false))
        {
            nodes.Add(ParseEntity(entity));
        }

        return nodes;
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> GetPeersByStatusAsync(PeerStatus status, CancellationToken cancellationToken)
    {
        await EnsureTableExistsAsync(cancellationToken).ConfigureAwait(false);

        var nodes = new List<PeerNode>();
        var statusString = status.ToString();
        var query = tableClient.QueryAsync<TableEntity>(
            $"PartitionKey eq '{partitionKey}' and Status eq '{statusString}'", 
            cancellationToken: cancellationToken);

        await foreach (var entity in query.ConfigureAwait(false))
        {
            nodes.Add(ParseEntity(entity));
        }

        return nodes;
    }

    private async ValueTask EnsureTableExistsAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref isTableEnsured) == 1)
        {
            return;
        }

        await tableClient.CreateIfNotExistsAsync(cancellationToken).ConfigureAwait(false);
        Interlocked.Exchange(ref isTableEnsured, 1);
    }

    private static PeerNode ParseEntity(TableEntity entity)
    {
        var idValue = Guid.Parse(entity.RowKey);
        var id = new PeerId(idValue);

        var host = entity.GetString("Host") ?? string.Empty;
        var port = entity.GetInt32("Port") ?? 0;
        
        var endpoint = new HttpPeerEndpoint(host, port);

        return new PeerNode(id, endpoint);
    }
}