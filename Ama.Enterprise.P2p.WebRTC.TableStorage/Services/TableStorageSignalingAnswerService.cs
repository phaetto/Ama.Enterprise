namespace Ama.Enterprise.P2p.WebRTC.TableStorage.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.WebRTC.Services;
using Ama.Enterprise.P2p.WebRTC.TableStorage.Extensions;
using Ama.Enterprise.P2p.WebRTC.TableStorage.Models;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Background hosted service continuously polling Azure Table Storage to accept remote WebRTC SDP invitations.
/// This service works in tandem with the offer service to ensure every node in the cluster interconnects,
/// establishing a fully distributed peer-to-peer network mesh.
/// </summary>
public sealed class TableStorageSignalingAnswerService : BackgroundService
{
    private readonly string meshId;
    private readonly IOptionsMonitor<TableStorageSignalingOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<TableStorageSignalingAnswerService> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="TableStorageSignalingAnswerService"/> class.
    /// </summary>
    public TableStorageSignalingAnswerService(
        string meshId,
        IOptionsMonitor<TableStorageSignalingOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        IServiceProvider serviceProvider,
        ILogger<TableStorageSignalingAnswerService> logger)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.optionsMonitor = optionsMonitor ?? throw new ArgumentNullException(nameof(optionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        TableClient? tableClient = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            var options = optionsMonitor.Get(meshId);

            try
            {
                if (!options.EnableOfferAcceptance)
                {
                    await Task.Delay(options.PollingInterval, stoppingToken).ConfigureAwait(false);
                    continue;
                }

                if (string.IsNullOrWhiteSpace(options.ConnectionString))
                {
                    logger.LogWarning("[{MeshId}] Table Storage signaling ConnectionString is empty. Answer signaling disabled.", meshId);
                    await Task.Delay(TimeSpan.FromMinutes(1), stoppingToken).ConfigureAwait(false);
                    continue;
                }

                if (tableClient == null)
                {
                    tableClient = new TableClient(options.ConnectionString, options.TableName);
                    await tableClient.CreateIfNotExistsAsync(cancellationToken: stoppingToken).ConfigureAwait(false);
                }

                // Resolve the generic invitation service scoped per localized Mesh
                var invitationService = serviceProvider.GetRequiredKeyedService<IWebRtcInvitationService>(meshId);

                await ProcessSignalingCycleAsync(tableClient, invitationService, stoppingToken).ConfigureAwait(false);
                
                await Task.Delay(options.PollingInterval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Termination
                break;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Error during WebRTC Table Storage answer signaling cycle intercepted.", meshId);
                
                // Prevent tight loops on continuous failures
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private async Task ProcessSignalingCycleAsync(TableClient tableClient, IWebRtcInvitationService invitationService, CancellationToken cancellationToken)
    {
        var options = optionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var localPeerId = nodeOptions.LocalPeerId;

        // Scan overarching Table limits for remote disconnected peer offers matching conditions
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {meshId}");
        var query = tableClient.QueryAsync<TableEntity>(filter, cancellationToken: cancellationToken);

        await foreach (var page in query.AsPages().WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            foreach (var entity in page.Values)
            {
                var model = entity.ToModel();
                
                if (model.CreatedAt < DateTimeOffset.UtcNow.Subtract(options.OfferExpiration))
                {
                    try
                    {
                        await tableClient.DeleteEntityAsync(entity.PartitionKey, entity.RowKey, entity.ETag, cancellationToken).ConfigureAwait(false);
                    }
                    catch { /* Ignore deletion collisions */ }
                    continue;
                }

                if (model.CreatorPeerId == localPeerId)
                {
                    continue; // Skip localized loopback bounds
                }

                if (string.IsNullOrWhiteSpace(model.AnswerSdp) && !string.IsNullOrWhiteSpace(model.OfferSdp))
                {
                    try
                    {
                        var acceptResult = await invitationService.AcceptInvitationAsync(model.OfferSdp, cancellationToken).ConfigureAwait(false);

                        var updatedModel = model with 
                        { 
                            AnswerSdp = acceptResult.SdpAnswer, 
                            ResponderPeerId = localPeerId 
                        };

                        // Use Table ETag limits performing optimistic concurrency
                        await tableClient.UpdateEntityAsync(updatedModel.ToTableEntity(), updatedModel.ETag, TableUpdateMode.Replace, cancellationToken).ConfigureAwait(false);
                        
                        logger.LogInformation("[{MeshId}] Answered distributed WebRTC signaling offer {RowKey}.", meshId, model.ConnectionId);
                        
                        // Restrict answering to only one remote peer per polling limit cycle
                        return;
                    }
                    catch (RequestFailedException ex) when (ex.Status == 412)
                    {
                        logger.LogDebug("[{MeshId}] Concurrency conflict answering offer {RowKey}. Handled by another peer.", meshId, model.ConnectionId);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "[{MeshId}] Failed to accept distributed table offer {RowKey}.", meshId, model.ConnectionId);
                    }
                }
            }
        }
    }
}