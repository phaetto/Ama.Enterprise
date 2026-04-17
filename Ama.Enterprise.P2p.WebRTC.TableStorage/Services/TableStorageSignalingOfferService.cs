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
/// Background hosted service continuously polling Azure Table Storage to generate and monitor WebRTC SDP invitations.
/// By constantly broadcasting connectivity offers, this service guarantees that the node will form outbound links
/// to the rest of the cluster, maintaining a complete full-mesh P2P network.
/// </summary>
public sealed class TableStorageSignalingOfferService : BackgroundService
{
    private readonly string meshId;
    private readonly IOptionsMonitor<TableStorageSignalingOptions> optionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<TableStorageSignalingOfferService> logger;

    private Guid? currentOfferConnectionId;

    /// <summary>
    /// Initializes a new instance of the <see cref="TableStorageSignalingOfferService"/> class.
    /// </summary>
    public TableStorageSignalingOfferService(
        string meshId,
        IOptionsMonitor<TableStorageSignalingOptions> optionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        IServiceProvider serviceProvider,
        ILogger<TableStorageSignalingOfferService> logger)
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
                if (string.IsNullOrWhiteSpace(options.ConnectionString))
                {
                    logger.LogWarning("[{MeshId}] Table Storage signaling ConnectionString is empty. Offer generation signaling disabled.", meshId);
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
                logger.LogError(ex, "[{MeshId}] Error during WebRTC Table Storage offer signaling cycle intercepted.", meshId);
                
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

        // Maintain and monitor localized active offers specifically
        if (currentOfferConnectionId.HasValue)
        {
            try
            {
                var response = await tableClient.GetEntityAsync<TableEntity>(
                    meshId, 
                    currentOfferConnectionId.Value.ToString(), 
                    cancellationToken: cancellationToken).ConfigureAwait(false);
                    
                var model = response.Value.ToModel();

                if (!string.IsNullOrWhiteSpace(model.AnswerSdp))
                {
                    logger.LogInformation("[{MeshId}] Received WebRTC signaling answer mapped for connection {ConnectionId}.", meshId, currentOfferConnectionId.Value);
                    
                    await invitationService.FinalizeInvitationAsync(currentOfferConnectionId.Value, model.AnswerSdp, cancellationToken).ConfigureAwait(false);
                    await tableClient.DeleteEntityAsync(meshId, currentOfferConnectionId.Value.ToString(), cancellationToken: cancellationToken).ConfigureAwait(false);
                    
                    currentOfferConnectionId = null;
                }
                else
                {
                    if (model.CreatedAt < DateTimeOffset.UtcNow.Subtract(options.OfferExpiration))
                    {
                        // Clean up timed out offers ensuring proper table hygiene
                        await tableClient.DeleteEntityAsync(meshId, currentOfferConnectionId.Value.ToString(), cancellationToken: cancellationToken).ConfigureAwait(false);
                        currentOfferConnectionId = null;
                    }
                }
            }
            catch (RequestFailedException ex) when (ex.Status == 404)
            {
                // Active localized offer deleted out-of-band
                currentOfferConnectionId = null;
            }
        }

        // Generate local offers ensuring robust network topology discovery
        if (!currentOfferConnectionId.HasValue && options.EnableOfferGeneration)
        {
            var invitationResult = await invitationService.CreateInvitationAsync(cancellationToken).ConfigureAwait(false);
            currentOfferConnectionId = invitationResult.ConnectionId;

            var newModel = new WebRtcSignalingModel(
                MeshId: meshId,
                ConnectionId: invitationResult.ConnectionId.ToString(),
                CreatorPeerId: localPeerId,
                OfferSdp: invitationResult.SdpOffer,
                CreatedAt: DateTimeOffset.UtcNow,
                AnswerSdp: null,
                ResponderPeerId: null,
                ETag: default
            );

            await tableClient.AddEntityAsync(newModel.ToTableEntity(), cancellationToken).ConfigureAwait(false);
            logger.LogDebug("[{MeshId}] Published new WebRTC signaling active offer {ConnectionId}.", meshId, currentOfferConnectionId.Value);
        }
    }
}