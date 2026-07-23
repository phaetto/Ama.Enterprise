namespace Ama.Enterprise.Monitoring.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Background service that bootstraps the WebRTC monitoring mesh by discovering the configured parent signaling node.
/// </summary>
public sealed class WebRtcMonitoringBootstrapper : BackgroundService
{
    private readonly IServiceProvider serviceProvider;
    private readonly ILogger<WebRtcMonitoringBootstrapper> logger;
    private readonly string meshId;
    private readonly string parentSignalingUrl;

    /// <summary>
    /// Initializes a new instance of the <see cref="WebRtcMonitoringBootstrapper"/> class.
    /// </summary>
    public WebRtcMonitoringBootstrapper(
        IServiceProvider serviceProvider,
        ILogger<WebRtcMonitoringBootstrapper> logger,
        string meshId,
        string parentSignalingUrl)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentException.ThrowIfNullOrWhiteSpace(parentSignalingUrl);

        this.serviceProvider = serviceProvider;
        this.logger = logger;
        this.meshId = meshId;
        this.parentSignalingUrl = parentSignalingUrl;
    }

    /// <summary>
    /// Executes the background operation actively evaluating standalone endpoints structurally out-of-band.
    /// </summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            logger.LogInformation("Bootstrapping WebRTC monitoring mesh '{MeshId}' explicitly connecting to parent at {Url}...", meshId, parentSignalingUrl);

            var discovery = serviceProvider.GetRequiredKeyedService<IWebRtcHttpPeerDiscovery>(meshId);
            var connected = await discovery.DiscoverPeerAsync(new Uri(parentSignalingUrl), null, stoppingToken).ConfigureAwait(false);

            if (connected)
            {
                logger.LogInformation("Successfully connected to monitoring parent node via WebRTC.");
            }
            else
            {
                logger.LogWarning("Failed to connect to monitoring parent node via WebRTC signaling. Node will wait for inbound connections natively.");
            }
        }
        catch (OperationCanceledException)
        {
            // Expected normal shutdown boundary
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "An error occurred while bootstrapping the WebRTC monitoring mesh configurations.");
        }
    }
}