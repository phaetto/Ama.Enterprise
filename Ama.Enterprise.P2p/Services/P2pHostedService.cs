namespace Ama.Enterprise.P2p.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// An orchestrating background service that boots all configured Keyed P2P meshes across the application lifecycle.
/// </summary>
public sealed class P2pHostedService : IHostedService
{
    private readonly IServiceProvider serviceProvider;
    private readonly IEnumerable<P2pMeshMetadata> meshes;
    private readonly IP2pProtocol p2pProtocol;
    private readonly ILogger<P2pHostedService> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="P2pHostedService"/> class.
    /// </summary>
    public P2pHostedService(
        IServiceProvider serviceProvider,
        IEnumerable<P2pMeshMetadata> meshes,
        IP2pProtocol p2pProtocol,
        ILogger<P2pHostedService> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
        this.p2pProtocol = p2pProtocol ?? throw new ArgumentNullException(nameof(p2pProtocol));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Start Global Transport Listeners mapping across multiplexed polymorphic messages cleanly natively
        var listeners = serviceProvider.GetServices<ITransportListener>();
        foreach (var listener in listeners)
        {
            await listener.StartListeningAsync(async msg => 
            {
                if (msg is GossipMessage gossipMsg)
                {
                    var targetQueue = serviceProvider.GetKeyedService<IInboundMessageQueue<GossipMessage>>(msg.MeshId);
                    if (targetQueue is not null)
                    {
                        await targetQueue.WriteAsync(gossipMsg, default).ConfigureAwait(false);
                    }
                    else
                    {
                        logger.LogWarning("Received multiplexed gossip message for unknown mesh {MeshId}.", msg.MeshId);
                    }
                }
                else
                {
                    // Designed for future generic expansion (e.g., PushPullMessage) decoupled appropriately natively
                    logger.LogDebug("Received unhandled multiplexed protocol message type {MessageType} for mesh {MeshId}.", msg.GetType().Name, msg.MeshId);
                }
            }, cancellationToken).ConfigureAwait(false);
        }

        foreach (var mesh in meshes)
        {
            logger.LogInformation("Orchestrating startup for P2P mesh network: {MeshId}", mesh.MeshId);

            var discovery = serviceProvider.GetKeyedService<IPeerDiscovery>(mesh.MeshId);
            if (discovery is IHostedService hostedDiscovery)
            {
                await hostedDiscovery.StartAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        // Start the overarching protocol across all defined meshes
        await p2pProtocol.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // Stop the protocol globally first to halt processing
        await p2pProtocol.StopAsync(cancellationToken).ConfigureAwait(false);

        foreach (var mesh in meshes)
        {
            logger.LogInformation("Orchestrating shutdown for P2P mesh network: {MeshId}", mesh.MeshId);

            var discovery = serviceProvider.GetKeyedService<IPeerDiscovery>(mesh.MeshId);
            if (discovery is IHostedService hostedDiscovery)
            {
                await hostedDiscovery.StopAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        // Stop Global Transport Listeners
        var listeners = serviceProvider.GetServices<ITransportListener>();
        foreach (var listener in listeners)
        {
            await listener.StopListeningAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}