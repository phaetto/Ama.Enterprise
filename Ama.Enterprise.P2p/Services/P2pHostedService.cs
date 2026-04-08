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
    private readonly ILogger<P2pHostedService> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="P2pHostedService"/> class.
    /// </summary>
    public P2pHostedService(
        IServiceProvider serviceProvider,
        IEnumerable<P2pMeshMetadata> meshes,
        ILogger<P2pHostedService> logger)
    {
        this.serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        this.meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var mesh in meshes)
        {
            logger.LogInformation("Orchestrating startup for P2P mesh network: {MeshId}", mesh.MeshId);

            var discovery = serviceProvider.GetKeyedService<IPeerDiscovery>(mesh.MeshId);
            if (discovery is IHostedService hostedDiscovery)
            {
                await hostedDiscovery.StartAsync(cancellationToken).ConfigureAwait(false);
            }

            var queue = serviceProvider.GetKeyedService<IInboundMessageQueue<GossipMessage>>(mesh.MeshId);
            var listeners = serviceProvider.GetKeyedServices<ITransportListener<GossipMessage>>(mesh.MeshId);
            
            if (queue is not null && listeners is not null)
            {
                foreach (var listener in listeners)
                {
                    await listener.StartListeningAsync(async msg => 
                        await queue.WriteAsync(msg, default).ConfigureAwait(false), cancellationToken).ConfigureAwait(false);
                }
            }

            var protocol = serviceProvider.GetKeyedService<IP2pProtocol>(mesh.MeshId);
            if (protocol is not null)
            {
                await protocol.StartAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var mesh in meshes)
        {
            logger.LogInformation("Orchestrating shutdown for P2P mesh network: {MeshId}", mesh.MeshId);

            var protocol = serviceProvider.GetKeyedService<IP2pProtocol>(mesh.MeshId);
            if (protocol is not null)
            {
                await protocol.StopAsync(cancellationToken).ConfigureAwait(false);
            }

            var listeners = serviceProvider.GetKeyedServices<ITransportListener<GossipMessage>>(mesh.MeshId);
            if (listeners is not null)
            {
                foreach (var listener in listeners)
                {
                    await listener.StopListeningAsync(cancellationToken).ConfigureAwait(false);
                }
            }

            var discovery = serviceProvider.GetKeyedService<IPeerDiscovery>(mesh.MeshId);
            if (discovery is IHostedService hostedDiscovery)
            {
                await hostedDiscovery.StopAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}