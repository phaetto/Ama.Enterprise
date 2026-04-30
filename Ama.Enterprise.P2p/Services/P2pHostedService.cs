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
/// <remarks>
/// Initializes a new instance of the <see cref="P2pHostedService"/> class.
/// </remarks>
public sealed class P2pHostedService(
    IServiceProvider serviceProvider,
    IEnumerable<P2pMeshMetadata> meshes,
    IP2pProtocol p2pProtocol,
    ILogger<P2pHostedService> logger) : IHostedService
{
    private readonly IServiceProvider serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    private readonly IEnumerable<P2pMeshMetadata> meshes = meshes ?? throw new ArgumentNullException(nameof(meshes));
    private readonly IP2pProtocol p2pProtocol = p2pProtocol ?? throw new ArgumentNullException(nameof(p2pProtocol));
    private readonly ILogger<P2pHostedService> logger = logger ?? throw new ArgumentNullException(nameof(logger));

    /// <inheritdoc />
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var mesh in meshes)
        {
            logger.LogInformation("Orchestrating startup for P2P mesh network: {MeshId}", mesh.MeshId);

            var listeners = serviceProvider.GetKeyedServices<ITransportListener>(mesh.MeshId);
            foreach (var listener in listeners)
            {
                await listener.StartListeningAsync(async msg => 
                {
                    var incomingVersion = new Version(0, 0, 0);
                    if (!string.IsNullOrWhiteSpace(msg.ProtocolVersion) && Version.TryParse(msg.ProtocolVersion, out var parsedVersion))
                    {
                        incomingVersion = parsedVersion;
                    }

                    var localVersion = Version.Parse(Constants.ProtocolVersion);
                    if (incomingVersion.Major != localVersion.Major)
                    {
                        logger.LogWarning("[{MeshId}] Rejected incoming protocol message due to major version mismatch. Local: {LocalVersion}, Incoming: {IncomingVersion}", mesh.MeshId, localVersion, incomingVersion);
                        throw new NotSupportedException($"Protocol major version mismatch. Local: {localVersion.Major}, Incoming: {incomingVersion.Major}");
                    }

                    if (!string.Equals(msg.MeshId, mesh.MeshId, StringComparison.Ordinal))
                    {
                        logger.LogWarning("[{ExpectedMeshId}] Listener received message isolated for a different mesh {ActualMeshId}.", mesh.MeshId, msg.MeshId);
                        return;
                    }

                    if (msg is GossipMessage gossipMsg)
                    {
                        var targetQueue = serviceProvider.GetKeyedService<IInboundMessageQueue<GossipMessage>>(mesh.MeshId);
                        if (targetQueue is not null)
                        {
                            await targetQueue.WriteAsync(gossipMsg, default).ConfigureAwait(false);
                        }
                        else
                        {
                            logger.LogWarning("[{MeshId}] Received gossip message for unknown isolated inbound queue.", mesh.MeshId);
                        }
                    }
                    else
                    {
                        logger.LogDebug("[{MeshId}] Received unhandled protocol message type {MessageType}.", mesh.MeshId, msg.GetType().Name);
                    }
                }, cancellationToken).ConfigureAwait(false);
            }

            var handshaker = serviceProvider.GetKeyedService<IPeerHandshaker>(mesh.MeshId);
            if (handshaker is IHostedService hostedHandshaker)
            {
                await hostedHandshaker.StartAsync(cancellationToken).ConfigureAwait(false);
            }

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

            var handshaker = serviceProvider.GetKeyedService<IPeerHandshaker>(mesh.MeshId);
            if (handshaker is IHostedService hostedHandshaker)
            {
                await hostedHandshaker.StopAsync(cancellationToken).ConfigureAwait(false);
            }

            var listeners = serviceProvider.GetKeyedServices<ITransportListener>(mesh.MeshId);
            foreach (var listener in listeners)
            {
                await listener.StopListeningAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}