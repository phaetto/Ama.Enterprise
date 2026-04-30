namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerDiscovery using DNS resolution (Phase 1) paired with abstract active handshaking (Phase 2).
/// </summary>
public sealed class DnsPeerDiscovery : IPeerDiscovery, IHostedService, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<DnsDiscoveryOptions> discoveryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint;
    private readonly IPeerHandshaker handshaker;
    private readonly ILogger<DnsPeerDiscovery> logger;
    private readonly IPeerRegistry peerRegistry;
    private readonly IPeerAuthenticator authenticator;
    private readonly IFailureDetector failureDetector;

    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? discoveryTask;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="DnsPeerDiscovery"/> class.
    /// </summary>
    /// <param name="meshId">The mesh context identifier.</param>
    /// <param name="discoveryOptionsMonitor">The DNS discovery configuration options monitor.</param>
    /// <param name="nodeOptionsMonitor">The global node configuration options monitor.</param>
    /// <param name="localEndpoint">The local network endpoint to advertise.</param>
    /// <param name="handshaker">The active handshaker implementation handling target resolution.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="peerRegistry">The peer registry to populate with discovered nodes.</param>
    /// <param name="authenticator">The peer authenticator to validate remote node connections.</param>
    /// <param name="failureDetector">The failure detector to track heartbeat signals during discovery pings.</param>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public DnsPeerDiscovery(
        string meshId,
        IOptionsMonitor<DnsDiscoveryOptions> discoveryOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        IPeerHandshaker handshaker,
        ILogger<DnsPeerDiscovery> logger,
        IPeerRegistry peerRegistry,
        IPeerAuthenticator authenticator,
        IFailureDetector failureDetector)
    {
        ArgumentNullException.ThrowIfNull(meshId);
        ArgumentNullException.ThrowIfNull(discoveryOptionsMonitor);
        ArgumentNullException.ThrowIfNull(nodeOptionsMonitor);
        ArgumentNullException.ThrowIfNull(localEndpoint);
        ArgumentNullException.ThrowIfNull(handshaker);
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(authenticator);
        ArgumentNullException.ThrowIfNull(failureDetector);

        this.meshId = meshId;
        this.discoveryOptionsMonitor = discoveryOptionsMonitor;
        this.nodeOptionsMonitor = nodeOptionsMonitor;
        this.localEndpoint = localEndpoint;
        this.handshaker = handshaker;
        this.logger = logger;
        this.peerRegistry = peerRegistry;
        this.authenticator = authenticator;
        this.failureDetector = failureDetector;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var options = discoveryOptionsMonitor.Get(meshId);
        backgroundTaskCancellationSource = new CancellationTokenSource();

        discoveryTask = DiscoveryLoopAsync(backgroundTaskCancellationSource.Token);

        logger.LogInformation(
            "[{MeshId}] DNS Peer Discovery started watching hostname {Hostname}",
            meshId,
            options.Hostname);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (backgroundTaskCancellationSource is null)
        {
            return;
        }

        await backgroundTaskCancellationSource.CancelAsync().ConfigureAwait(false);

        if (discoveryTask is not null)
        {
            try
            {
                await discoveryTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var options = discoveryOptionsMonitor.Get(meshId);
        var nodeOptions = nodeOptionsMonitor.Get(meshId);
        var discoveredPeers = new HashSet<PeerNode>();

        IPAddress[] resolvedAddresses;
        try
        {
            resolvedAddresses = await Dns.GetHostAddressesAsync(options.Hostname, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to resolve DNS hostname {Hostname}.", meshId, options.Hostname);
            return discoveredPeers;
        }

        var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);

        var handshakeTasks = resolvedAddresses.Select(async ip =>
        {
            try
            {
                using var timeoutCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCancellationSource.CancelAfter(options.HandshakeTimeout);

                var targetEndpoint = new IPEndPoint(ip, options.Port);
                var remoteNode = await handshaker.HandshakeAsync(localNode, targetEndpoint, timeoutCancellationSource.Token).ConfigureAwait(false);

                // Check .HasValue since PeerNode? maps to Nullable<PeerNode> struct bounds
                if (remoteNode.HasValue && remoteNode.Value.Id.Value != nodeOptions.LocalPeerId && remoteNode.Value.Id.Value != Guid.Empty)
                {
                    var isAuthenticated = await authenticator.AuthenticateAsync(remoteNode.Value, ReadOnlyMemory<byte>.Empty, timeoutCancellationSource.Token).ConfigureAwait(false);

                    if (isAuthenticated)
                    {
                        await failureDetector.RecordHeartbeatAsync(remoteNode.Value.Id, timeoutCancellationSource.Token).ConfigureAwait(false);
                        lock (discoveredPeers)
                        {
                            discoveredPeers.Add(remoteNode.Value);
                        }
                    }
                    else
                    {
                        logger.LogDebug("[{MeshId}] DNS handshaked peer {PeerId} failed authentication.", meshId, remoteNode.Value.Id.Value);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                logger.LogTrace(ex, "[{MeshId}] Active handshake failed for IP {IpAddress}.", meshId, ip);
            }
        });

        await Task.WhenAll(handshakeTasks).ConfigureAwait(false);

        return discoveredPeers;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        backgroundTaskCancellationSource?.Cancel();
        backgroundTaskCancellationSource?.Dispose();
        isDisposed = true;
    }

    private async Task DiscoveryLoopAsync(CancellationToken token)
    {
        try
        {
            // Initial jitter delay
            await Task.Delay(TimeSpan.FromSeconds(2), token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        while (!token.IsCancellationRequested)
        {
            var options = discoveryOptionsMonitor.Get(meshId);

            try
            {
                var discoveredPeers = await DiscoverPeersAsync(token).ConfigureAwait(false);

                foreach (var peer in discoveredPeers)
                {
                    await peerRegistry.AddOrUpdatePeerAsync(meshId, peer, PeerStatus.Active, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "[{MeshId}] Unexpected error in DNS peer discovery loop.", meshId);
            }

            if (token.IsCancellationRequested)
            {
                break;
            }

            try
            {
                await Task.Delay(options.DiscoveryInterval, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { }
        }
    }
}