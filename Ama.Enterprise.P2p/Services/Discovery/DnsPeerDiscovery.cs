namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
using System.Collections.Generic;
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
/// Supports optional SRV record routing through injected dependencies.
/// </summary>
public sealed class DnsPeerDiscovery(
    string meshId,
    IOptionsMonitor<DnsDiscoveryOptions> discoveryOptionsMonitor,
    IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
    PeerEndpoint localEndpoint,
    IPeerHandshaker handshaker,
    ILogger<DnsPeerDiscovery> logger,
    IPeerRegistry peerRegistry,
    IPeerAuthenticator authenticator,
    IFailureDetector failureDetector,
    IDnsSrvResolver? srvResolver = null) : IPeerDiscovery, IHostedService, IDisposable
{
    private readonly string meshId = meshId;
    private readonly IOptionsMonitor<DnsDiscoveryOptions> discoveryOptionsMonitor = discoveryOptionsMonitor;
    private readonly IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor = nodeOptionsMonitor;
    private readonly PeerEndpoint localEndpoint = localEndpoint;
    private readonly IPeerHandshaker handshaker = handshaker;
    private readonly ILogger<DnsPeerDiscovery> logger = logger;
    private readonly IPeerRegistry peerRegistry = peerRegistry;
    private readonly IPeerAuthenticator authenticator = authenticator;
    private readonly IFailureDetector failureDetector = failureDetector;
    private readonly IDnsSrvResolver? srvResolver = srvResolver;

    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? discoveryTask;
    private bool isDisposed;

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
        var localNode = new PeerNode(new PeerId(nodeOptions.LocalPeerId), localEndpoint);

        var handshakeTasks = new List<Task>();

        if (options.UseSrvRecords)
        {
            if (srvResolver is null)
            {
                logger.LogError("[{MeshId}] DNS Discovery is configured to use SRV records, but no IDnsSrvResolver is registered in the DI container.", meshId);
                return discoveredPeers;
            }

            IEnumerable<SrvRecordTarget> srvTargets;
            try
            {
                srvTargets = await srvResolver.ResolveSrvAsync(options.Hostname, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "[{MeshId}] Failed to resolve DNS SRV records for {Hostname}.", meshId, options.Hostname);
                return discoveredPeers;
            }

            if (srvTargets is not null)
            {
                foreach (var target in srvTargets)
                {
                    if (string.IsNullOrWhiteSpace(target.Hostname))
                    {
                        continue;
                    }

                    IPAddress[] resolvedAddresses;
                    try
                    {
                        resolvedAddresses = await Dns.GetHostAddressesAsync(target.Hostname, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex)
                    {
                        logger.LogWarning(ex, "[{MeshId}] Failed to resolve IP for SRV target {TargetHostname}.", meshId, target.Hostname);
                        continue;
                    }

                    foreach (var ip in resolvedAddresses)
                    {
                        handshakeTasks.Add(PerformHandshakeAsync(ip, target.Port, options, nodeOptions, localNode, discoveredPeers, cancellationToken));
                    }
                }
            }
        }
        else
        {
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

            foreach (var ip in resolvedAddresses)
            {
                handshakeTasks.Add(PerformHandshakeAsync(ip, options.TargetPort, options, nodeOptions, localNode, discoveredPeers, cancellationToken));
            }
        }

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

    private async Task PerformHandshakeAsync(
        IPAddress ip,
        int port,
        DnsDiscoveryOptions options,
        P2pNodeOptions nodeOptions,
        PeerNode localNode,
        HashSet<PeerNode> discoveredPeers,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(ip);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(nodeOptions);
        ArgumentNullException.ThrowIfNull(discoveredPeers);

        try
        {
            using var timeoutCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeoutCancellationSource.CancelAfter(options.HandshakeTimeout);

            var endpoint = new IPEndPoint(ip, port);
            var remoteNode = await handshaker.HandshakeAsync(localNode, endpoint, timeoutCancellationSource.Token).ConfigureAwait(false);

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
            logger.LogTrace(ex, "[{MeshId}] Active handshake failed for IP {IpAddress}:{Port}.", meshId, ip, port);
        }
    }

    private async Task DiscoveryLoopAsync(CancellationToken token)
    {
        try
        {
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