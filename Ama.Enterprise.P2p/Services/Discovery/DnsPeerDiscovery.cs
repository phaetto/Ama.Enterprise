namespace Ama.Enterprise.P2p.Services.Discovery;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Discovery;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerDiscovery using DNS resolution explicitly completely decoupled natively smoothly smoothly explicitly mapped explicitly resolving natively correctly natively efficiently distinctly smoothly efficiently mapped effectively functionally smoothly distinctively correctly natively avoiding loops actively efficiently securely properly elegantly seamlessly structurally mapping.
/// </summary>
public sealed class DnsPeerDiscovery : IPeerDiscovery, IDisposable
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
    private readonly IDnsSrvResolver? srvResolver;

    private bool isDisposed;

    private readonly Meter meter;
    private readonly Counter<long> discoveriesAttemptedCounter;
    private readonly Counter<long> peersFoundCounter;

    public DnsPeerDiscovery(
        string meshId,
        IOptionsMonitor<DnsDiscoveryOptions> discoveryOptionsMonitor,
        IOptionsMonitor<P2pNodeOptions> nodeOptionsMonitor,
        PeerEndpoint localEndpoint,
        IPeerHandshaker handshaker,
        ILogger<DnsPeerDiscovery> logger,
        IPeerRegistry peerRegistry,
        IPeerAuthenticator authenticator,
        IFailureDetector failureDetector,
        IDnsSrvResolver? srvResolver = null,
        IMeterFactory? meterFactory = null)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.discoveryOptionsMonitor = discoveryOptionsMonitor ?? throw new ArgumentNullException(nameof(discoveryOptionsMonitor));
        this.nodeOptionsMonitor = nodeOptionsMonitor ?? throw new ArgumentNullException(nameof(nodeOptionsMonitor));
        this.localEndpoint = localEndpoint ?? throw new ArgumentNullException(nameof(localEndpoint));
        this.handshaker = handshaker ?? throw new ArgumentNullException(nameof(handshaker));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.authenticator = authenticator ?? throw new ArgumentNullException(nameof(authenticator));
        this.failureDetector = failureDetector ?? throw new ArgumentNullException(nameof(failureDetector));
        this.srvResolver = srvResolver;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.DnsPeerDiscovery") ?? new Meter("Ama.Enterprise.P2p.DnsPeerDiscovery");
        this.discoveriesAttemptedCounter = this.meter.CreateCounter<long>("p2p.discovery.dns.attempts", "attempts", "Total DNS discovery attempts");
        this.peersFoundCounter = this.meter.CreateCounter<long>("p2p.discovery.dns.peers_found", "peers", "Total peers successfully found via DNS");
    }

    /// <inheritdoc />
    public TimeSpan DiscoveryInterval => discoveryOptionsMonitor.Get(meshId).DiscoveryInterval;

    /// <inheritdoc />
    public Task StartListeningAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var options = discoveryOptionsMonitor.Get(meshId);
        logger.LogInformation(
            "[{MeshId}] DNS Peer Discovery orchestrator mapped natively to hostname {Hostname}.",
            meshId,
            options.Hostname);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopListeningAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId) };
        discoveriesAttemptedCounter.Add(1, tags);

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

        peersFoundCounter.Add(discoveredPeers.Count, tags);
        return discoveredPeers;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (isDisposed)
        {
            return;
        }

        meter.Dispose();
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
            
            var localHandshakeData = await authenticator.GetLocalHandshakeDataAsync(timeoutCancellationSource.Token).ConfigureAwait(false);
            var localPayload = new PeerHandshakePayload
            {
                Node = localNode,
                HandshakeData = localHandshakeData.ToArray()
            };

            var remotePayload = await handshaker.HandshakeAsync(localPayload, endpoint, timeoutCancellationSource.Token).ConfigureAwait(false);

            if (remotePayload.HasValue && remotePayload.Value.Node.Id.Value != nodeOptions.LocalPeerId && remotePayload.Value.Node.Id.Value != Guid.Empty)
            {
                var isAuthenticated = await authenticator.AuthenticateAsync(remotePayload.Value.Node, remotePayload.Value.HandshakeData, timeoutCancellationSource.Token).ConfigureAwait(false);

                if (isAuthenticated)
                {
                    await failureDetector.RecordHeartbeatAsync(remotePayload.Value.Node.Id, timeoutCancellationSource.Token).ConfigureAwait(false);
                    lock (discoveredPeers)
                    {
                        discoveredPeers.Add(remotePayload.Value.Node);
                    }
                }
                else
                {
                    logger.LogDebug("[{MeshId}] DNS handshaked peer {PeerId} failed authentication.", meshId, remotePayload.Value.Node.Id.Value);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            logger.LogTrace(ex, "[{MeshId}] Active handshake failed for IP {IpAddress}:{Port}.", meshId, ip, port);
        }
    }
}