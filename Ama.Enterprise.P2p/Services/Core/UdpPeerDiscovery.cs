namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of IPeerDiscovery using UDP multicast for local network discovery.
/// Also implements IHostedService to run a background listener that responds to incoming discovery requests.
/// </summary>
public sealed class UdpPeerDiscovery : IPeerDiscovery, IHostedService, IDisposable
{
    private readonly UdpDiscoveryOptions options;
    private readonly ILogger<UdpPeerDiscovery> logger;
    private readonly IPeerRegistry peerRegistry;
    private readonly JsonSerializerOptions serializerOptions;
    
    private UdpClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? listenTask;
    private Task? discoveryTask;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="UdpPeerDiscovery"/> class.
    /// </summary>
    /// <param name="options">The UDP discovery configuration options.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="peerRegistry">The peer registry to populate with discovered nodes.</param>
    /// <param name="serializerOptions">The JSON serializer options provided by the base library.</param>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public UdpPeerDiscovery(
        IOptions<UdpDiscoveryOptions> options, 
        ILogger<UdpPeerDiscovery> logger,
        IPeerRegistry peerRegistry,
        [FromKeyedServices("Ama.CRDT")] JsonSerializerOptions serializerOptions)
    {
        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        this.options = options.Value ?? throw new ArgumentException("Options value cannot be null", nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.peerRegistry = peerRegistry ?? throw new ArgumentNullException(nameof(peerRegistry));
        this.serializerOptions = serializerOptions ?? throw new ArgumentNullException(nameof(serializerOptions));
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        backgroundTaskCancellationSource = new CancellationTokenSource();
        
        var localIpEndpoint = new IPEndPoint(IPAddress.Any, options.MulticastPort);
        listener = new UdpClient(AddressFamily.InterNetwork);
        
        // Allow multiple listeners on the same port (e.g., multiple node instances on the same machine)
        listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        listener.Client.Bind(localIpEndpoint);
        
        var multicastAddress = IPAddress.Parse(options.MulticastAddress);
        listener.JoinMulticastGroup(multicastAddress);

        listenTask = ListenLoopAsync(backgroundTaskCancellationSource.Token);
        discoveryTask = DiscoveryLoopAsync(backgroundTaskCancellationSource.Token);

        logger.LogInformation(
            "UDP Peer Discovery started listening on multicast group {Address}:{Port}", 
            options.MulticastAddress, 
            options.MulticastPort);

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

        var tasksToWait = new List<Task>();

        if (listenTask is not null)
        {
            tasksToWait.Add(listenTask);
        }

        if (discoveryTask is not null)
        {
            tasksToWait.Add(discoveryTask);
        }

        if (tasksToWait.Count > 0)
        {
            try
            {
                await Task.WhenAll(tasksToWait).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        listener?.Close();
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);

        var discoveredPeers = new HashSet<PeerNode>();
        
        using var client = new UdpClient(AddressFamily.InterNetwork);
        client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        
        // Bind to any available local port for receiving responses
        client.Client.Bind(new IPEndPoint(IPAddress.Any, 0)); 
        
        var requestBytes = new byte[] { (byte)'D', (byte)'I', (byte)'S', (byte)'C' };
        var targetEndpoint = new IPEndPoint(IPAddress.Parse(options.MulticastAddress), options.MulticastPort);
        
        await client.SendAsync(requestBytes, requestBytes.Length, targetEndpoint).ConfigureAwait(false);
        
        using var timeoutCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCancellationSource.CancelAfter(options.DiscoveryTimeout);
        
        try
        {
            while (!timeoutCancellationSource.Token.IsCancellationRequested)
            {
                var result = await client.ReceiveAsync(timeoutCancellationSource.Token).ConfigureAwait(false);
                var payload = result.Buffer;

                try
                {
                    var typeInfo = serializerOptions.GetTypeInfo(typeof(PeerNode));
                    var deserializedResult = JsonSerializer.Deserialize(payload, typeInfo);

                    if (deserializedResult is PeerNode node && node.Id.Value != options.LocalPeerId && node.Id.Value != Guid.Empty)
                    {
                        discoveredPeers.Add(node);
                    }
                }
                catch (JsonException)
                {
                    // Not a valid PeerNode JSON, ignore and continue listening
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout reached, this is the expected behavior for ending the discovery window
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "An error occurred while receiving UDP discovery responses.");
        }
        
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
        listener?.Dispose();
        isDisposed = true;
    }

    private async Task ListenLoopAsync(CancellationToken token)
    {
        if (listener is null)
        {
            return;
        }

        try
        {
            while (!token.IsCancellationRequested)
            {
                var result = await listener.ReceiveAsync(token).ConfigureAwait(false);
                var payload = result.Buffer;
                
                // Fast check to ensure the payload is our known "DISC" request
                if (payload.Length == 4 && payload[0] == 'D' && payload[1] == 'I' && payload[2] == 'S' && payload[3] == 'C')
                {
                    var localId = new PeerId(options.LocalPeerId);
                    var localEndpoint = new PeerEndpoint(options.LocalEndpointHost, options.LocalEndpointPort);
                    var localNode = new PeerNode(localId, localEndpoint);
                    
                    var typeInfo = serializerOptions.GetTypeInfo(typeof(PeerNode));
                    var responseBytes = JsonSerializer.SerializeToUtf8Bytes(localNode, typeInfo);
                    
                    // Send the response directly to the endpoint that requested discovery
                    await listener.SendAsync(responseBytes, responseBytes.Length, result.RemoteEndPoint).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Task was cancelled via token, graceful shutdown
        }
        catch (SocketException ex)
        {
            logger.LogDebug(ex, "UDP listener socket exception during shutdown or network configuration change.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error in UDP peer discovery background listen loop.");
        }
    }

    private async Task DiscoveryLoopAsync(CancellationToken token)
    {
        // Add a small initial delay to avoid a discovery storm immediately upon startup
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
            try
            {
                var discoveredPeers = await DiscoverPeersAsync(token).ConfigureAwait(false);
                
                foreach (var peer in discoveredPeers)
                {
                    await peerRegistry.AddOrUpdatePeerAsync(peer, PeerStatus.Active, token).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error in UDP peer discovery loop.");
            }

            if (token.IsCancellationRequested)
            {
                break;
            }

            try
            {
                await Task.Delay(options.DiscoveryInterval, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }
    }
}