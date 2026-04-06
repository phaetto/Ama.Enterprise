namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
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
    
    private UdpClient? listener;
    private CancellationTokenSource? backgroundTaskCancellationSource;
    private Task? listenTask;
    private bool isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="UdpPeerDiscovery"/> class.
    /// </summary>
    /// <param name="options">The UDP discovery configuration options.</param>
    /// <param name="logger">The logger instance.</param>
    /// <exception cref="ArgumentNullException">Thrown if any argument is null.</exception>
    public UdpPeerDiscovery(IOptions<UdpDiscoveryOptions> options, ILogger<UdpPeerDiscovery> logger)
    {
        if (options is null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        this.options = options.Value ?? throw new ArgumentException("Options value cannot be null", nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.isDisposed, this);

        this.backgroundTaskCancellationSource = new CancellationTokenSource();
        
        var localIpEndpoint = new IPEndPoint(IPAddress.Any, this.options.MulticastPort);
        this.listener = new UdpClient(AddressFamily.InterNetwork);
        
        // Allow multiple listeners on the same port (e.g., multiple node instances on the same machine)
        this.listener.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        this.listener.Client.Bind(localIpEndpoint);
        
        var multicastAddress = IPAddress.Parse(this.options.MulticastAddress);
        this.listener.JoinMulticastGroup(multicastAddress);

        this.listenTask = this.ListenLoopAsync(this.backgroundTaskCancellationSource.Token);

        this.logger.LogInformation(
            "UDP Peer Discovery started listening on multicast group {Address}:{Port}", 
            this.options.MulticastAddress, 
            this.options.MulticastPort);

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (this.backgroundTaskCancellationSource is null)
        {
            return;
        }

        await this.backgroundTaskCancellationSource.CancelAsync().ConfigureAwait(false);

        if (this.listenTask is not null)
        {
            try
            {
                await this.listenTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected during shutdown
            }
        }

        this.listener?.Close();
    }

    /// <inheritdoc />
    public async Task<IEnumerable<PeerNode>> DiscoverPeersAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(this.isDisposed, this);

        var discoveredPeers = new HashSet<PeerNode>();
        
        using var client = new UdpClient(AddressFamily.InterNetwork);
        client.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
        
        // Bind to any available local port for receiving responses
        client.Client.Bind(new IPEndPoint(IPAddress.Any, 0)); 
        
        var requestBytes = new byte[] { (byte)'D', (byte)'I', (byte)'S', (byte)'C' };
        var targetEndpoint = new IPEndPoint(IPAddress.Parse(this.options.MulticastAddress), this.options.MulticastPort);
        
        await client.SendAsync(requestBytes, requestBytes.Length, targetEndpoint).ConfigureAwait(false);
        
        using var timeoutCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutCancellationSource.CancelAfter(this.options.DiscoveryTimeout);
        
        try
        {
            while (!timeoutCancellationSource.Token.IsCancellationRequested)
            {
                var result = await client.ReceiveAsync(timeoutCancellationSource.Token).ConfigureAwait(false);
                var payload = result.Buffer;

                try
                {
                    var node = JsonSerializer.Deserialize(payload, UdpDiscoveryJsonContext.Default.PeerNode);
                    if (node.Id.Value != this.options.LocalPeerId && node.Id.Value != Guid.Empty)
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
            this.logger.LogWarning(ex, "An error occurred while receiving UDP discovery responses.");
        }
        
        return discoveredPeers;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (this.isDisposed)
        {
            return;
        }

        this.backgroundTaskCancellationSource?.Cancel();
        this.backgroundTaskCancellationSource?.Dispose();
        this.listener?.Dispose();
        this.isDisposed = true;
    }

    private async Task ListenLoopAsync(CancellationToken token)
    {
        if (this.listener is null)
        {
            return;
        }

        try
        {
            while (!token.IsCancellationRequested)
            {
                var result = await this.listener.ReceiveAsync(token).ConfigureAwait(false);
                var payload = result.Buffer;
                
                // Fast check to ensure the payload is our known "DISC" request
                if (payload.Length == 4 && payload[0] == 'D' && payload[1] == 'I' && payload[2] == 'S' && payload[3] == 'C')
                {
                    var localId = new PeerId(this.options.LocalPeerId);
                    var localEndpoint = new PeerEndpoint(this.options.LocalEndpointHost, this.options.LocalEndpointPort);
                    var localNode = new PeerNode(localId, localEndpoint);
                    
                    var responseBytes = JsonSerializer.SerializeToUtf8Bytes(localNode, UdpDiscoveryJsonContext.Default.PeerNode);
                    
                    // Send the response directly to the endpoint that requested discovery
                    await this.listener.SendAsync(responseBytes, responseBytes.Length, result.RemoteEndPoint).ConfigureAwait(false);
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Task was cancelled via token, graceful shutdown
        }
        catch (SocketException ex)
        {
            this.logger.LogDebug(ex, "UDP listener socket exception during shutdown or network configuration change.");
        }
        catch (Exception ex)
        {
            this.logger.LogError(ex, "Unexpected error in UDP peer discovery background listen loop.");
        }
    }
}