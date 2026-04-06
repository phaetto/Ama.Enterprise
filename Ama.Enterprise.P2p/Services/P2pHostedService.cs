using Microsoft.Extensions.Hosting;

namespace Ama.Enterprise.P2p.Services;

/// <summary>
/// A background service that ties the P2P Gossip protocol to the .NET generic host lifetime.
/// Ensures the network starts when the application starts and shuts down gracefully.
/// </summary>
public sealed class P2pHostedService : IHostedService
{
    private readonly IGossipProtocol gossipProtocol;

    /// <summary>
    /// Initializes a new instance of the <see cref="P2pHostedService"/> class.
    /// </summary>
    /// <param name="gossipProtocol">The gossip protocol orchestrator.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="gossipProtocol"/> is null.</exception>
    public P2pHostedService(IGossipProtocol gossipProtocol)
    {
        this.gossipProtocol = gossipProtocol ?? throw new ArgumentNullException(nameof(gossipProtocol));
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return gossipProtocol.StartAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return gossipProtocol.StopAsync(cancellationToken);
    }
}