using Microsoft.Extensions.Hosting;

namespace Ama.Enterprise.P2p.Services.Gossip;

/// <summary>
/// A background service that ties the P2P Gossip protocol to the .NET generic host lifetime.
/// Ensures the network starts when the application starts and shuts down gracefully.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="P2pHostedService"/> class.
/// </remarks>
/// <param name="gossipProtocol">The gossip protocol orchestrator.</param>
/// <exception cref="ArgumentNullException">Thrown when <paramref name="gossipProtocol"/> is null.</exception>
public sealed class P2pHostedService(IGossipProtocol gossipProtocol) : IHostedService
{
    private readonly IGossipProtocol gossipProtocol = gossipProtocol ?? throw new ArgumentNullException(nameof(gossipProtocol));

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