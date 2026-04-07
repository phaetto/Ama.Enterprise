namespace Ama.Enterprise.P2p.Services.Gossip;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Hosting;

/// <summary>
/// A background service that ties the P2P protocol to the .NET generic host lifetime.
/// Ensures the network starts when the application starts and shuts down gracefully.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="P2pHostedService"/> class.
/// </remarks>
/// <param name="p2pProtocol">The generic P2P protocol orchestrator.</param>
/// <exception cref="ArgumentNullException">Thrown when <paramref name="p2pProtocol"/> is null.</exception>
public sealed class P2pHostedService(IP2pProtocol p2pProtocol) : IHostedService
{
    private readonly IP2pProtocol p2pProtocol = p2pProtocol ?? throw new ArgumentNullException(nameof(p2pProtocol));

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        return p2pProtocol.StartAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return p2pProtocol.StopAsync(cancellationToken);
    }
}