namespace Ama.Enterprise.P2p.IntegrationTests.Gossip.Handlers;

using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// A test handler that simply records all incoming gossip messages for later assertions.
/// </summary>
public sealed class TestMessageHandler : IMessageHandler<GossipMessage>
{
    /// <summary>
    /// Gets the collection of messages received by this handler instance.
    /// </summary>
    public ConcurrentBag<GossipMessage> ReceivedMessages { get; } = new ConcurrentBag<GossipMessage>();

    /// <inheritdoc />
    public Task HandleAsync(GossipMessage message, CancellationToken cancellationToken)
    {
        this.ReceivedMessages.Add(message);
        return Task.CompletedTask;
    }
}