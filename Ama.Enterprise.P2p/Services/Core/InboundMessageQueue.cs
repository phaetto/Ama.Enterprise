namespace Ama.Enterprise.P2p.Services.Core;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

/// <summary>
/// Channel-backed implementation of the inbound message queue, decoupling network IO from protocol logic.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public sealed class InboundMessageQueue<TMessage> : IInboundMessageQueue<TMessage>
{
    private readonly Channel<TMessage> channel = Channel.CreateUnbounded<TMessage>();

    /// <inheritdoc />
    public ValueTask WriteAsync(TMessage message, CancellationToken cancellationToken)
    {
        return channel.Writer.WriteAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<TMessage> ReadAllAsync(CancellationToken cancellationToken)
    {
        return channel.Reader.ReadAllAsync(cancellationToken);
    }
}