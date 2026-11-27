namespace Ama.Enterprise.P2p.Services.Core;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Channel-backed implementation of the inbound message queue, decoupling network IO from protocol logic
/// while applying TCP socket backpressure when inbound application processing lags behind network reception.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public sealed class InboundMessageQueue<TMessage> : IInboundMessageQueue<TMessage> where TMessage : IMeshMessage
{
    private const int DefaultMaxInboundQueueCapacity = 8_192;

    private readonly Channel<TMessage> channel = Channel.CreateBounded<TMessage>(new BoundedChannelOptions(DefaultMaxInboundQueueCapacity)
    {
        SingleReader = false,
        SingleWriter = false,
        FullMode = BoundedChannelFullMode.Wait
    });

    /// <inheritdoc />
    public int PendingCount => channel.Reader.CanCount ? channel.Reader.Count : 0;

    /// <inheritdoc />
    public ValueTask WriteAsync(TMessage message, CancellationToken cancellationToken)
    {
        if (channel.Writer.TryWrite(message))
        {
            return ValueTask.CompletedTask;
        }

        return channel.Writer.WriteAsync(message, cancellationToken);
    }

    /// <inheritdoc />
    public IAsyncEnumerable<TMessage> ReadAllAsync(CancellationToken cancellationToken)
    {
        return channel.Reader.ReadAllAsync(cancellationToken);
    }
}