namespace Ama.Enterprise.P2p.Services.Core;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines an internal queue for decoupling inbound network listeners from the protocol logic.
/// </summary>
/// <typeparam name="TMessage">The type of the message being queued.</typeparam>
/// <example>
/// <code>
/// public async Task CheckBacklogAsync(IInboundMessageQueue&lt;IMeshMessage&gt; queue, CancellationToken ct)
/// {
///     if (queue.PendingCount &gt; 100)
///     {
///         await foreach (var msg in queue.ReadAllAsync(ct))
///         {
///             // Process message
///         }
///     }
/// }
/// </code>
/// </example>
public interface IInboundMessageQueue<TMessage> where TMessage : IMeshMessage
{
    /// <summary>
    /// Gets the current number of messages waiting to be read from the internal queue.
    /// </summary>
    int PendingCount => 0;

    /// <summary>
    /// Writes a message to the internal queue.
    /// </summary>
    /// <param name="message">The message to write.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A value task representing the asynchronous write operation.</returns>
    ValueTask WriteAsync(TMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// Reads all messages from the internal queue as an asynchronous enumerable.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An asynchronous enumerable of messages.</returns>
    IAsyncEnumerable<TMessage> ReadAllAsync(CancellationToken cancellationToken);
}