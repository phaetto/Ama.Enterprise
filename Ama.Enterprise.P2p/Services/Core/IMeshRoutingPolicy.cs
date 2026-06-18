namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines a routing policy that determines whether messages are allowed to be sent or received based on session data and network rules.
/// </summary>
/// <example>
/// <code>
/// public class AdminOnlyRoutingPolicy : IMeshRoutingPolicy
/// {
///     public Task&lt;bool&gt; EvaluateOutboundAsync(string meshId, SessionContext session, IMeshMessage message, CancellationToken cancellationToken)
///     {
///         // Only allow sending messages to peers with the Admin role
///         bool isAllowed = session.Claims.TryGetValue("Role", out string? role) &amp;&amp; role == "Admin";
///         return Task.FromResult(isAllowed);
///     }
/// 
///     public Task&lt;bool&gt; EvaluateInboundAsync(string meshId, SessionContext session, ReadOnlyMemory&lt;byte&gt; payload, CancellationToken cancellationToken)
///     {
///         // Allow all incoming messages from authenticated sessions
///         return Task.FromResult(true);
///     }
/// }
/// </code>
/// </example>
public interface IMeshRoutingPolicy
{
    /// <summary>
    /// Checks if an outgoing message is allowed to be sent to a specific peer session.
    /// </summary>
    /// <param name="meshId">The identifier of the specific P2P mesh.</param>
    /// <param name="session">The validated session context of the target peer.</param>
    /// <param name="message">The transport message to be transmitted.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>True if the message is allowed to be sent; otherwise, false to drop it.</returns>
    Task<bool> EvaluateOutboundAsync(string meshId, SessionContext session, IMeshMessage message, CancellationToken cancellationToken);

    /// <summary>
    /// Checks if an incoming message is allowed to be processed by the local node based on the sender's session.
    /// </summary>
    /// <param name="meshId">The identifier of the specific P2P mesh.</param>
    /// <param name="session">The validated session context of the remote peer.</param>
    /// <param name="payload">The raw application payload received.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>True if the message is allowed to be processed; otherwise, false to reject it.</returns>
    Task<bool> EvaluateInboundAsync(string meshId, SessionContext session, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken);
}