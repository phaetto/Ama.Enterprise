namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Defines a contract for validating authorization tokens, such as JSON Web Tokens (JWTs), to establish authenticated sessions between peers.
/// </summary>
/// <example>
/// <code>
/// public class CustomJwtValidator : ISessionTokenValidator
/// {
///     public Task&lt;ReadOnlyMemory&lt;byte&gt;&gt; GetLocalTokenBytesAsync(string meshId, CancellationToken cancellationToken)
///     {
///         byte[] token = GenerateJwtForNode();
///         return Task.FromResult&lt;ReadOnlyMemory&lt;byte&gt;&gt;(token);
///     }
///     
///     public Task&lt;SessionContext?&gt; ValidateTokenAsync(string meshId, ReadOnlyMemory&lt;byte&gt; tokenData, CancellationToken cancellationToken)
///     {
///         SessionContext? context = ParseAndValidateJwt(tokenData);
///         return Task.FromResult(context);
///     }
/// }
/// </code>
/// </example>
public interface ISessionTokenValidator
{
    /// <summary>
    /// Gets the raw byte array of the local authorization token to send to other peers during a network handshake.
    /// </summary>
    /// <param name="meshId">The identifier of the specific P2P mesh.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A raw sequence of bytes representing the token to broadcast.</returns>
    Task<ReadOnlyMemory<byte>> GetLocalTokenBytesAsync(string meshId, CancellationToken cancellationToken);

    /// <summary>
    /// Reads and validates a token received from a remote peer during a network handshake.
    /// </summary>
    /// <param name="meshId">The identifier of the specific P2P mesh.</param>
    /// <param name="tokenData">The raw encoded token received from the remote peer.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A validated session context if the token is authorized, or null if the token is invalid.</returns>
    Task<SessionContext?> ValidateTokenAsync(string meshId, ReadOnlyMemory<byte> tokenData, CancellationToken cancellationToken);
}