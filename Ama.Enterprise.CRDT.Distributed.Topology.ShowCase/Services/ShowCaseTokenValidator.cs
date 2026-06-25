namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// Provides explicit token validation executing session bounds natively tracking defined RBAC and tags.
/// </summary>
public sealed class ShowCaseTokenValidator : ISessionTokenValidator
{
    private readonly ShowCaseNodeContext context;

    public ShowCaseTokenValidator(ShowCaseNodeContext context)
    {
        this.context = context;
    }

    /// <inheritdoc />
    public Task<ReadOnlyMemory<byte>> GetLocalTokenBytesAsync(string meshId, CancellationToken cancellationToken)
    {
        var tokenString = $"{context.Role}|{context.Region}";
        return Task.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes(tokenString));
    }

    /// <inheritdoc />
    public Task<SessionContext?> ValidateTokenAsync(string meshId, ReadOnlyMemory<byte> tokenData, CancellationToken cancellationToken)
    {
        var tokenString = Encoding.UTF8.GetString(tokenData.Span);
        var parts = tokenString.Split('|');
        if (parts.Length != 2)
        {
            return Task.FromResult<SessionContext?>(null);
        }

        var claims = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            { "role", parts[0] },
            { "region", parts[1] }
        };

        var session = new SessionContext(
            Guid.NewGuid().ToString(),
            claims,
            DateTimeOffset.UtcNow.AddHours(24)
        );

        return Task.FromResult<SessionContext?>(session);
    }
}