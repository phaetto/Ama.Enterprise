namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Decorator validating distinct inbound multi-mesh payload logic applying explicit policy checks safely prior to structural executions.
/// </summary>
public sealed class PolicyEnforcingPayloadDispatcher : IApplicationPayloadDispatcher
{
    private readonly string meshId;
    private readonly IApplicationPayloadDispatcher innerDispatcher;
    private readonly IEnumerable<IMeshRoutingPolicy> policies;
    private readonly IPeerSessionRegistry sessionRegistry;
    private readonly IOptionsMonitor<SessionAuthenticatorOptions> optionsMonitor;
    private readonly ILogger<PolicyEnforcingPayloadDispatcher> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="PolicyEnforcingPayloadDispatcher"/> class.
    /// </summary>
    public PolicyEnforcingPayloadDispatcher(
        string meshId,
        IApplicationPayloadDispatcher innerDispatcher,
        IEnumerable<IMeshRoutingPolicy> policies,
        IPeerSessionRegistry sessionRegistry,
        IOptionsMonitor<SessionAuthenticatorOptions> optionsMonitor,
        ILogger<PolicyEnforcingPayloadDispatcher> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(innerDispatcher);
        ArgumentNullException.ThrowIfNull(policies);
        ArgumentNullException.ThrowIfNull(sessionRegistry);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.innerDispatcher = innerDispatcher;
        this.policies = policies;
        this.sessionRegistry = sessionRegistry;
        this.optionsMonitor = optionsMonitor;
        this.logger = logger;
    }

    /// <inheritdoc />
    public async Task DispatchAsync(string mappedMeshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        if (!string.Equals(mappedMeshId, meshId, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Dispatcher tracking targeted mismatches. Evaluated '{mappedMeshId}' explicitly bound to '{meshId}'.");
        }

        var options = optionsMonitor.Get(meshId);
        var session = await sessionRegistry.GetSessionAsync(meshId, senderId, cancellationToken).ConfigureAwait(false);

        if (session.HasValue)
        {
            foreach (var policy in policies)
            {
                var isAllowed = await policy.EvaluateInboundAsync(meshId, session.Value, payload, cancellationToken).ConfigureAwait(false);
                if (!isAllowed)
                {
                    logger.LogWarning("[{MeshId}] Discarded inbound boundaries gracefully limiting {PeerId} explicitly via policy constraints.", meshId, senderId.Value);
                    return;
                }
            }
        }
        else if (options.RequireSession)
        {
            logger.LogWarning("[{MeshId}] Inbound allocations bypassing generic evaluations dropped structurally tracking blank sessions {PeerId}.", meshId, senderId.Value);
            return;
        }

        await innerDispatcher.DispatchAsync(meshId, senderId, payload, cancellationToken).ConfigureAwait(false);
    }
}