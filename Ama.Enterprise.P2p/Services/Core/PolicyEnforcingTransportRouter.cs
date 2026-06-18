namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Decorator wrapping transport dispatchers natively evaluating outbound structural policies prior to network broadcasts preventing data leaks implicitly.
/// </summary>
public sealed class PolicyEnforcingTransportRouter : ITransportRouter
{
    private readonly string meshId;
    private readonly ITransportRouter innerRouter;
    private readonly IEnumerable<IMeshRoutingPolicy> policies;
    private readonly IPeerRegistry peerRegistry;
    private readonly IPeerSessionRegistry sessionRegistry;
    private readonly IOptionsMonitor<SessionAuthenticatorOptions> optionsMonitor;
    private readonly ILogger<PolicyEnforcingTransportRouter> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="PolicyEnforcingTransportRouter"/> class.
    /// </summary>
    public PolicyEnforcingTransportRouter(
        string meshId,
        ITransportRouter innerRouter,
        IEnumerable<IMeshRoutingPolicy> policies,
        IPeerRegistry peerRegistry,
        IPeerSessionRegistry sessionRegistry,
        IOptionsMonitor<SessionAuthenticatorOptions> optionsMonitor,
        ILogger<PolicyEnforcingTransportRouter> logger)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(innerRouter);
        ArgumentNullException.ThrowIfNull(policies);
        ArgumentNullException.ThrowIfNull(peerRegistry);
        ArgumentNullException.ThrowIfNull(sessionRegistry);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.innerRouter = innerRouter;
        this.policies = policies;
        this.peerRegistry = peerRegistry;
        this.sessionRegistry = sessionRegistry;
        this.optionsMonitor = optionsMonitor;
        this.logger = logger;
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint)
    {
        return innerRouter.CanHandle(endpoint);
    }

    /// <inheritdoc />
    public async Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        var options = optionsMonitor.Get(meshId);
        var activePeers = await peerRegistry.GetAllPeersAsync(meshId, cancellationToken).ConfigureAwait(false);
        var targetPeer = activePeers.FirstOrDefault(p => p.Endpoint.Equals(endpoint));

        if (targetPeer.Id.Value == Guid.Empty)
        {
            if (options.RequireSession)
            {
                logger.LogWarning("[{MeshId}] Blocked transmission mapping explicit unregistered endpoint limits routing policy.", meshId);
                return;
            }
        }
        else
        {
            var session = await sessionRegistry.GetSessionAsync(meshId, targetPeer.Id, cancellationToken).ConfigureAwait(false);
            if (session.HasValue)
            {
                foreach (var policy in policies)
                {
                    var isAllowed = await policy.EvaluateOutboundAsync(meshId, session.Value, message, cancellationToken).ConfigureAwait(false);
                    if (!isAllowed)
                    {
                        logger.LogDebug("[{MeshId}] Policy bounding limits explicitly dropped outbound payload {MessageId} mapped {PeerId}.", meshId, message.MessageId, targetPeer.Id.Value);
                        return;
                    }
                }
            }
            else if (options.RequireSession)
            {
                logger.LogWarning("[{MeshId}] Enforced dropped constraints routing untracked missing sessions implicitly node {PeerId}.", meshId, targetPeer.Id.Value);
                return;
            }
        }

        await innerRouter.SendAsync(endpoint, message, cancellationToken).ConfigureAwait(false);
    }
}