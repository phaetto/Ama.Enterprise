namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;

/// <summary>
/// A default implementation of <see cref="IPeerAuthenticator"/> that accepts all peer connections.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="PassThroughPeerAuthenticator"/> class.
/// </remarks>
public sealed class PassThroughPeerAuthenticator : IPeerAuthenticator, IDisposable
{
    private readonly string meshId;
    private readonly ILogger<PassThroughPeerAuthenticator> logger;

    private readonly Meter meter;
    private readonly Counter<long> authenticationsCounter;

    public PassThroughPeerAuthenticator(string meshId, ILogger<PassThroughPeerAuthenticator> logger, IMeterFactory? meterFactory = null)
    {
        this.meshId = meshId ?? throw new ArgumentNullException(nameof(meshId));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.PassThroughPeerAuthenticator") ?? new Meter("Ama.Enterprise.P2p.PassThroughPeerAuthenticator");
        this.authenticationsCounter = this.meter.CreateCounter<long>("p2p.authenticator.passthrough.authentications", "authentications", "Total authentications via pass-through strategy");
    }

    /// <inheritdoc />
    public Task<bool> AuthenticateAsync(PeerNode node, ReadOnlyMemory<byte> handshakeData, CancellationToken cancellationToken)
    {
        if (node.Id.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(node));
        }

        authenticationsCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));

        logger.LogDebug("[{MeshId}] Auto-authenticating peer {PeerId} via pass-through strategy.", meshId, node.Id.Value);

        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}