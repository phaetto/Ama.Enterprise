namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Composite transport router.
/// </summary>
public sealed class TransportRouter : ITransportRouter
{
    private readonly IEnumerable<ITransport> transports;

    /// <summary>
    /// Initializes a new instance of the <see cref="TransportRouter"/> class.
    /// </summary>
    /// <param name="transports">The collection of available transports.</param>
    public TransportRouter(IEnumerable<ITransport> transports)
    {
        this.transports = transports ?? throw new ArgumentNullException(nameof(transports));
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint) => transports.Any(t => t.CanHandle(endpoint));

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        var transport = transports.FirstOrDefault(t => t.CanHandle(endpoint));
        if (transport is null)
        {
            throw new NotSupportedException($"No outbound transport found for endpoint type {endpoint.GetType().Name}.");
        }

        return transport.SendAsync(endpoint, message, cancellationToken);
    }
}