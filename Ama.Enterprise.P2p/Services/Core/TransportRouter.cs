namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Composite transport router.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="TransportRouter"/> class.
/// </remarks>
/// <param name="transports">The collection of available transports.</param>
public sealed class TransportRouter : ITransportRouter, IDisposable
{
    private readonly IEnumerable<ITransport> transports;
    
    private readonly Meter meter;
    private readonly Counter<long> messagesRoutedCounter;

    public TransportRouter(IEnumerable<ITransport> transports, IMeterFactory? meterFactory = null)
    {
        this.transports = transports ?? throw new ArgumentNullException(nameof(transports));

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.TransportRouter") ?? new Meter("Ama.Enterprise.P2p.TransportRouter");
        this.messagesRoutedCounter = this.meter.CreateCounter<long>("p2p.router.messages_routed", "messages", "Total messages routed through the transport router");
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint) => transports.Any(t => t.CanHandle(endpoint));

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, IMeshMessage message, CancellationToken cancellationToken)
    {
        messagesRoutedCounter.Add(1, new KeyValuePair<string, object?>("endpoint_type", endpoint.GetType().Name));

        var transport = transports.FirstOrDefault(t => t.CanHandle(endpoint));
        if (transport is null)
        {
            throw new NotSupportedException($"No outbound transport found for endpoint type {endpoint.GetType().Name}.");
        }

        return transport.SendAsync(endpoint, message, cancellationToken);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}