namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Composite transport router that delegates sending messages to the correct specific transport implementation explicitly.
/// </summary>
/// <typeparam name="TMessage">The type of the message being transported.</typeparam>
public sealed class TransportRouter<TMessage> : ITransportRouter<TMessage> where TMessage : IMeshMessage
{
    private readonly IEnumerable<ITransport<TMessage>> transports;

    /// <summary>
    /// Initializes a new instance of the <see cref="TransportRouter{TMessage}"/> class.
    /// </summary>
    /// <param name="transports">The collection of available specialized transports.</param>
    public TransportRouter(IEnumerable<ITransport<TMessage>> transports)
    {
        this.transports = transports ?? throw new ArgumentNullException(nameof(transports));
    }

    /// <inheritdoc />
    public bool CanHandle(PeerEndpoint endpoint) => transports.Any(t => t.CanHandle(endpoint));

    /// <inheritdoc />
    public Task SendAsync(PeerEndpoint endpoint, TMessage message, CancellationToken cancellationToken)
    {
        var transport = transports.FirstOrDefault(t => t.CanHandle(endpoint));
        if (transport is null)
        {
            throw new NotSupportedException($"No outbound explicitly mapped generic transport found that can handle endpoint type {endpoint.GetType().Name}.");
        }

        return transport.SendAsync(endpoint, message, cancellationToken);
    }
}