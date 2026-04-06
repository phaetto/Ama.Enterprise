namespace Ama.Enterprise.P2p.IntegrationTests.Models;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.IntegrationTests.Handlers;
using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

/// <summary>
/// Represents a fully configured test node mapping its internal services.
/// </summary>
public sealed class TestNode : IAsyncDisposable
{
    /// <summary>
    /// Gets the underlying service provider.
    /// </summary>
    public ServiceProvider Provider { get; }

    /// <summary>
    /// Gets the unique peer identity.
    /// </summary>
    public PeerId Id { get; }

    /// <summary>
    /// Gets the local binding endpoint.
    /// </summary>
    public PeerEndpoint Endpoint { get; }

    /// <summary>
    /// Gets the mock test handler tracking received messages.
    /// </summary>
    public TestMessageHandler Handler { get; }

    /// <summary>
    /// Gets the generic host wrapper driving the protocol loops.
    /// </summary>
    public IHostedService HostedService { get; }

    /// <summary>
    /// Gets the protocol instance.
    /// </summary>
    public IGossipProtocol Protocol { get; }

    /// <summary>
    /// Gets the routing registry instance.
    /// </summary>
    public IPeerRegistry Registry { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="TestNode"/> class.
    /// </summary>
    public TestNode(
        ServiceProvider provider,
        PeerId id,
        PeerEndpoint endpoint,
        TestMessageHandler handler,
        IHostedService hostedService,
        IGossipProtocol protocol,
        IPeerRegistry registry)
    {
        this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
        this.Id = id;
        this.Endpoint = endpoint;
        this.Handler = handler ?? throw new ArgumentNullException(nameof(handler));
        this.HostedService = hostedService ?? throw new ArgumentNullException(nameof(hostedService));
        this.Protocol = protocol ?? throw new ArgumentNullException(nameof(protocol));
        this.Registry = registry ?? throw new ArgumentNullException(nameof(registry));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (this.HostedService is not null)
        {
            try
            {
                await this.HostedService.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Suppress disposal network exceptions to avoid masking test failures
            }
        }

        if (this.Provider is not null)
        {
            await this.Provider.DisposeAsync().ConfigureAwait(false);
        }
    }
}