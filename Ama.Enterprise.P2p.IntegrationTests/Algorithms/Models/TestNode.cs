namespace Ama.Enterprise.P2p.IntegrationTests.Algorithms.Models;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.IntegrationTests.Algorithms.Handlers;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

/// <summary>
/// Represents a fully configured test node mapping its internal services.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="TestNode"/> class.
/// </remarks>
public sealed class TestNode(
    ServiceProvider provider,
    PeerId id,
    PeerEndpoint endpoint,
    TestMessageHandler handler,
    IHostedService hostedService,
    IP2pAlgorithm protocol,
    IPeerRegistry registry) : IAsyncDisposable
{
    /// <summary>
    /// Gets the underlying service provider.
    /// </summary>
    public ServiceProvider Provider { get; } = provider ?? throw new ArgumentNullException(nameof(provider));

    /// <summary>
    /// Gets the unique peer identity.
    /// </summary>
    public PeerId Id { get; } = id;

    /// <summary>
    /// Gets the local binding endpoint.
    /// </summary>
    public PeerEndpoint Endpoint { get; } = endpoint;

    /// <summary>
    /// Gets the mock test handler tracking received messages.
    /// </summary>
    public TestMessageHandler Handler { get; } = handler ?? throw new ArgumentNullException(nameof(handler));

    /// <summary>
    /// Gets the generic host wrapper driving the protocol loops.
    /// </summary>
    public IHostedService HostedService { get; } = hostedService ?? throw new ArgumentNullException(nameof(hostedService));

    /// <summary>
    /// Gets the generic protocol instance.
    /// </summary>
    public IP2pAlgorithm Protocol { get; } = protocol ?? throw new ArgumentNullException(nameof(protocol));

    /// <summary>
    /// Gets the routing registry instance.
    /// </summary>
    public IPeerRegistry Registry { get; } = registry ?? throw new ArgumentNullException(nameof(registry));

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (HostedService is not null)
        {
            try
            {
                await HostedService.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // Suppress disposal network exceptions to avoid masking test failures
            }
        }

        if (Provider is not null)
        {
            await Provider.DisposeAsync().ConfigureAwait(false);
        }
    }
}