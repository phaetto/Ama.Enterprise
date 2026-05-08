namespace Ama.Enterprise.P2p.Telemetry.IntegrationTests.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Transports;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Telemetry.Extensions;
using Ama.Enterprise.P2p.Telemetry.Models;
using Ama.Enterprise.P2p.Telemetry.Services;
using Ama.Enterprise.UnitTests.Attributes;
using Ama.Enterprise.UnitTests.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shouldly;
using Xunit;

/// <summary>
/// Contains integration tests verifying the decoupled Telemetry Push Protocol natively extracting, 
/// transmitting, and aggregating distributed metric payloads across active P2P mesh bounds.
/// </summary>
public sealed class TelemetryNetworkIntegrationTests(ITestOutputHelper testOutputHelper) : IDisposable
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    private readonly IList<ServiceProvider> serviceProviders = new List<ServiceProvider>();

    private static int httpPortCounter = 15000;
    private static int GetNextHttpPort() => Interlocked.Increment(ref httpPortCounter);

    [IntegrationFact]
    public async Task EndToEnd_TelemetryMetrics_ShouldBeForwarded_ToAggregatorNodeAsync()
    {
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var forwarderNodeId = Guid.NewGuid();
        var aggregatorNodeId = Guid.NewGuid();
        var portForwarder = GetNextHttpPort();
        var portAggregator = GetNextHttpPort();
        var meshId = "TelemetryTestMesh";
        var customMeters = new[] { "IntegrationTestMeter" };

        // Setup Forwarder Node (Collects and pushes metrics)
        var providerForwarder = CreateTelemetryNode(forwarderNodeId, portForwarder, meshId, isForwarder: true, isAggregator: false, flushIntervalMs: 500, customMeters);
        
        // Setup Aggregator Node (Receives and aggregates metrics)
        var providerAggregator = CreateTelemetryNode(aggregatorNodeId, portAggregator, meshId, isForwarder: false, isAggregator: true, flushIntervalMs: 500);

        var hostedServicesForwarder = providerForwarder.GetServices<IHostedService>().ToList();
        var hostedServicesAggregator = providerAggregator.GetServices<IHostedService>().ToList();

        await StartAllHostedServicesAsync(hostedServicesForwarder, cancellationSource.Token).ConfigureAwait(false);
        await StartAllHostedServicesAsync(hostedServicesAggregator, cancellationSource.Token).ConfigureAwait(false);

        // Manually start the TelemetryPushProtocol since it relies on explicit protocol hooks.
        var pushProtocolForwarder = providerForwarder.GetRequiredService<TelemetryPushProtocol>();
        await pushProtocolForwarder.StartAsync(cancellationSource.Token).ConfigureAwait(false);

        // Inject the Aggregator Node directly into the Forwarder Node's topology registry to establish a direct route.
        var registryForwarder = providerForwarder.GetRequiredService<IPeerRegistry>();
        var aggregatorEndpoint = new HttpPeerEndpoint("127.0.0.1", portAggregator);
        var peerAggregator = new PeerNode(new PeerId(aggregatorNodeId), aggregatorEndpoint);
        
        await registryForwarder.AddOrUpdatePeerAsync(meshId, peerAggregator, PeerStatus.Active, cancellationSource.Token).ConfigureAwait(false);

        // Allow background loops to stabilize and establish routing context.
        await Task.Delay(1000, cancellationSource.Token).ConfigureAwait(false);

        // Act - Create and record an explicit generic metric targeted for network evaluation using custom names.
        using var meter = new Meter("IntegrationTestMeter", "1.0");
        var counter = meter.CreateCounter<long>("integration_test_messages_sent");
        
        counter.Add(150, new KeyValuePair<string, object?>("custom_dimension", "beta_test"));

        // Evaluate - Poll the Aggregator Node until the explicit metric transmission evaluates successfully.
        var aggregatorService = providerAggregator.GetRequiredService<ITelemetryAggregator>();
        TelemetryPayloadDto? receivedPayload = null;

        for (int i = 0; i < 20; i++)
        {
            receivedPayload = aggregatorService.GetNodeMetrics(forwarderNodeId);
            
            if (receivedPayload is not null && receivedPayload.Metrics.Any(m => m.Name == "integration_test_messages_sent"))
            {
                break;
            }
            
            await Task.Delay(500, cancellationSource.Token).ConfigureAwait(false);
        }

        // Assert - Validate exact metric structures guaranteeing AOT decoupled DTO integrity constraints natively.
        receivedPayload.ShouldNotBeNull("Telemetry payload was not pushed correctly to the aggregator node.");
        receivedPayload!.NodeId.ShouldBe(forwarderNodeId);
        
        var targetMetric = receivedPayload.Metrics.FirstOrDefault(m => m.Name == "integration_test_messages_sent");
        targetMetric.ShouldNotBeNull();
        targetMetric.Value.ShouldBe(150);
        targetMetric.Type.ShouldBe("Counter");

        var targetTag = targetMetric.Tags.FirstOrDefault(t => string.Equals(t.Key, "custom_dimension", StringComparison.Ordinal));
        targetTag.Value.ShouldBe("beta_test");

        // Teardown
        await pushProtocolForwarder.StopAsync(cancellationSource.Token).ConfigureAwait(false);

        await StopAllHostedServicesAsync(hostedServicesForwarder, cancellationSource.Token).ConfigureAwait(false);
        await StopAllHostedServicesAsync(hostedServicesAggregator, cancellationSource.Token).ConfigureAwait(false);
    }

    [IntegrationFact]
    public async Task EndToEnd_CustomSystemAndAspNetCoreMetrics_ShouldBeForwarded_WhenConfiguredAsync()
    {
        using var cancellationSource = new CancellationTokenSource(TimeSpan.FromSeconds(30));

        var forwarderNodeId = Guid.NewGuid();
        var aggregatorNodeId = Guid.NewGuid();
        var portForwarder = GetNextHttpPort();
        var portAggregator = GetNextHttpPort();
        var meshId = "TelemetryCustomMesh";
        
        // Target built-in framework telemetry channels for evaluation
        var customMeters = new[] { "System.Runtime", "Microsoft.AspNetCore.Hosting" };

        var providerForwarder = CreateTelemetryNode(forwarderNodeId, portForwarder, meshId, isForwarder: true, isAggregator: false, flushIntervalMs: 500, customMeters);
        var providerAggregator = CreateTelemetryNode(aggregatorNodeId, portAggregator, meshId, isForwarder: false, isAggregator: true, flushIntervalMs: 500);

        var hostedServicesForwarder = providerForwarder.GetServices<IHostedService>().ToList();
        var hostedServicesAggregator = providerAggregator.GetServices<IHostedService>().ToList();

        await StartAllHostedServicesAsync(hostedServicesForwarder, cancellationSource.Token).ConfigureAwait(false);
        await StartAllHostedServicesAsync(hostedServicesAggregator, cancellationSource.Token).ConfigureAwait(false);

        var pushProtocolForwarder = providerForwarder.GetRequiredService<TelemetryPushProtocol>();
        await pushProtocolForwarder.StartAsync(cancellationSource.Token).ConfigureAwait(false);

        var registryForwarder = providerForwarder.GetRequiredService<IPeerRegistry>();
        var aggregatorEndpoint = new HttpPeerEndpoint("127.0.0.1", portAggregator);
        var peerAggregator = new PeerNode(new PeerId(aggregatorNodeId), aggregatorEndpoint);
        
        await registryForwarder.AddOrUpdatePeerAsync(meshId, peerAggregator, PeerStatus.Active, cancellationSource.Token).ConfigureAwait(false);

        await Task.Delay(1000, cancellationSource.Token).ConfigureAwait(false);

        // Act - Simulate the emission of explicit generic built-in memory and ASP.NET Core metrics.
        using var memoryMeter = new Meter("System.Runtime", "1.0");
        var allocCounter = memoryMeter.CreateCounter<long>("allocations.total");
        allocCounter.Add(1024000, new KeyValuePair<string, object?>("type", "gen0"));

        using var aspNetMeter = new Meter("Microsoft.AspNetCore.Hosting", "1.0");
        var requestCounter = aspNetMeter.CreateCounter<long>("requests.active");
        requestCounter.Add(42, new KeyValuePair<string, object?>("endpoint", "/api/data"));

        // Evaluate - Poll the Aggregator Node mapping the natively decoupled metrics correctly.
        var aggregatorService = providerAggregator.GetRequiredService<ITelemetryAggregator>();
        TelemetryPayloadDto? receivedPayload = null;

        for (int i = 0; i < 20; i++)
        {
            receivedPayload = aggregatorService.GetNodeMetrics(forwarderNodeId);
            
            if (receivedPayload is not null && 
                receivedPayload.Metrics.Any(m => m.Name == "allocations.total") &&
                receivedPayload.Metrics.Any(m => m.Name == "requests.active"))
            {
                break;
            }
            
            await Task.Delay(500, cancellationSource.Token).ConfigureAwait(false);
        }

        // Assert
        receivedPayload.ShouldNotBeNull("Telemetry payload was not pushed correctly to the aggregator node evaluating multiple targeted system meters.");
        receivedPayload!.NodeId.ShouldBe(forwarderNodeId);
        
        var memoryMetric = receivedPayload.Metrics.FirstOrDefault(m => m.Name == "allocations.total");
        memoryMetric.ShouldNotBeNull();
        memoryMetric.Value.ShouldBe(1024000);
        memoryMetric.Type.ShouldBe("Counter");
        
        var memoryTag = memoryMetric.Tags.FirstOrDefault(t => string.Equals(t.Key, "type", StringComparison.Ordinal));
        memoryTag.Value.ShouldBe("gen0");

        var aspNetMetric = receivedPayload.Metrics.FirstOrDefault(m => m.Name == "requests.active");
        aspNetMetric.ShouldNotBeNull();
        aspNetMetric.Value.ShouldBe(42);
        aspNetMetric.Type.ShouldBe("Counter");

        var aspTag = aspNetMetric.Tags.FirstOrDefault(t => string.Equals(t.Key, "endpoint", StringComparison.Ordinal));
        aspTag.Value.ShouldBe("/api/data");

        // Teardown
        await pushProtocolForwarder.StopAsync(cancellationSource.Token).ConfigureAwait(false);

        await StopAllHostedServicesAsync(hostedServicesForwarder, cancellationSource.Token).ConfigureAwait(false);
        await StopAllHostedServicesAsync(hostedServicesAggregator, cancellationSource.Token).ConfigureAwait(false);
    }

    [IntegrationFact]
    public async Task TelemetryAggregator_ShouldPruneStaleNodes_WhenEvaluatingAgeConstraints()
    {
        // Setup
        var aggregatorNodeId = Guid.NewGuid();
        var targetMeshId = "TelemetryPruningMesh";
        var provider = CreateTelemetryNode(aggregatorNodeId, GetNextHttpPort(), targetMeshId, isForwarder: false, isAggregator: true, flushIntervalMs: 500);

        var aggregator = provider.GetRequiredService<ITelemetryAggregator>();
        var remoteNodeId = Guid.NewGuid();

        // Act - Inject a payload with a timestamp explicitly set in the past.
        var stalePayload = new TelemetryPayloadDto
        {
            NodeId = remoteNodeId,
            Timestamp = DateTimeOffset.UtcNow.Subtract(TimeSpan.FromMinutes(10)),
            Metrics = new List<MetricSnapshotDto>
            {
                new() { Name = "test_stale_metric", Type = "Gauge", Value = 1 }
            }
        };

        aggregator.UpdateNodeTelemetry(stalePayload);
        
        var capturedBefore = aggregator.GetNodeMetrics(remoteNodeId);
        capturedBefore.ShouldNotBeNull();

        // Act - Execute pruning for everything older than 5 minutes.
        aggregator.PruneStaleNodes(TimeSpan.FromMinutes(5));

        // Assert - The stale payload must be wiped out safely.
        var capturedAfter = aggregator.GetNodeMetrics(remoteNodeId);
        capturedAfter.ShouldBeNull();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var provider in serviceProviders)
        {
            provider.Dispose();
        }
        
        serviceProviders.Clear();
    }

    private ServiceProvider CreateTelemetryNode(
        Guid peerId, 
        int listenPort, 
        string meshId, 
        bool isForwarder, 
        bool isAggregator, 
        int flushIntervalMs,
        string[]? customMeters = null)
    {
        var services = new ServiceCollection();

        services.AddCrdt();

        services.AddLogging(builder => 
        {
            builder.AddXunit(testOutputHelper);
            builder.SetMinimumLevel(LogLevel.Trace);
        });

        services.AddP2pMesh(meshId, nodeOptions => 
            {
                nodeOptions.LocalPeerId = peerId;
            })
            .AddHttpTransport(opts => 
            { 
                opts.ListenPort = listenPort; 
                opts.ListenHost = "127.0.0.1"; 
                opts.PathPrefix = $"/p2p/{meshId.ToLower()}/";
            });

        if (isForwarder)
        {
            services.AddP2pTelemetryForwarder(opts => 
            {
                opts.TargetMeshId = meshId;
                opts.FlushInterval = TimeSpan.FromMilliseconds(flushIntervalMs);
                opts.IsEnabled = true;

                if (customMeters is not null)
                {
                    foreach (var meter in customMeters)
                    {
                        opts.IncludedMeterNames.Add(meter);
                    }
                }
            });
        }

        if (isAggregator)
        {
            services.AddP2pTelemetryAggregator(meshId);
        }

        var provider = services.BuildServiceProvider();
        serviceProviders.Add(provider);

        return provider;
    }

    private static async Task StartAllHostedServicesAsync(IEnumerable<IHostedService> services, CancellationToken cancellationToken)
    {
        foreach (var service in services)
        {
            await service.StartAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task StopAllHostedServicesAsync(IEnumerable<IHostedService> services, CancellationToken cancellationToken)
    {
        foreach (var service in services.Reverse())
        {
            await service.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}