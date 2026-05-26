# Ama.Enterprise.P2p

`Ama.Enterprise.P2p` is a decentralized, extensible, and high-performance peer-to-peer networking foundation for .NET 10 applications. Built from the ground up for Native AOT compatibility, it provides the core building blocks for constructing isolated, multi-mesh network topologies within a single application process utilizing modern .NET Keyed Dependency Injection.

## Features

- **Multi-Mesh Architecture**: Run multiple independent P2P networks concurrently within the same application process using decoupled Keyed DI boundaries.
- **Native AOT Ready**: Designed without dynamic reflection or emit, ensuring full compatibility with Native AOT compilation workflows.
- **Pluggable Transports**: Built-in support for TCP streams and UDP datagrams.
- **Two-Phase Discovery**: Multi-protocol peer discovery supporting UDP Multicast and DNS A/SRV record polling.
- **Advanced Routing Protocols**: Orchestrates standard Epidemic Gossip and Push-Pull Anti-Entropy Gossip routing topologies.
- **Automatic Failure Detection**: Configurable time-based heartbeats evaluating node lifecycles and network partition tolerance.

---

## Getting Started

The library exposes a fluent DI builder pattern via `IServiceCollection`.

### 1. Initialize the Mesh Identity

Every mesh requires a unique string identifier and a globally unique local `PeerId`. This identifier acts as the DI Key for all internal services.

```csharp
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.DependencyInjection;

var services = new ServiceCollection();

// Register the core mesh defining its isolated Keyed boundary
services.AddP2pMesh("InternalCluster", options =>
{
    options.LocalPeerId = Guid.NewGuid();
});
```

### 2. Configure Peer Transports

A mesh requires a transport protocol for communication. You must choose either TCP or UDP for a given mesh configuration.

```csharp
services.AddP2pMesh("InternalCluster")
    .AddTcpTransport(options =>
    {
        options.ListenHost = "127.0.0.1";
        options.ListenPort = 8080;
    });
```

### 3. Setup Discovery and Handshaking

Nodes orchestrate a two-phase discovery. 
* **Phase 1 (Discovery)** defines how peers are located (e.g., UDP Multicast or DNS A/SRV records). 
* **Phase 2 (Handshake)** dictates how initial protocol handshakes establish verified peer connections.

**UDP Multicast Example:**
```csharp
services.AddP2pMesh("InternalCluster")
    // Phase 1: UDP Multicast Polling
    .AddUdpPeerDiscovery(options =>
    {
        options.MulticastAddress = "239.255.0.1";
        options.MulticastPort = 50000;
        options.DiscoveryInterval = TimeSpan.FromSeconds(30);
        options.DiscoveryTimeout = TimeSpan.FromSeconds(5);
    })
    // Phase 2: UDP Unicast Handshake
    .AddUdpPeerHandshake(options =>
    {
        options.ListenPort = 50001;
        options.HandshakeTimeout = TimeSpan.FromSeconds(5);
    });
```

**DNS Discovery Example:**
```csharp
services.AddP2pMesh("InternalCluster")
    // Phase 1: DNS Resolution (Polled via A or SRV Records)
    .AddDnsPeerDiscovery(options =>
    {
        options.Hostname = "p2p-headless.default.svc.cluster.local";
        options.TargetPort = 8081; // Used for A records
        options.UseSrvRecords = true; // Enables SRV target port resolution
        options.DiscoveryInterval = TimeSpan.FromSeconds(30);
    })
    // Phase 2: Handshake
    .AddUdpPeerHandshake(options =>
    {
        options.ListenPort = 50001;
    });
```

### 4. Select the Gossip Algorithm

Decide how data is distributed across the bounded nodes.

```csharp
services.AddP2pMesh("InternalCluster")
    // Use Epidemic Gossip for high throughput broadcast
    .AddGossipNetwork(options =>
    {
        options.GossipInterval = TimeSpan.FromMilliseconds(500); 
        options.Fanout = 3;
        options.DefaultTimeToLive = 10;
    });

    // OR Use Anti-Entropy for synchronization guarantees:
    // .AddPushPullGossipNetwork(options => 
    // { 
    //     options.EnablePushPull = true;
    //     options.PushPullInterval = TimeSpan.FromSeconds(5);
    //     options.MaxDigestSize = 100;
    // });
```

### 5. Tune the Failure Detector

The detector handles eviction loops by identifying unresponsive nodes.

```csharp
services.AddP2pMesh("InternalCluster")
    .ConfigureFailureDetector(options =>
    {
        options.HeartbeatInterval = TimeSpan.FromSeconds(5);
        options.SuspectThresholdMultiplier = 3; // Evaluated as Suspect after 15s
        options.DeadThresholdMultiplier = 6;    // Evicted as Dead after 30s
    });
```

---

## Interacting with the Mesh

### Handling Inbound Payloads

Domain logic should implement `IApplicationPayloadHandler`. This handler must be registered as a `.NET Keyed Service` bounded to your target `meshId`.

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Models.Core;

public sealed class TelemetryPayloadHandler : IApplicationPayloadHandler
{
    public Task HandlePayloadAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        Console.WriteLine($"Received {payload.Length} bytes from {senderId.Value} on {meshId}");
        return Task.CompletedTask;
    }
}

// Registration
services.AddKeyedSingleton<IApplicationPayloadHandler>("InternalCluster", (sp, key) => new TelemetryPayloadHandler());
```

### Outbound Direct Messaging

For directed, non-broadcast payloads (like direct synchronization or targeted replies), use `IDirectMessageSender`.

```csharp
using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Models.Core;

public sealed class MyService
{
    private readonly IDirectMessageSender _directMessageSender;

    public MyService(IDirectMessageSender directMessageSender)
    {
        _directMessageSender = directMessageSender ?? throw new ArgumentNullException(nameof(directMessageSender));
    }

    public async Task SendSyncDataAsync(PeerId targetNodeId, ReadOnlyMemory<byte> payload, CancellationToken ct)
    {
        await _directMessageSender.SendDirectAsync("InternalCluster", targetNodeId, payload, ct).ConfigureAwait(false);
    }
}
```

### Background Execution

The network architecture runs behind generic host abstractions. Ensure your application starts hosted services using `IHostBuilder.Build().RunAsync()` or `WebApplication.RunAsync()`. The `P2pHostedService` coordinates initialization, discovery, and algorithm loops automatically in the background.