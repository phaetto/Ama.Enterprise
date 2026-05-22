# Ama.Enterprise

A .NET 10 enterprise-grade toolkit for building decentralized, masterless Peer-to-Peer (P2P) distributed systems. It combines robust networking meshes with Conflict-free Replicated Data Types (CRDTs) to guarantee high availability, fault tolerance, and eventual consistency across nodes without requiring centralized databases or brokers.

## Features

- **Native AOT Ready**: Designed from the ground up for Native AOT. Zero runtime reflection, utilizing strict source-generated `System.Text.Json` contexts and MessagePack polymorphic resolvers for high-performance serialization.
- **Masterless P2P Mesh Networking**: Form decentralized clusters dynamically. Nodes automatically discover peers, negotiate connections, and replicate states via highly optimized Gossip and Anti-Entropy protocols.
- **Transport Agnostic**: Out-of-the-box support for multiple network transports and topologies:
  - **TCP & UDP**: Low-level stream and datagram routing.
  - **ASP.NET Core (Kestrel)**: Bind P2P meshes directly into existing web host pipelines natively.
  - **WebRTC**: Out-of-band signaling and data channels for direct browser-to-server or NAT-traversing node communication.
  - **MQTT**: Decoupled multi-mesh architectures using standard IoT brokers for discovery and transport.
- **Pluggable Peer Discovery**: Locate peers dynamically using UDP Multicast, DNS SRV records, or active HTTP polling depending on your infrastructure constraints.
- **Distributed CRDT Orchestrator**: Manage the lifecycles of hundreds of distributed CRDT documents dynamically. Create, sync, and tombstone documents across the mesh automatically with built-in snapshotting and journal truncation.
- **Storage Backends**: Persist distributed states safely using ephemeral Memory, relational Native AOT SQLite, or massively scalable Azure Table Storage backends.
- **Built-in Telemetry**: Natively integrated with `System.Diagnostics.Metrics`. It includes a P2P metric aggregator that pushes time-series hardware and mesh statistics across isolated nodes dynamically.

## Project Structure & Architecture

To understand the repository in 60 seconds, the architecture is divided into three primary layers:

### 1. The Network Layer (`Ama.Enterprise.P2p.*`)
The foundation of the system. It handles decentralized node discovery, failure detection, transport routing, and payload delivery (Gossip/Anti-Entropy).
- `Ama.Enterprise.P2p`: Core interfaces, base generic algorithms, and UDP/TCP/DNS implementations.
- `Ama.Enterprise.P2p.AspNetCore`: HTTP/Kestrel integration.
- `Ama.Enterprise.P2p.Mqtt`: MQTT integration.
- `Ama.Enterprise.P2p.WebRTC`: WebRTC data channels and signaling.
- `Ama.Enterprise.P2p.Telemetry`: P2P cluster metrics aggregation.

### 2. The Distributed Orchestration Layer (`Ama.Enterprise.CRDT.Distributed.*`)
Built on top of the P2P layer and `Ama.CRDT`. It manages the global registry of instantiated CRDT documents across the mesh, evaluating version vectors, syncing states, and abstracting data storage.
- `Ama.Enterprise.CRDT.Distributed`: Core orchestrator, background sync workers, and scope managers.
- `Ama.Enterprise.CRDT.Distributed.TableStorage`: Azure Table backend.
- `Ama.Enterprise.CRDT.MessagePack`: Binary serialization formatters.

### 3. Application Domain Layer (`Ama.Enterprise.FeatureFlags`)
High-level features utilizing the CRDT Orchestrator to provide instant business value.
- `Ama.Enterprise.FeatureFlags`: A decentralized, masterless feature flag system.

## Quick Start

### 1. Setup AOT Contexts & DI

Define your CRDT models and hook up the network and distributed orchestration services in your standard DI container.

```csharp
using Microsoft.Extensions.DependencyInjection;
using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.P2p.Extensions;

var services = new ServiceCollection();

// 1. Add Distributed CRDT Core Services
services.AddDistributedCrdtCore(options =>
{
    options.ActiveSyncEnabled = true;
    options.CheckpointIntervalSeconds = 30;
    options.AntiEntropyIntervalSeconds = 5;
});

// 2. Register your AOT JSON Contexts and Types
services.AddCrdt()
        .AddCrdtJsonTypeInfoResolver(MyJsonContext.Default)
        .AddCrdtAotContext(new MyCrdtAotContext())
        .AddCrdtSerializableType<TaskItem>("task-item")
        .AddCrdtSystemTextJson();

// 3. Register generic CRDT documents into the Orchestrator
services.AddDistributedDocumentType<TaskListState>("task-list");

// 4. Bind the Orchestrator to a specific P2P Mesh architecture
services.AddDistributedCrdtP2p("internal", replicaId: "node-1");

// 5. Configure the Network Mesh (Transports & Discovery)
services.AddP2pMesh("internal")
        .AddGossipNetwork()
        .AddTcpTransport(options => { options.ListenPort = 8100; })
        .AddUdpPeerDiscovery(options => 
        {
            options.MulticastAddress = "239.255.0.2";
            options.MulticastPort = 8036;
        })
        .AddUdpPeerHandshake(options => { options.ListenPort = 8037; });
```

### 2. Interact with Distributed Documents

Once booted, retrieve your documents from the orchestrator. Any mutations are automatically broadcast to all connected peers in the mesh.

```csharp
var scopeManager = provider.GetRequiredService<DistributedCrdtScopeManager>();
var scope = scopeManager.GetOrCreateScope("node-1");

var orchestrator = scope.ServiceProvider.GetRequiredService<ICrdtDocumentOrchestrator>();
var taskManager = scope.ServiceProvider.GetRequiredService<ITaskManager>();

// Create a distributed document locally (it will replicate to the mesh)
await orchestrator.CreateDocumentAsync("dev-team-list", "task-list", cancellationToken);

// Mutate the state. Changes are natively synced!
await taskManager.SetTaskAsync("dev-team-list", "task-1", "Review PR", isDone: false, cancellationToken);
```

## Showcases

This repository includes highly interactive console applications demonstrating the framework across dynamically spawned nodes:

- [**Distributed CRDT Showcase**](Ama.Enterprise.CRDT.Distributed.ShowCase/README.md) (`Ama.Enterprise.CRDT.Distributed.ShowCase`): Demonstrates the `ICrdtDocumentOrchestrator` managing multiple generic CRDT collections (Task Lists and IoT Fleet Statuses) simultaneously. Features local SQLite storage, Native MessagePack binary serialization, and a `hammer` load-testing mode that pumps thousands of concurrent operations into the mesh.
- [**Feature Flags Showcase**](Ama.Enterprise.FeatureFlags.ShowCase/README.md) (`Ama.Enterprise.FeatureFlags.ShowCase`): A masterless P2P Feature Flag management console. Demonstrates extracting high-level applications backed by CRDT synchronization and dynamic UDP cluster discovery.

*Tip: While running either showcase, type `clone` into the console. This will automatically spawn a brand-new node process on a new port that instantly discovers and syncs with your primary node.*

## Building and Testing

To build the project:

```bash
dotnet build
```

To run the unit and integration tests (which spin up isolated P2P meshes in memory and on local loopbacks):

```bash
dotnet test
```

## AI Coding Assistance

To maintain full transparency, please note that AI coding assistants and Large Language Models (LLMs) were actively used in the design, development, testing, and documentation of this repository. While AI tools significantly accelerated the generation of code and ideas, all output was rigorously reviewed, steered, tested, and refined by human developers (me). I believe in leveraging these tools to enhance productivity while taking complete responsibility for the library's architecture, security, and mathematical correctness.

## License

While the library is under the first release (< 1.0.0) the code is licensed under [GPL-3.0](./GPL-3.0-LICENSE).

Future releases will (_only a plan for now_) be dual-licensed depending on the company's revenue, or MIT for all others. Custom licenses can be provided if you contact the author.

### Honor-Based Licensing System

To balance open-source accessibility with sustainable enterprise development, this toolkit employs a non-intrusive, honor-based licensing model. I believe in trusting developers. **_There is no draconian DRM, no obfuscation, and no hidden "phone home" analytics_**.

The built-in `HonorLicenseManager` uses standard RSA cryptographic signatures to validate commercial or custom enterprise keys locally. When you purchase or receive a custom license, you are provided with a signed key that you inject during your application's bootstrap phase.

#### How to configure your License Key

If you operate under a custom or enterprise license, you need to configure your unique license key during the dependency injection phase. This ensures that your mesh operates in compliance with your commercial agreement without generating missing license warnings in your logs.

```csharp
services.AddP2pMesh("internal")
        .ConfigureLicense(options =>
        {
            options.Licensee = "Your Company Name";
            options.LicenseKey = "RSA-SIGNED-BASE64-KEY-STRING";
        })
        // ... add transports and discovery mechanisms
```

*If you require a commercial license or have questions about dual-licensing thresholds, please contact the author.*