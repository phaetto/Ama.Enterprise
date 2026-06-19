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
- **Pluggable Peer Discovery**: Locate peers dynamically using UDP Multicast, DNS A/SRV records, or active HTTP polling depending on your infrastructure constraints.
- **Distributed CRDT Orchestrator**: Manage the lifecycles of hundreds of distributed CRDT documents dynamically. Create, sync, and tombstone documents across the mesh automatically with built-in snapshotting and journal truncation.
- **Storage Backends**: Persist distributed states safely using ephemeral Memory, scalable Azure Table Storage or easily connect your own backend (`Ama.Enterprise.CRDT.Distributed.ShowCase` has it own implementation of AOT SQLite).
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
using Ama.Enterprise.Licensing.Extensions;
using Ama.Enterprise.Licensing.Models;

var services = new ServiceCollection();

// 1. Declare Community (Source-Available) or Enterprise license type
services.ConfigureAmaCommunityLicense(); 

// 2. Add Distributed CRDT Core Services
services.AddDistributedCrdtCore(options =>
{
    options.ActiveSyncEnabled = true;
    options.CheckpointIntervalSeconds = 30;
    options.AntiEntropyIntervalSeconds = 5;
});

// 3. Register your AOT JSON Contexts and Types
services.AddCrdt()
        .AddCrdtJsonTypeInfoResolver(MyJsonContext.Default)
        .AddCrdtAotContext(new MyCrdtAotContext())
        .AddCrdtSerializableType<TaskItem>("task-item")
        .AddCrdtSystemTextJson();

// 4. Register generic CRDT documents into the Orchestrator
services.AddDistributedDocumentType<TaskListState>("task-list");

// 5. Bind the Orchestrator to a specific P2P Mesh architecture
services.AddDistributedCrdtP2p("internal", replicaId: "node-1");

// 6. Configure the Network Mesh (Transports & Discovery)
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

## License & Pricing

`Ama.Enterprise` is built on a sustainable, developer-first licensing model. I believe in trusting developers. There is no draconian DRM, and absolutely no "phone home" analytics or telemetry.

To balance source-available accessibility with the reality of maintaining enterprise-grade distributed systems, `Ama.Enterprise` uses a revenue-capped dual license system enforced by an honor-based cryptographic key.

### Types of license

#### 1. Community License (Free)
This license is designed for startups, indie developers, hobbyists, and non-profit source-available projects.

You qualify for the free Community License if your company (including parent/controlling entities) or you as an individual have less than $1,000,000 USD in Trailing 12 Months (TTM) gross revenue, or if you are a registered non-profit with less than a $1,000,000 USD annual total budget. Government or quasi-government agencies do not qualify. 

Note: To remain eligible, your entity or organization must not have ever received more than $1,000,000 USD in outside capital, such as Private Equity, Venture Capital, or Angel investments. (Standard commercial bank debt or loans are explicitly excluded from this capital limit).

You do not need a license key. The software will run completely unrestricted.

#### 2. Enterprise License (Paid)
This license is for established companies, enterprises, heavily funded startups, and government agencies.

If your organization generates $1,000,000 USD or more in Trailing 12 Months (TTM) gross revenue, has a budget over $1,000,000 USD, has raised $1,000,000 USD or more in outside capital, or is a government/quasi-government agency, you are required to purchase an Enterprise License.

When you purchase an Enterprise License, you are paying for three things:
1. Legal Compliance & Risk Mitigation: A commercial Enterprise EULA that clears your legal department. The base Community License is provided strictly "AS IS" with an absolute limitation of liability protecting the author. The Enterprise tier provides the explicit commercial agreements and risk mitigation required by corporate compliance teams.
2. Guaranteed Support SLAs: Direct access to the author for architectural guidance, debugging, and prioritized bug fixes.
3. The Sustainability of the Toolkit: Ensuring the P2P mesh and CRDT engine you rely on continues to receive updates, security patches, and new features.

### How the "Honor-Based" System Works
Developers despise DRM, and so do I. License servers introduce single points of failure that have no place in a masterless P2P mesh.

`Ama.Enterprise` uses an honor-based cryptographic license:
* When you purchase a license, you receive a Base64-encoded RSA signature string.
* You inject this string during your application's DI bootstrap phase:

```csharp
services.ConfigureAmaEnterpriseLicense(options =>
{
    options.LicenseKey = "RSA-SIGNED-BASE64-KEY-STRING";
});

services.AddP2pMesh("internal")
        // ... add transports and discovery mechanisms
```

* Note on Client-Side Environments: To prevent inadvertently leaking private enterprise licenses into public-facing frontends, the `ConfigureAmaEnterpriseLicense` API forces a compile-time error (`[UnsupportedOSPlatform("browser")]`) if called from a Blazor WebAssembly environment. Client-side environments strictly rely on backend services for validation. For client-side deployments (like Blazor WebAssembly), use the `ConfigureAmaCommunityLicense()` declaration locally to satisfy initialization requirements. Your Enterprise license is successfully validated and enforced at the backend cluster level.
* The node validates the cryptographic signature locally. No network requests are ever made.
* If a license is missing or expired, the node will never crash, pause, or throttle your application. It will simply emit a single warning to your application logs on startup reminding you to acquire a license. Your mesh will remain 100% operational.

### Agencies, Consultancies, and SaaS
If you are an agency or consultancy building software for a client, the licensing requirement applies to the end-client running the software in production. 
* If your client qualifies under the thresholds, the Community License applies.
* If you are building a system for a large company or government entity exceeding the thresholds, the end-client (or the specific project budget) must procure the Enterprise License.

For SaaS products, the license tier is based on the Trailing 12 Months (TTM) revenue of the SaaS company providing the service, not the users of the SaaS.

### Open Source Ecosystem & Transitive Dependencies
You may not package `Ama.Enterprise` as a transitive dependency in a public library or framework (e.g., publishing an MIT-licensed package to NuGet) without explicitly disclosing and enforcing these revenue caps on your downstream users. Bypassing the revenue cap by wrapping the toolkit in a permissive open-source library is strictly prohibited.

### Enterprise Support & SLAs
If you decide to buy the Enterprise license in addition to supporting the project you get accountability as well. An Enterprise License includes the following support guarantees:

* Direct Developer Access: Private email and issue-tracker access directly to the library's developer.
* Prioritized Hotfixes: If you find a critical bug in the core CRDT or P2P networking layers, a patched NuGet package will be provided via a best-effort prioritized response within 2 business days (excluding public holidays and scheduled developer unavailability).
* Architectural Guidance: Up to 2 hours per year of direct architectural review to ensure you are configuring your CRDTs, mesh topologies, and data models correctly for your specific use case.