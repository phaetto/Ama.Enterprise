# Ama.Enterprise

A .NET 10 enterprise-grade toolkit for building decentralized, masterless Peer-to-Peer (P2P) distributed systems. It combines robust networking meshes with Conflict-free Replicated Data Types (CRDTs) to guarantee high availability, fault tolerance, and eventual consistency across nodes without requiring centralized databases or brokers.

## Features

- **Native AOT Ready**: Designed from the ground up for Native AOT. Zero runtime reflection, utilizing strict source-generated `System.Text.Json` contexts and MessagePack polymorphic resolvers for high-performance serialization.
- **Masterless P2P Mesh Networking**: Form decentralized clusters dynamically. Nodes automatically discover peers, negotiate connections, and replicate states via highly optimized Gossip and Anti-Entropy protocols.
- **Transport Agnostic**: Out-of-the-box support for multiple network transports and topologies:
  - **TCP & UDP**: Low-level stream and datagram routing.
  - **QUIC**: High-performance natively multiplexed and encrypted TLS 1.3 streams.
  - **ASP.NET Core (Kestrel)**: Bind P2P meshes directly into existing web host pipelines natively.
  - **WebRTC**: Out-of-band signaling and data channels for direct browser-to-server or NAT-traversing node communication (includes Secure WebSockets/WSS support).
  - **MQTT**: Decoupled multi-mesh architectures using standard IoT brokers for discovery and transport.
- **Enterprise Security & Zero-Trust**: Implement End-to-End (E2E) AES-GCM data-in-transit wire encryption. Secure node connections using strict X.509 Certificate authentication (with offline/air-gapped revocation support) or dynamic Zero-Trust session token routing policies.
- **High-Performance Architecture**: Lock-free single-reader object pooled channels (`IValueTaskSource`), dynamic journal backpressure limits, and anti-entropy network jitter algorithms to prevent "thundering herd" broadcast storms. Includes built-in distributed amnesia and split-brain protection.
- **Pluggable Peer Discovery**: Locate peers dynamically using UDP Multicast, DNS A records (with possible extension to SRV), or active HTTP polling depending on your infrastructure constraints.
- **Distributed CRDT Orchestrator**: Manage the lifecycles of hundreds of distributed CRDT documents dynamically. Create, sync, and tombstone documents across the mesh automatically with built-in snapshotting and journal truncation.
- **Storage Backends**: Persist distributed states safely using ephemeral Memory, scalable Azure Table Storage (with unified bounds tracking), or easily connect your own backend (`Ama.Enterprise.CRDT.Distributed.ShowCase` has its own implementation of AOT SQLite).
- **Built-in Telemetry**: Natively integrated with `System.Diagnostics.Metrics`. It includes a P2P metric aggregator utilizing magic-byte binary slicing that pushes time-series hardware and mesh statistics across isolated nodes dynamically.

## Architecture Overview

The architecture of `Ama.Enterprise` is strictly layered to provide a clean separation of concerns, ensuring that high-level business logic remains decoupled from the complexities of decentralized network routing and state synchronization.

### High-Level Ecosystem & Layering

![End Products Overview](./Images/Enterprise%20CRDTs%20-%20End%20Products%20Overview.png)

At the macro level, the ecosystem is built upon three foundational pillars:
1. **The P2P Mesh Layer**: The lowest level handling raw byte distribution, node discovery, transport streams, and cryptographic boundaries.
2. **The CRDT Distributed Layer**: The orchestration engine sitting on top of the mesh. It abstracts the network away entirely, treating the mesh as a medium to synchronize local replica storage mathematical structures via Version Vectors.
3. **The End Products & Applications**: The top-level domain. Applications like the Feature Flags module, the P2P Telemetry CLI, or your own custom services consume the CRDT layer. They simply read and mutate standard .NET objects, and the underlying layers guarantee that those mutations are eventually consistent across the entire global cluster.

### The P2P Mesh Networking Layer

![P2P Architecture](./Images/Enterprise%20CRDTs%20-%20P2P.png)

The Peer-to-Peer layer (`Ama.Enterprise.P2p.*`) is designed to be completely masterless and highly adaptable to different infrastructure environments.
- **Discovery**: Nodes bootstrap into the network dynamically. Depending on the environment, they can discover each other via UDP Multicast (for local networks), DNS resolution (for Kubernetes/Cloud environments), or HTTP polling.
- **Security & Authentication**: Before accepting any topology connections, nodes must pass strict Zero-Trust boundaries. This is handled either by mutual TLS (mTLS) X.509 Certificates or dynamic token-based Session Authenticators. Data-in-transit is secured via AES-GCM wire encoders preventing eavesdropping.
- **Transports**: The routing dispatcher is transport-agnostic. Packets can be seamlessly multiplexed over TCP, UDP, QUIC, ASP.NET Core Kestrel, or WebRTC data channels depending on the configured Multi-Mesh bindings.
- **Algorithms**: Network states are replicated via push-pull Gossip algorithms for rapid epidemic payload dissemination and targeted Anti-Entropy background loops to heal partitioned network islands silently.

### The Distributed CRDT Orchestration Layer

![Distributed CRDTs Architecture](./Images/Enterprise%20CRDTs%20-%20Distributed%20CRDTs.png)

The Orchestration layer (`Ama.Enterprise.CRDT.Distributed.*`) bridges the gap between your data models and the raw P2P mesh.
- **Local Replica Scopes**: Every node acts as an independent replica holding a localized state of a document. Modifications are applied instantly to the local scope without waiting for network locks.
- **Storage & Journaling**: Mutations are captured mathematically as structural patches and journaled into a pluggable storage backend. This could be high-performance Ephemeral Memory, a local SQLite database, or highly scalable Azure Table Storage.
- **Mesh Synchronization**: The `ICrdtDocumentOrchestrator` runs continuous maintenance loops. It leverages Dotted Version Vectors (DVV) to track exact causality across nodes. When an Anti-Entropy sync triggers, nodes evaluate their version vectors and exchange only the missing mathematical patches (or fallback to full snapshots if log truncation has occurred).
- **Tombstoning & Eviction**: The orchestrator inherently handles the lifecycle of dynamic objects, safely tombstoning deleted documents, rejecting zombie states (amnesia), and cooling down evicted nodes before pruning them from the global cluster registry.

## Showcases

This repository includes highly interactive console applications demonstrating the framework across dynamically spawned nodes:

- [**Distributed CRDT Showcase**](Ama.Enterprise.CRDT.Distributed.ShowCase) (`Ama.Enterprise.CRDT.Distributed.ShowCase`): Demonstrates the `ICrdtDocumentOrchestrator` managing multiple generic CRDT collections (Task Lists and IoT Fleet Statuses) simultaneously. Features local file-based storage, Native MessagePack binary serialization, out-of-band admin telemetry forwarding, and a `hammer` load-testing mode that pumps thousands of concurrent operations into the mesh.
- [**Distributed CRDT Topology Showcase**](Ama.Enterprise.CRDT.Distributed.Topology.ShowCase) (`Ama.Enterprise.CRDT.Distributed.Topology.ShowCase`): Demonstrates advanced Multi-Mesh architectures using isolated network topologies (Server TCP/UDP meshes and User WebRTC out-of-band signaling meshes) bridged by decoupled Zero-Trust RBAC routing policies. Features dynamic multi-role session capabilities, WebRTC signaling bridging, and region-based generic clustering.
- [**Feature Flags Showcase**](Ama.Enterprise.FeatureFlags.ShowCase) (`Ama.Enterprise.FeatureFlags.ShowCase`): A masterless P2P Feature Flag management console. Demonstrates extracting high-level applications backed by CRDT synchronization, Azure Table Storage persistence, strict X.509 Certificate mutual authentication, and E2E AES-GCM data-in-transit wire encryption.

*Tip: While running any showcase, type `clone` into the console. This will automatically spawn a brand-new node process on a new port that instantly discovers and syncs with your primary node.*

For additional advanced scenarios, tutorials, and complete sample projects, please check out the [Ama.Enterprise.Examples](https://github.com/amuste/Ama.Enterprise.Examples) repository.

## Applications Provided Out-Of-The-Box

This repository includes ready-to-use tooling designed to integrate seamlessly into your decentralized environments:

- **P2P Telemetry CLI** (`Ama.Enterprise.P2p.Telemetry.Cli`): A terminal-based real-time UI built with `Terminal.Gui`. It connects to the mesh as a passive observer to aggregate and display decentralized cluster metrics, time-series histories, and hardware utilization across the distributed topology.

## Prerequisites

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) or later.
- A compatible IDE such as Visual Studio 2022 (latest preview), JetBrains Rider, or VS Code.
- *(Optional)* Azurite / Azure Storage Emulator if you plan to use the Azure Table Storage persistence backend locally.

## Installation

The toolkit is highly modular. You only need to install the specific packages required for your node architecture. 

To get started with the core P2P network and the distributed CRDT orchestrator, install the primary packages via the .NET CLI:

```bash
# Core P2P Mesh Networking
dotnet add package Ama.Enterprise.P2p

# Distributed CRDT Orchestrator
dotnet add package Ama.Enterprise.CRDT.Distributed
```

Depending on your environment, you may also want to install specific transports or persistence mechanisms to extend the capabilities of your mesh:

```bash
# ASP.NET Core HTTP/Kestrel Transport integration
dotnet add package Ama.Enterprise.P2p.AspNetCore

# Azure Table Storage backend for CRDT persistence
dotnet add package Ama.Enterprise.CRDT.Distributed.TableStorage

# MessagePack AOT serialization support
dotnet add package Ama.Enterprise.CRDT.MessagePack
```

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
    options.JournalBackpressureCeilingThreshold = 10000;
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

#### Contact
Do you have any inquiries or questions? Feel free to contact me on my [LinkedIn](https://www.linkedin.com/in/alexandermantzoukas)

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
* Prioritized Hotfixes: If you find a critical bug in the core CRDT or P2P networking layers, a patched NuGet package will be provided via a best-effort prioritized response within a number of business days (excluding public holidays and scheduled developer unavailability).
* Architectural Guidance: A number of hours per year of direct architectural review to ensure you are configuring your CRDTs, mesh topologies, and data models correctly for your specific use case.