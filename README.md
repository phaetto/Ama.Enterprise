# Ama.Enterprise

A .NET 10 enterprise-grade toolkit for building decentralized, masterless Peer-to-Peer (P2P) distributed systems. It combines networking meshes with Conflict-free Replicated Data Types (CRDTs) to keep data highly available and eventually consistent across nodes without needing centralized databases or brokers.

## Features

- Native AOT Ready: Built for Native AOT. Uses source-generated `System.Text.Json` contexts and MessagePack polymorphic resolvers for high-performance serialization without runtime reflection.
- Masterless P2P Mesh Networking: Nodes form clusters dynamically. They find peers, connect, and sync data automatically using Gossip and Anti-Entropy protocols.
- Transport Agnostic: Supports multiple network transports and topologies out of the box:
  - TCP & UDP: Standard stream and datagram routing.
  - QUIC: Fast, encrypted TLS 1.3 streams.
  - ASP.NET Core (Kestrel): Integrate P2P meshes directly into existing web applications.
  - WebRTC: Direct browser-to-server or node-to-node communication across firewalls (includes Secure WebSockets/WSS support).
  - MQTT: Connect multiple distinct network meshes using standard IoT brokers.
- Enterprise Security & Zero-Trust: Encrypt data in transit using AES-GCM. Authenticate node connections via strict X.509 Certificates (with offline support) or dynamic Zero-Trust session token routing policies.
- High-Performance Architecture: Uses object pools, lock-free channels, dynamic journal limits, and network jitter controls to handle high traffic and prevent broadcast storms.
- Pluggable Peer Discovery: Find other nodes using UDP Multicast, DNS A/SRV records, or active HTTP polling depending on your infrastructure.
- Distributed CRDT Orchestrator: Manage the lifecycles of distributed data documents. The system creates, syncs, and removes documents across the mesh automatically, and handles snapshots and log truncation.
- Sharding: Built-in consistent hash sharding to distribute data and scale CRDT documents on nodes across the network.
- Storage Backends: Save data using memory, Azure Table Storage, or custom databases (like the included AOT SQLite implementation).
- Built-in Telemetry: Integrates with `System.Diagnostics.Metrics`. It includes a P2P metric aggregator that shares hardware and network stats across nodes using fast binary data slicing.

## State of the repo

The repo is still in preview and I am working to reach the 0.1 version. Until the version reaches 1.0 there will be frequent updates, fixes, but also _refactorings_ that might introduce breaking changes.

The code is running on some PROD systems and is very stable. The main work now is to wrap up protocols, algorithms, and tooling to provide more features out-of-the-box.

## Quickstart

Check the [samples repo here](https://github.com/phaetto/Ama.Enterprise.Samples) if you just want to start using some code.

## Architecture Overview

The architecture of `Ama.Enterprise` is divided into strict layers to separate concerns. This keeps your business logic isolated from the complex details of network routing and data synchronization.

### High-Level Ecosystem & Layering

![End Products Overview](./Images/Enterprise%20CRDTs%20-%20End%20Products%20Overview.png)

The ecosystem is built on three main layers:
1. The P2P Mesh Layer: The lowest level. It handles sending bytes, finding nodes, managing network streams, and enforcing encryption.
2. The CRDT Distributed Layer: Sits on top of the mesh. It ignores the network details and focuses on syncing local data changes using Version Vectors.
3. The End Products & Applications: The top level. Your applications (like the Feature Flags module or custom services) use the CRDT layer. You just read and change standard .NET objects, and the lower layers ensure these changes reach all other nodes.

### The P2P Mesh Networking Layer

![P2P Architecture](./Images/Enterprise%20CRDTs%20-%20P2P.png)

The Peer-to-Peer layer (`Ama.Enterprise.P2p.*`) is masterless and adapts to different environments.
- Discovery: Nodes join the network automatically. They can find each other via UDP Multicast (local networks), DNS (cloud/Kubernetes), or HTTP polling.
- Security & Authentication: Nodes must pass Zero-Trust boundaries before connecting. This uses either X.509 Certificates or Session tokens. Data is encrypted on the wire with AES-GCM.
- Transports: The routing system does not depend on one specific protocol. Messages can travel over TCP, UDP, QUIC, Kestrel, or WebRTC depending on your configuration.
- Algorithms: Data spreads quickly across the network using Gossip protocols. Anti-Entropy background loops run to fix missing data and heal separated network parts.

### The Distributed CRDT Orchestration Layer

![Distributed CRDTs Architecture](./Images/Enterprise%20CRDTs%20-%20Distributed%20CRDTs.png)

The Orchestration layer (`Ama.Enterprise.CRDT.Distributed.*`) connects your data models to the P2P mesh.
- Local Replica Scopes: Every node keeps a local copy of a document. Changes are applied instantly locally without waiting for network locks.
- Storage & Journaling: Changes are saved as mathematical patches into a storage backend like Memory, SQLite, or Azure Table Storage.
- Mesh Synchronization: The orchestrator runs continuous background loops. It uses Version Vectors to track the exact order of events across nodes. Nodes compare these vectors and exchange only the missing data patches (or full snapshots if needed).
- Tombstoning & Eviction: The system manages the lifecycle of dynamic objects. It handles deleting documents, ignoring outdated states, and removing disconnected nodes from the cluster.

## Load tests

[Check the azure load tests for a performance overview.](performance.md)

## Showcases

This repository includes interactive console applications that demonstrate the framework across dynamically created nodes:

- [Distributed CRDT Showcase](Ama.Enterprise.CRDT.Distributed.ShowCase) (`Ama.Enterprise.CRDT.Distributed.ShowCase`): Shows how the orchestrator manages multiple CRDT collections (Task Lists and IoT Fleet Statuses). Features local file storage, Native MessagePack serialization, admin telemetry, and a load-testing mode that pumps thousands of operations into the mesh.
- [Distributed CRDT Topology Showcase](Ama.Enterprise.CRDT.Distributed.Topology.ShowCase) (`Ama.Enterprise.CRDT.Distributed.Topology.ShowCase`): Demonstrates Multi-Mesh architectures using isolated networks (TCP/UDP servers and WebRTC clients) connected by Zero-Trust routing policies. Features multi-role sessions and WebRTC signaling.
- [Feature Flags Showcase](Ama.Enterprise.FeatureFlags.ShowCase) (`Ama.Enterprise.FeatureFlags.ShowCase`): A masterless P2P Feature Flag management console. Uses Azure Table Storage, certificate authentication, and AES-GCM encryption.

*Tip: While running any showcase, type `clone` into the console. This will automatically start a new node process on a new port that instantly discovers and syncs with your primary node.*

For additional advanced scenarios, tutorials, and complete sample projects, please check out the [Ama.Enterprise.Examples](https://github.com/amuste/Ama.Enterprise.Examples) repository.

## Applications Provided Out-Of-The-Box

This repository includes ready-to-use tooling designed to integrate into your decentralized environments:

- P2P Telemetry CLI (`Ama.Enterprise.P2p.Telemetry.Cli`): A terminal UI built with `Terminal.Gui`. It connects to the mesh to gather and display cluster metrics, hardware stats, and network histories.

## Prerequisites

- [.NET 10.0 SDK](https://dotnet.microsoft.com/download/dotnet/10.0) or later.
- A compatible IDE such as Visual Studio 2022 (latest preview), JetBrains Rider, or VS Code.
- *(Optional)* Azurite / Azure Storage Emulator if you plan to use the Azure Table Storage backend locally.

## Installation

The toolkit is highly modular. You only need to install the packages required for your node architecture. 

To get started with the core P2P network and the distributed CRDT orchestrator, install the primary packages via the .NET CLI:

```bash
# Core P2P Mesh Networking
dotnet add package Ama.Enterprise.P2p

# Distributed CRDT Orchestrator
dotnet add package Ama.Enterprise.CRDT.Distributed
```

Depending on your environment, you can install specific transports or storage tools:

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

// Mutate the state. Changes are synced across the network!
await taskManager.SetTaskAsync("dev-team-list", "task-1", "Review PR", isDone: false, cancellationToken);
```

## AI Coding Assistance

To maintain full transparency, please note that AI coding assistants and Large Language Models (LLMs) were actively used in the design, development, testing, and documentation of this repository. While AI tools significantly accelerated the generation of code and ideas, all output was rigorously reviewed, steered, tested, and refined by human developers (me). I believe in leveraging these tools to enhance productivity while taking responsibility for the library's architecture, security, and mathematical accuracy.

## License & Pricing

`Ama.Enterprise` is built on a sustainable, developer-first licensing model. I believe in trusting developers. There is no draconian DRM, and absolutely no "phone home" analytics or telemetry.

To balance source-available accessibility with the reality of maintaining enterprise-grade distributed systems, `Ama.Enterprise` uses a revenue-capped dual license system enforced by an honor-based cryptographic key.

### Types of license

#### 1. Community License (Free)
This license is designed for startups, indie developers, hobbyists, and non-profit source-available projects.

You qualify for the free Community License if your company (including parent/controlling entities) or you as an individual have less than $1,000,000 USD in Trailing 12 Months (TTM) gross revenue, or if you are a registered non-profit with less than a $1,000,000 USD annual total budget. Government or quasi-government agencies do not qualify. 

Note: To remain eligible, your entity or organization must not have ever received more than $1,000,000 USD in outside capital, such as Private Equity, Venture Capital, or Angel investments. (Standard commercial bank debt or loans are explicitly excluded from this capital limit).

You do not need a license key. The software will run unrestricted.

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

* Note on Client-Side Environments: To prevent inadvertently leaking private enterprise licenses into public-facing frontends, the `ConfigureAmaEnterpriseLicense` API forces a compile-time error (`[UnsupportedOSPlatform("browser")]`) if called from a Blazor WebAssembly environment. Client-side environments rely on backend services for validation. For client-side deployments (like Blazor WebAssembly), use the `ConfigureAmaCommunityLicense()` declaration locally to satisfy initialization requirements. Your Enterprise license is validated and enforced at the backend cluster level.
* The node validates the cryptographic signature locally. No network requests are ever made.
* If a license is missing or expired, the node will never crash, pause, or throttle your application. It will emit a single warning to your application logs on startup reminding you to acquire a license. Your mesh will remain 100% operational.

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
* Architectural Guidance: A number of hours per year of direct architectural review to ensure you are configuring your CRDTs, mesh topologies, and data models for your specific use case.