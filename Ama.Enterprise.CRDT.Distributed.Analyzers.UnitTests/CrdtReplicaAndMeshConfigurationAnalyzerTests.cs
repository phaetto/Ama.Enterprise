namespace Ama.Enterprise.CRDT.Distributed.Analyzers.UnitTests;

using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using Xunit;

public sealed class CrdtReplicaAndMeshConfigurationAnalyzerTests
{
    private const string StubDefinitions = @"
using System;

namespace Microsoft.Extensions.DependencyInjection
{
    public interface IServiceCollection { }
    public class ServiceCollection : IServiceCollection { }
}

namespace Ama.Enterprise.P2p.Extensions
{
    using Microsoft.Extensions.DependencyInjection;

    public interface IP2pMeshBuilder
    {
        string MeshId { get; }
        IServiceCollection Services { get; }
    }

    public static class P2pServiceCollectionExtensions
    {
        public static IP2pMeshBuilder AddP2pMesh(this IServiceCollection services, string meshId, Action<object>? configure = null) => null!;
        public static IP2pMeshBuilder AddGossipNetwork(this IP2pMeshBuilder builder) => builder;
    }
}

namespace Ama.Enterprise.CRDT.Distributed.Extensions
{
    using Ama.Enterprise.P2p.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public static class ServiceCollectionExtensions
    {
        public static IServiceCollection AddDistributedCrdtCore(this IServiceCollection services, Action<object>? configure = null) => services;
        public static IServiceCollection AddDistributedCrdtReplica(this IServiceCollection services, string replicaId) => services;
        public static IServiceCollection AddDistributedCrdtReplica(this IServiceCollection services, string replicaId, params string[] meshIds) => services;
        public static IServiceCollection AddDistributedCrdtP2p(this IServiceCollection services, string meshId, string replicaId) => services;
        public static IP2pMeshBuilder BindCrdtReplica(this IP2pMeshBuilder builder, string? replicaId = null) => builder;
    }
}
";

    [Fact]
    public async Task WhenCoreCalledWithoutAnyReplica_ShouldReportDCRDT0001()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.CRDT.Distributed.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services)
        {
            services.{|#0:AddDistributedCrdtCore|}();
        }
    }
}
";
        var expected = new DiagnosticResult(CrdtReplicaAndMeshConfigurationAnalyzer.MissingReplicaDiagnosticId, DiagnosticSeverity.Warning)
            .WithLocation(0);

        var test = CreateTest(source, expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenReplicaRegisteredWithoutAnyP2pBinding_ShouldReportDCRDT0002()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.CRDT.Distributed.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services)
        {
            services.AddDistributedCrdtCore();
            services.{|#0:AddDistributedCrdtReplica|}(""node-1"");
        }
    }
}
";
        var expected = new DiagnosticResult(CrdtReplicaAndMeshConfigurationAnalyzer.UnboundReplicaDiagnosticId, DiagnosticSeverity.Warning)
            .WithLocation(0)
            .WithArguments("node-1");

        var test = CreateTest(source, expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenP2pMeshNotBoundToAnyReplica_ShouldReportDCRDT0003()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.CRDT.Distributed.Extensions;
    using Ama.Enterprise.P2p.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services)
        {
            var replicaId = ""node-1"";
            services.AddDistributedCrdtCore();
            services.AddDistributedCrdtP2p(""internal"", replicaId);
            services.AddP2pMesh(""internal"");
            services.{|#0:AddP2pMesh|}(""admin-telemetry"");
        }
    }
}
";
        var expected = new DiagnosticResult(CrdtReplicaAndMeshConfigurationAnalyzer.UnboundMeshDiagnosticId, DiagnosticSeverity.Warning)
            .WithLocation(0)
            .WithArguments("admin-telemetry");

        var test = CreateTest(source, expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenUnboundTelemetryMeshSuppressedWithPragma_ShouldNotReportWarning()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.CRDT.Distributed.Extensions;
    using Ama.Enterprise.P2p.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services)
        {
            var replicaId = ""node-1"";
            services.AddDistributedCrdtCore();
            services.AddDistributedCrdtP2p(""internal"", replicaId);
            services.AddP2pMesh(""internal"");
#pragma warning disable DCRDT0003
            services.AddP2pMesh(""admin-telemetry"");
#pragma warning restore DCRDT0003
        }
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenP2pBindingReferencesUnconfiguredMesh_ShouldReportDCRDT0004()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.CRDT.Distributed.Extensions;
    using Ama.Enterprise.P2p.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services)
        {
            services.AddDistributedCrdtCore();
            services.{|#0:AddDistributedCrdtP2p|}(""interal-typo"", ""node-1"");
            services.{|#1:AddP2pMesh|}(""internal"");
        }
    }
}
";
        var expectedUnconfigured = new DiagnosticResult(CrdtReplicaAndMeshConfigurationAnalyzer.UnconfiguredMeshDiagnosticId, DiagnosticSeverity.Warning)
            .WithLocation(0)
            .WithArguments("node-1", "interal-typo");

        var expectedUnboundMesh = new DiagnosticResult(CrdtReplicaAndMeshConfigurationAnalyzer.UnboundMeshDiagnosticId, DiagnosticSeverity.Warning)
            .WithLocation(1)
            .WithArguments("internal");

        var test = CreateTest(source, expectedUnconfigured, expectedUnboundMesh);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenSingleReplicaSharedAcrossMultipleMeshes_ShouldNotReportDiagnostics()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.CRDT.Distributed.Extensions;
    using Ama.Enterprise.P2p.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services, int port)
        {
            var replicaId = $""node-{port}"";
            services.AddDistributedCrdtCore();
            services.AddDistributedCrdtReplica(replicaId, ""server-tcp"", ""user-webrtc"");
            services.AddP2pMesh(""server-tcp"");
            services.AddP2pMesh(""user-webrtc"");
        }
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenBindCrdtReplicaChainedOnP2pMeshBuilder_ShouldNotReportDiagnostics()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.CRDT.Distributed.Extensions;
    using Ama.Enterprise.P2p.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services)
        {
            services.AddDistributedCrdtCore();
            services
                .AddP2pMesh(""internal"")
                .AddGossipNetwork()
                .BindCrdtReplica(""node-1"");
        }
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenOnlyP2pUsedWithoutCrdt_ShouldNotReportDiagnostics()
    {
        var source = StubDefinitions + @"
namespace TestApp
{
    using Ama.Enterprise.P2p.Extensions;
    using Microsoft.Extensions.DependencyInjection;

    public class Startup
    {
        public void Configure(IServiceCollection services)
        {
            services.AddP2pMesh(""monitoring-only"");
        }
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    private static CSharpAnalyzerTest<CrdtReplicaAndMeshConfigurationAnalyzer, DefaultVerifier> CreateTest(
        string source,
        params DiagnosticResult[] expected)
    {
        var test = new CSharpAnalyzerTest<CrdtReplicaAndMeshConfigurationAnalyzer, DefaultVerifier>
        {
            TestCode = source
        };

        test.ExpectedDiagnostics.AddRange(expected);
        return test;
    }
}