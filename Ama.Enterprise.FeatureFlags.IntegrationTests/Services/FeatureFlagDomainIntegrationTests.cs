namespace Ama.Enterprise.FeatureFlags.IntegrationTests.Services;

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.FeatureFlags.Extensions;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Moq;
using Shouldly;

public sealed class FeatureFlagDomainIntegrationTests
{
    [IntegrationFact]
    public async Task PrematureAccess_ThrowsInvalidOperationException_BeforeBootstrapperExecution()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();

        // Act & Assert
        var exception = await Should.ThrowAsync<InvalidOperationException>(async () => 
            await manager.SetFlagAsync("TestFlag", true, "TestUser", null, null, CancellationToken.None));
            
        exception.Message.ShouldContain("initialized");
    }

    [IntegrationFact]
    public async Task Bootstrapper_EagerlyInitializesGlobalDocument_And_ManagerExposesState()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();

        // Act
        await StartAllHostedServicesAsync(provider);

        // Assert
        var flags = manager.GetFlags();
        flags.ShouldNotBeNull();
        flags.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task SetFlagAsync_PreservesAuditAndMetadata_OnPartialUpdates()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();

        await StartAllHostedServicesAsync(provider);

        var initialMetadata = new FeatureFlagMetadata("EnterpriseERP", "Tenant-XYZ");
        var initialOwnership = new FeatureFlagOwnership("BackendTeam", "backend@ama.com");

        await WaitForStateChangeAsync(manager, () => 
            manager.SetFlagAsync("BetaFeature", true, "Admin", initialMetadata, initialOwnership, CancellationToken.None));

        var initialFlag = manager.GetFlags()["BetaFeature"];
        
        // Wait slightly to ensure DateTimeOffset ticks advance naturally for the audit trail assertion
        await Task.Delay(10);

        // Act - Partial Update dropping specific fields
        await WaitForStateChangeAsync(manager, () => 
            manager.SetFlagAsync("BetaFeature", false, "SystemAuto", metadata: null, ownership: null, CancellationToken.None));

        // Assert
        var updatedFlag = manager.GetFlags()["BetaFeature"];

        updatedFlag.IsEnabled.ShouldBeFalse();
        updatedFlag.Audit.LastModifiedBy.ShouldBe("SystemAuto");
        updatedFlag.Audit.CreatedAt.ShouldBe(initialFlag.Audit.CreatedAt);
        updatedFlag.Audit.UpdatedAt.ShouldBeGreaterThan(initialFlag.Audit.UpdatedAt);
        
        updatedFlag.Metadata.ProductId.ShouldBe("EnterpriseERP");
        updatedFlag.Ownership.Owner.ShouldBe("BackendTeam");
    }

    [IntegrationFact]
    public async Task RemoveFlagAsync_SuccessfullyTombstonesFlag_FromState()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();

        await StartAllHostedServicesAsync(provider);

        await WaitForStateChangeAsync(manager, () => 
            manager.SetFlagAsync("ObsoleteFeature", true, "Admin", null, null, CancellationToken.None));

        manager.GetFlags().ShouldContainKey("ObsoleteFeature");

        // Act
        await WaitForStateChangeAsync(manager, () => 
            manager.RemoveFlagAsync("ObsoleteFeature", CancellationToken.None));

        // Assert
        manager.GetFlags().ShouldNotContainKey("ObsoleteFeature");
    }

    [IntegrationFact]
    public void FeatureFlagState_IEquatable_ReturnsTrue_ForDeeplyEqualStructures()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var metadata = new FeatureFlagMetadata("ProductA", "TenantX");
        var ownership = new FeatureFlagOwnership("OwnerY", "ContactZ");
        var audit = new FeatureFlagAudit("UserA", timestamp, timestamp);

        var flag1 = new FeatureFlag("Flag1", true, metadata, audit, ownership);
        var flag2 = new FeatureFlag("Flag1", true, metadata, audit, ownership);

        var state1 = new FeatureFlagState();
        state1.Flags.Add("Flag1", flag1);

        var state2 = new FeatureFlagState();
        state2.Flags.Add("Flag1", flag2);

        // Act & Assert
        state1.Equals(state2).ShouldBeTrue();
        state1.GetHashCode().ShouldBe(state2.GetHashCode());
    }

    private ServiceProvider BuildTestProvider()
    {
        var services = new ServiceCollection();
        
        // Standard DI logger bypassing abstract Xunit logging constraints explicitly.
        services.AddLogging(); 

        // Satisfy transport constraints required by the CRDT P2P layer natively avoiding opening explicit network sockets
        services.AddSingleton(Mock.Of<IDirectMessageSender>());
        services.AddSingleton(Mock.Of<IApplicationPayloadDispatcher>());
        services.AddSingleton(Mock.Of<IPeerRegistry>());

        // Register default memory storage mapped to the distributed core explicitly guaranteeing an ephemeral domain space
        services.AddSingleton<IDistributedCrdtStorage, MemoryCrdtStorage>();

        // Bootstrap the feature flags exactly as a real application would natively
        services.AddFeatureFlags("integration-replica-1", opts => 
        {
            opts.InternalMeshId = "integration-mesh";
        });

        return services.BuildServiceProvider();
    }

    private async Task StartAllHostedServicesAsync(ServiceProvider provider)
    {
        var hostedServices = provider.GetServices<IHostedService>();
        foreach (var service in hostedServices)
        {
            await service.StartAsync(CancellationToken.None);
        }
    }

    private async Task WaitForStateChangeAsync(IFeatureFlagClusterManager manager, Func<Task> action, int timeoutMs = 5000)
    {
        var tcs = new TaskCompletionSource();
        
        void Handler(object? sender, EventArgs e) => tcs.TrySetResult();
        
        manager.StateChanged += Handler;
        try
        {
            await action();
            await tcs.Task.WaitAsync(TimeSpan.FromMilliseconds(timeoutMs));
        }
        finally
        {
            manager.StateChanged -= Handler;
        }
    }
}