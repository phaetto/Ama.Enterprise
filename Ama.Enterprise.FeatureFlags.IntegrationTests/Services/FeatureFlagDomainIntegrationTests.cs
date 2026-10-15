namespace Ama.Enterprise.FeatureFlags.IntegrationTests.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.FeatureFlags.Extensions;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.Project.Tests.Common.Attributes;
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
            await manager.SetFlagAsync("TestFlag", true, "TestUser", null, null, IntentDeliveryMode.Acknowledged, CancellationToken.None));
            
        exception.Message.ShouldContain("Orchestrator registry is not initialized");
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
        var flags = await manager.GetFlagsAsync(CancellationToken.None);
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
        var observer = provider.GetRequiredService<TestStateObserver>();

        await StartAllHostedServicesAsync(provider);

        var initialMetadata = new FeatureFlagMetadata { ProductId = "EnterpriseERP", TenantId = "Tenant-XYZ" };
        var initialOwnership = new FeatureFlagOwnership { Owner = "BackendTeam", ContactInfo = "backend@ama.com" };

        await observer.WaitForStateChangeAsync(() => 
            manager.SetFlagAsync("BetaFeature", true, "Admin", initialMetadata, initialOwnership, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        var initialFlags = await manager.GetFlagsAsync(CancellationToken.None);
        var initialFlag = initialFlags["BetaFeature"];
        
        // Wait slightly to ensure DateTimeOffset ticks advance naturally for the audit trail assertion
        await Task.Delay(10);

        // Act - Partial Update dropping specific fields
        await observer.WaitForStateChangeAsync(() => 
            manager.SetFlagAsync("BetaFeature", false, "SystemAuto", metadata: null, ownership: null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        // Assert
        var updatedFlags = await manager.GetFlagsAsync(CancellationToken.None);
        var updatedFlag = updatedFlags["BetaFeature"];

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
        var observer = provider.GetRequiredService<TestStateObserver>();

        await StartAllHostedServicesAsync(provider);

        await observer.WaitForStateChangeAsync(() => 
            manager.SetFlagAsync("ObsoleteFeature", true, "Admin", null, null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        var flagsBefore = await manager.GetFlagsAsync(CancellationToken.None);
        flagsBefore.ShouldContainKey("ObsoleteFeature");

        // Act
        await observer.WaitForStateChangeAsync(() => 
            manager.RemoveFlagAsync("ObsoleteFeature", CancellationToken.None));

        // Assert
        var flagsAfter = await manager.GetFlagsAsync(CancellationToken.None);
        flagsAfter.ShouldNotContainKey("ObsoleteFeature");
    }

    [IntegrationFact]
    public void FeatureFlagState_IEquatable_ReturnsTrue_ForDeeplyEqualStructures()
    {
        // Arrange
        var timestamp = DateTimeOffset.UtcNow;
        var metadata = new FeatureFlagMetadata { ProductId = "ProductA", TenantId = "TenantX" };
        var ownership = new FeatureFlagOwnership { Owner = "OwnerY", ContactInfo = "ContactZ" };
        var audit = new FeatureFlagAudit { LastModifiedBy = "UserA", CreatedAt = timestamp, UpdatedAt = timestamp };

        var flag1 = new FeatureFlag { Name = "Flag1", IsEnabled = true, Metadata = metadata, Audit = audit, Ownership = ownership };
        var flag2 = new FeatureFlag { Name = "Flag1", IsEnabled = true, Metadata = metadata, Audit = audit, Ownership = ownership };

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

        // Satisfy transport constraints required by the CRDT P2P layer natively avoiding opening explicit network sockets.
        // Explicitly set the default return for Tasks to prevent background channel crash loops executing NullReferenceExceptions.
        var mockSender = new Mock<IDirectMessageSender>();
        mockSender.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockSender.Object);

        var mockDispatcher = new Mock<IApplicationPayloadDispatcher>();
        mockDispatcher.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockDispatcher.Object);

        var mockRegistry = new Mock<IPeerRegistry>();
        services.AddSingleton(mockRegistry.Object);

        var mockP2pRpcCorrelator = new Mock<IP2pRpcCorrelator>();
        mockP2pRpcCorrelator.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2pRpcCorrelator.Object);

        var mockP2p = new Mock<IP2pAlgorithm>();
        mockP2p.SetReturnsDefault(Task.CompletedTask);
        services.AddSingleton(mockP2p.Object);

        // Register default memory storage mapped to the distributed core explicitly guaranteeing an ephemeral domain space
        services.AddSingleton<IDistributedCrdtStorage, MemoryCrdtStorage>();

        // Explicitly register the distributed CRDT topological core bounds
        services.AddDistributedCrdtReplica("integration-replica-1");
        services.AddDistributedCrdtCore(x => {
            x.ActiveSyncEnabled = true;
        });
        services.AddDistributedCrdtP2p("integration-mesh", "integration-replica-1");

        // Bootstrap the feature flags exactly as a real application would natively
        services.AddFeatureFlags();

        // Map test observer explicit tracker validating decoupled state events securely
        services.AddSingleton<TestStateObserver>();
        services.AddSingleton<IFeatureFlagStateObserver>(sp => sp.GetRequiredService<TestStateObserver>());

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

    private sealed class TestStateObserver : IFeatureFlagStateObserver
    {
        private TaskCompletionSource? tcs;

        public Task OnFeatureFlagsUpdatedAsync(CancellationToken cancellationToken = default)
        {
            tcs?.TrySetResult();
            return Task.CompletedTask;
        }

        public async Task WaitForStateChangeAsync(Func<Task> action, int timeoutMs = 15000)
        {
            tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            await action().ConfigureAwait(false);
            await tcs.Task.WaitAsync(TimeSpan.FromMilliseconds(timeoutMs)).ConfigureAwait(false);
            tcs = null;
        }
    }
}