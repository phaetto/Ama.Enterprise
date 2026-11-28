namespace Ama.Enterprise.FeatureFlags.IntegrationTests.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Models;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.CRDT.Distributed.Services.Storage;
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
    public async Task PrematureAccess_ThrowsInvalidOperationException_BeforeOrchestratorInitialization()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagCrdtManager>();

        // Act & Assert
        var exception = await Should.ThrowAsync<InvalidOperationException>(async () => 
            await manager.SetFlagAsync("EnterpriseERP", "Tenant-XYZ", "TestFlag", true, "TestUser", null, IntentDeliveryMode.Acknowledged, CancellationToken.None));
            
        exception.Message.ShouldContain("Orchestrator registry is not initialized");
    }

    [IntegrationFact]
    public async Task GetFlagsAsync_BeforeAnyFlagsCreated_ReturnsEmpty()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagCrdtManager>();

        // Act
        await StartAllHostedServicesAsync(provider);

        // Assert
        var flags = await manager.GetFlagsAsync("EnterpriseERP", "Tenant-XYZ", CancellationToken.None);
        flags.ShouldNotBeNull();
        flags.ShouldBeEmpty();

        var allFlags = await manager.GetAllFlagsAsync(CancellationToken.None);
        allFlags.ShouldNotBeNull();
        allFlags.ShouldBeEmpty();
    }

    [IntegrationFact]
    public async Task SetFlagAsync_PreservesAuditAndMetadata_OnPartialUpdates()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagCrdtManager>();
        var observer = provider.GetRequiredService<TestStateObserver>();

        await StartAllHostedServicesAsync(provider);

        var initialOwnership = new FeatureFlagOwnership { Owner = "BackendTeam", ContactInfo = "backend@ama.com" };

        await observer.WaitForStateChangeAsync(() => 
            manager.SetFlagAsync("EnterpriseERP", "Tenant-XYZ", "BetaFeature", true, "Admin", initialOwnership, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        var initialFlags = await manager.GetFlagsAsync("EnterpriseERP", "Tenant-XYZ", CancellationToken.None);
        var initialFlag = initialFlags["BetaFeature"];
        
        // Wait slightly to ensure DateTimeOffset ticks advance naturally for the audit trail assertion
        await Task.Delay(10);

        // Act - Partial Update dropping specific fields
        await observer.WaitForStateChangeAsync(() => 
            manager.SetFlagAsync("EnterpriseERP", "Tenant-XYZ", "BetaFeature", false, "SystemAuto", ownership: null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        // Assert
        var updatedFlags = await manager.GetFlagsAsync("EnterpriseERP", "Tenant-XYZ", CancellationToken.None);
        var updatedFlag = updatedFlags["BetaFeature"];

        updatedFlag.IsEnabled.ShouldBeFalse();
        updatedFlag.Audit.LastModifiedBy.ShouldBe("SystemAuto");
        updatedFlag.Audit.CreatedAt.ShouldBe(initialFlag.Audit.CreatedAt);
        updatedFlag.Audit.UpdatedAt.ShouldBeGreaterThan(initialFlag.Audit.UpdatedAt);
        
        updatedFlag.Metadata.ProductId.ShouldBe("EnterpriseERP");
        updatedFlag.Metadata.TenantId.ShouldBe("Tenant-XYZ");
        updatedFlag.Ownership.Owner.ShouldBe("BackendTeam");
    }

    [IntegrationFact]
    public async Task RemoveFlagAsync_SuccessfullyTombstonesFlag_FromState()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagCrdtManager>();
        var observer = provider.GetRequiredService<TestStateObserver>();

        await StartAllHostedServicesAsync(provider);

        await observer.WaitForStateChangeAsync(() => 
            manager.SetFlagAsync("EnterpriseERP", "Tenant-XYZ", "ObsoleteFeature", true, "Admin", null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        var flagsBefore = await manager.GetFlagsAsync("EnterpriseERP", "Tenant-XYZ", CancellationToken.None);
        flagsBefore.ShouldContainKey("ObsoleteFeature");

        // Act
        await observer.WaitForStateChangeAsync(() => 
            manager.RemoveFlagAsync("EnterpriseERP", "Tenant-XYZ", "ObsoleteFeature", IntentDeliveryMode.Acknowledged, CancellationToken.None));

        // Assert
        var flagsAfter = await manager.GetFlagsAsync("EnterpriseERP", "Tenant-XYZ", CancellationToken.None);
        flagsAfter.ShouldNotContainKey("ObsoleteFeature");
    }

    [IntegrationFact]
    public async Task SetFlagAsync_ShardsDocumentsPerProductAndTenant_And_AggregatesAcrossCluster()
    {
        // Arrange
        using var provider = BuildTestProvider();
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope("integration-replica-1");
        var manager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagCrdtManager>();
        var orchestrator = crdtScope.Orchestrator;
        var observer = provider.GetRequiredService<TestStateObserver>();

        await StartAllHostedServicesAsync(provider);

        // Act
        await observer.WaitForStateChangeAsync(() =>
            manager.SetFlagAsync("ProductA", "Tenant1", "FlagA1", true, "Admin", null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        await observer.WaitForStateChangeAsync(() =>
            manager.SetFlagAsync("ProductB", "Tenant2", "FlagB2", false, "Admin", null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        await observer.WaitForStateChangeAsync(() =>
            manager.SetFlagAsync("ProductA", "Tenant1", "SharedFlag", true, "Admin", null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        await observer.WaitForStateChangeAsync(() =>
            manager.SetFlagAsync("ProductA", "Tenant2", "SharedFlag", false, "Admin", null, IntentDeliveryMode.Acknowledged, CancellationToken.None));

        // Assert - Individual documents exist per product & tenant
        var docId1 = Constants.GetDocumentId("ProductA", "Tenant1");
        var docId2 = Constants.GetDocumentId("ProductB", "Tenant2");
        var docId3 = Constants.GetDocumentId("ProductA", "Tenant2");

        (await orchestrator.DocumentExistsAsync<FeatureFlagState>(docId1, CancellationToken.None)).ShouldBeTrue();
        (await orchestrator.DocumentExistsAsync<FeatureFlagState>(docId2, CancellationToken.None)).ShouldBeTrue();
        (await orchestrator.DocumentExistsAsync<FeatureFlagState>(docId3, CancellationToken.None)).ShouldBeTrue();

        var productATenant1Flags = await manager.GetFlagsAsync("ProductA", "Tenant1", CancellationToken.None);
        productATenant1Flags.Count.ShouldBe(2);
        productATenant1Flags.ShouldContainKey("FlagA1");
        productATenant1Flags.ShouldContainKey("SharedFlag");
        productATenant1Flags["SharedFlag"].IsEnabled.ShouldBeTrue();

        var productATenant2Flags = await manager.GetFlagsAsync("ProductA", "Tenant2", CancellationToken.None);
        productATenant2Flags.Count.ShouldBe(1);
        productATenant2Flags.ShouldContainKey("SharedFlag");
        productATenant2Flags["SharedFlag"].IsEnabled.ShouldBeFalse();

        var productBFlags = await manager.GetFlagsAsync("ProductB", "Tenant2", CancellationToken.None);
        productBFlags.Count.ShouldBe(1);
        productBFlags.ShouldContainKey("FlagB2");

        // All flags query aggregates across all product/tenant documents without colliding on flag name
        var allFlags = await manager.GetAllFlagsAsync(CancellationToken.None);
        allFlags.Count.ShouldBe(4);
        allFlags.ShouldContainKey("ProductA:Tenant1:FlagA1");
        allFlags.ShouldContainKey("ProductB:Tenant2:FlagB2");
        allFlags.ShouldContainKey("ProductA:Tenant1:SharedFlag");
        allFlags.ShouldContainKey("ProductA:Tenant2:SharedFlag");
        allFlags["ProductA:Tenant1:SharedFlag"].IsEnabled.ShouldBeTrue();
        allFlags["ProductA:Tenant2:SharedFlag"].IsEnabled.ShouldBeFalse();

        // Targeted removal from a specific product/tenant shard
        await observer.WaitForStateChangeAsync(() =>
            manager.RemoveFlagAsync("ProductA", "Tenant1", "FlagA1", IntentDeliveryMode.Acknowledged, CancellationToken.None));

        var productAFlagsAfter = await manager.GetFlagsAsync("ProductA", "Tenant1", CancellationToken.None);
        productAFlagsAfter.ShouldNotContainKey("FlagA1");
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
        services.AddDistributedCrdtCore(x => {
            x.ActiveSyncEnabled = true;
        });
        services.AddDistributedCrdtReplica("integration-replica-1", "integration-mesh");

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