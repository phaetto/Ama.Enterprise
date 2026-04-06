namespace Ama.Enterprise.FeatureFlags.UnitTests.Extensions;

using System;
using Ama.Enterprise.FeatureFlags.Extensions;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.FeatureFlags.Services.P2p;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shouldly;
using Xunit;

public sealed class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddFeatureFlags_ShouldThrowArgumentNullException_WhenServicesIsNull()
    {
        IServiceCollection services = null!;
        Should.Throw<ArgumentNullException>(() => services.AddFeatureFlags());
    }

    [Fact]
    public void AddFeatureFlags_ShouldRegisterExpectedServices()
    {
        var services = new ServiceCollection();
        
        services.AddFeatureFlags();

        services.Count.ShouldBeGreaterThan(0);
        services.ShouldContain(s => s.ServiceType == typeof(MemoryJournal));
        services.ShouldContain(s => s.ServiceType == typeof(IFeatureFlagClusterManager));
    }

    [Fact]
    public void AddFeatureFlagsP2p_ShouldThrowArgumentNullException_WhenServicesIsNull()
    {
        IServiceCollection services = null!;
        Should.Throw<ArgumentNullException>(() => services.AddFeatureFlagsP2p());
    }

    [Fact]
    public void AddFeatureFlagsP2p_ShouldRegisterExpectedServices()
    {
        var services = new ServiceCollection();
        
        services.AddFeatureFlagsP2p();

        services.ShouldContain(s => s.ServiceType == typeof(IMessageHandler<GossipMessage>));
        services.ShouldContain(s => s.ServiceType == typeof(IHostedService) && s.ImplementationType == typeof(FeatureFlagAntiEntropyService));
    }
}