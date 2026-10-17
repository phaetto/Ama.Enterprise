namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using Ama.CRDT.Services.Providers;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Internal service provider wrapper that ensures any resolved <see cref="IShardIdProvider"/>
/// is decorated with <see cref="SystemRegistryShardIdProviderDecorator"/> regardless of service registration order.
/// </summary>
internal sealed class SystemRegistryServiceProvider : IServiceProvider, ISupportRequiredService, IKeyedServiceProvider
{
    private readonly IServiceProvider inner;

    public SystemRegistryServiceProvider(IServiceProvider inner)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public object? GetService(Type serviceType)
    {
        ArgumentNullException.ThrowIfNull(serviceType);

        if (serviceType == typeof(IShardIdProvider))
        {
            var provider = inner.GetService(typeof(IShardIdProvider)) as IShardIdProvider;
            if (provider is null)
            {
                return null;
            }

            return provider as SystemRegistryShardIdProviderDecorator
                ?? new SystemRegistryShardIdProviderDecorator(provider);
        }

        if (serviceType == typeof(IServiceProvider))
        {
            return this;
        }

        return inner.GetService(serviceType);
    }

    public object GetRequiredService(Type serviceType)
    {
        ArgumentNullException.ThrowIfNull(serviceType);

        if (serviceType == typeof(IShardIdProvider))
        {
            var provider = inner.GetRequiredService<IShardIdProvider>();
            return provider as SystemRegistryShardIdProviderDecorator
                ?? new SystemRegistryShardIdProviderDecorator(provider);
        }

        if (serviceType == typeof(IServiceProvider))
        {
            return this;
        }

        if (inner is ISupportRequiredService requiredService)
        {
            return requiredService.GetRequiredService(serviceType);
        }

        return inner.GetRequiredService(serviceType);
    }

    public object? GetKeyedService(Type serviceType, object? serviceKey)
    {
        ArgumentNullException.ThrowIfNull(serviceType);

        if (inner is IKeyedServiceProvider keyedServiceProvider)
        {
            return keyedServiceProvider.GetKeyedService(serviceType, serviceKey);
        }

        return null;
    }

    public object GetRequiredKeyedService(Type serviceType, object? serviceKey)
    {
        ArgumentNullException.ThrowIfNull(serviceType);

        if (inner is IKeyedServiceProvider keyedServiceProvider)
        {
            return keyedServiceProvider.GetRequiredKeyedService(serviceType, serviceKey);
        }

        throw new InvalidOperationException("The underlying service provider does not support keyed services.");
    }
}