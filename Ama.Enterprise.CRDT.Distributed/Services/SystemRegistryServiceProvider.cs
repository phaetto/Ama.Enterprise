namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using Ama.CRDT.Services.Providers;

/// <summary>
/// Service provider wrapper that decorates <see cref="IShardIdProvider"/> and <see cref="IDocumentIdProvider"/>
/// with <see cref="SystemRegistryShardIdProviderDecorator"/> and <see cref="SystemRegistryDocumentIdProviderDecorator"/>
/// even if custom providers were registered in the <see cref="Microsoft.Extensions.DependencyInjection.IServiceCollection"/>
/// after <c>AddDistributedCrdtCore()</c>.
/// </summary>
public sealed class SystemRegistryServiceProvider : IServiceProvider
{
    private readonly IServiceProvider inner;
    private IShardIdProvider? decoratedShardIdProvider;
    private IDocumentIdProvider? decoratedDocumentIdProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="SystemRegistryServiceProvider"/> class.
    /// </summary>
    /// <param name="inner">The underlying service provider.</param>
    public SystemRegistryServiceProvider(IServiceProvider inner)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <inheritdoc />
    public object? GetService(Type serviceType)
    {
        if (serviceType == typeof(IServiceProvider))
        {
            return this;
        }

        if (serviceType == typeof(IShardIdProvider))
        {
            if (decoratedShardIdProvider != null)
            {
                return decoratedShardIdProvider;
            }

            var resolved = inner.GetService(typeof(IShardIdProvider)) as IShardIdProvider;
            if (resolved == null)
            {
                return null;
            }

            if (resolved is SystemRegistryShardIdProviderDecorator existingShardDecorator)
            {
                existingShardDecorator.EnsureInnerResolved(inner);
                decoratedShardIdProvider = existingShardDecorator;
            }
            else
            {
                decoratedShardIdProvider = new SystemRegistryShardIdProviderDecorator(resolved);
            }

            return decoratedShardIdProvider;
        }

        if (serviceType == typeof(IDocumentIdProvider))
        {
            if (decoratedDocumentIdProvider != null)
            {
                return decoratedDocumentIdProvider;
            }

            var resolved = inner.GetService(typeof(IDocumentIdProvider)) as IDocumentIdProvider;
            if (resolved == null)
            {
                return null;
            }

            if (resolved is SystemRegistryDocumentIdProviderDecorator existingDocDecorator)
            {
                existingDocDecorator.EnsureInnerResolved(inner);
                decoratedDocumentIdProvider = existingDocDecorator;
            }
            else
            {
                decoratedDocumentIdProvider = new SystemRegistryDocumentIdProviderDecorator(resolved);
            }

            return decoratedDocumentIdProvider;
        }

        return inner.GetService(serviceType);
    }
}