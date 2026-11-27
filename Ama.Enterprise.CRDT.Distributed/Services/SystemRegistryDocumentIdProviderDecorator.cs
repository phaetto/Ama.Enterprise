namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Decorator for <see cref="IDocumentIdProvider"/> that handles <see cref="CrdtRegistryState"/>
/// document ID resolution and instantiation while delegating all other document types to the inner provider.
/// </summary>
public sealed class SystemRegistryDocumentIdProviderDecorator : IDocumentIdProvider
{
    private readonly ServiceDescriptor? innerDescriptor;
    private IDocumentIdProvider? inner;

    /// <summary>
    /// Initializes a new instance of the <see cref="SystemRegistryDocumentIdProviderDecorator"/> class.
    /// </summary>
    /// <param name="inner">The underlying document ID provider.</param>
    public SystemRegistryDocumentIdProviderDecorator(IDocumentIdProvider inner)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    internal SystemRegistryDocumentIdProviderDecorator(ServiceDescriptor innerDescriptor)
    {
        this.innerDescriptor = innerDescriptor ?? throw new ArgumentNullException(nameof(innerDescriptor));
    }

    [UnconditionalSuppressMessage("Trimming", "IL2067", Justification = "Target types are already registered in the DI container with preserved constructors.")]
    internal void EnsureInnerResolved(IServiceProvider serviceProvider)
    {
        ArgumentNullException.ThrowIfNull(serviceProvider);

        if (Volatile.Read(ref inner) != null || innerDescriptor == null)
        {
            return;
        }

        var resolved = innerDescriptor.ImplementationInstance as IDocumentIdProvider
            ?? (innerDescriptor.ImplementationFactory != null
                ? (IDocumentIdProvider)innerDescriptor.ImplementationFactory(serviceProvider)
                : (IDocumentIdProvider)ActivatorUtilities.CreateInstance(serviceProvider, innerDescriptor.ImplementationType!));

        if (resolved is SystemRegistryDocumentIdProviderDecorator nested)
        {
            nested.EnsureInnerResolved(serviceProvider);
            resolved = nested.inner ?? nested;
        }

        Interlocked.CompareExchange(ref inner, resolved, null);
    }

    /// <inheritdoc />
    public IComparable GetDocumentId<T>(T? obj)
    {
        if (obj is null)
        {
            throw new ArgumentNullException(nameof(obj));
        }

        if (obj is CrdtRegistryState registryState)
        {
            return string.IsNullOrWhiteSpace(registryState.Id)
                ? Constants.SystemRegistryDocumentId
                : registryState.Id;
        }

        return GetResolvedInner().GetDocumentId(obj);
    }

    /// <inheritdoc />
    public void SetDocumentId<T>(T obj, IComparable id)
    {
        if (obj is null)
        {
            throw new ArgumentNullException(nameof(obj));
        }

        ArgumentNullException.ThrowIfNull(id);

        if (obj is CrdtRegistryState registryState)
        {
            registryState.Id = id.ToString() ?? Constants.SystemRegistryDocumentId;
            return;
        }

        GetResolvedInner().SetDocumentId(obj, id);
    }

    /// <inheritdoc />
    public T CreateDocumentWithId<T>(IComparable id)
    {
        ArgumentNullException.ThrowIfNull(id);

        if (typeof(T) == typeof(CrdtRegistryState))
        {
            var state = new CrdtRegistryState
            {
                Id = id.ToString() ?? Constants.SystemRegistryDocumentId
            };
            return (T)(object)state;
        }

        return GetResolvedInner().CreateDocumentWithId<T>(id);
    }

    private IDocumentIdProvider GetResolvedInner()
    {
        return Volatile.Read(ref inner)
            ?? throw new InvalidOperationException("The inner IDocumentIdProvider has not been resolved yet.");
    }
}