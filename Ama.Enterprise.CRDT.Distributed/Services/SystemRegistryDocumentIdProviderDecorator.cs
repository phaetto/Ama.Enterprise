namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Decorator for <see cref="IDocumentIdProvider"/> that handles extracting, setting, and instantiating
/// the internal <see cref="CrdtRegistryState"/> document without requiring custom user implementations
/// to know about internal system models.
/// </summary>
public sealed class SystemRegistryDocumentIdProviderDecorator : IDocumentIdProvider
{
    private readonly IDocumentIdProvider inner;

    /// <summary>
    /// Initializes a new instance of the <see cref="SystemRegistryDocumentIdProviderDecorator"/> class.
    /// </summary>
    /// <param name="inner">The underlying document ID provider.</param>
    public SystemRegistryDocumentIdProviderDecorator(IDocumentIdProvider inner)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <inheritdoc />
    public IComparable GetDocumentId<T>(T? obj)
    {
        ArgumentNullException.ThrowIfNull(obj);

        if (obj is CrdtRegistryState registryState)
        {
            return string.IsNullOrEmpty(registryState.Id)
                ? Constants.SystemRegistryDocumentId
                : registryState.Id;
        }

        return inner.GetDocumentId(obj);
    }

    /// <inheritdoc />
    public void SetDocumentId<T>(T obj, IComparable id)
    {
        ArgumentNullException.ThrowIfNull(obj);
        ArgumentNullException.ThrowIfNull(id);

        if (obj is CrdtRegistryState registryState)
        {
            registryState.Id = id.ToString() ?? Constants.SystemRegistryDocumentId;
            return;
        }

        inner.SetDocumentId(obj, id);
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

        return inner.CreateDocumentWithId<T>(id);
    }
}