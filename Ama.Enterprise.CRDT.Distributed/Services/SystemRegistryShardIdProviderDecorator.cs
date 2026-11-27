namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Decorator for <see cref="IShardIdProvider"/> that isolates the system document registry
/// into its own dedicated shard (<c>system-document-registry</c>) while delegating all other
/// document types to the inner shard ID provider.
/// </summary>
public sealed class SystemRegistryShardIdProviderDecorator : IShardIdProvider
{
    private readonly ServiceDescriptor? innerDescriptor;
    private IShardIdProvider? inner;

    /// <summary>
    /// Initializes a new instance of the <see cref="SystemRegistryShardIdProviderDecorator"/> class.
    /// </summary>
    /// <param name="inner">The underlying shard ID provider.</param>
    public SystemRegistryShardIdProviderDecorator(IShardIdProvider inner)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    internal SystemRegistryShardIdProviderDecorator(ServiceDescriptor innerDescriptor)
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

        var resolved = innerDescriptor.ImplementationInstance as IShardIdProvider
            ?? (innerDescriptor.ImplementationFactory != null
                ? (IShardIdProvider)innerDescriptor.ImplementationFactory(serviceProvider)
                : (IShardIdProvider)ActivatorUtilities.CreateInstance(serviceProvider, innerDescriptor.ImplementationType!));

        if (resolved is SystemRegistryShardIdProviderDecorator nested)
        {
            nested.EnsureInnerResolved(serviceProvider);
            resolved = nested.inner ?? nested;
        }

        Interlocked.CompareExchange(ref inner, resolved, null);
    }

    /// <inheritdoc />
    public string GetShardId<T>(CrdtDocument<T> document) where T : class
    {
        if (typeof(T) == typeof(CrdtRegistryState) || document.Data is CrdtRegistryState)
        {
            return Constants.SystemRegistryDocumentId;
        }

        return GetResolvedInner().GetShardId(document);
    }

    /// <inheritdoc />
    public string GetShardId<T>(IComparable documentId) where T : class
    {
        ArgumentNullException.ThrowIfNull(documentId);

        if (typeof(T) == typeof(CrdtRegistryState) || string.Equals(documentId.ToString(), Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
        {
            return Constants.SystemRegistryDocumentId;
        }

        return GetResolvedInner().GetShardId<T>(documentId);
    }

    /// <inheritdoc />
    public string GetShardId(IComparable documentId, Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(documentType);

        if (documentType == typeof(CrdtRegistryState) ||
            documentType == typeof(CrdtRegistryEntry) ||
            typeof(IDictionary<string, CrdtRegistryEntry>).IsAssignableFrom(documentType) ||
            string.Equals(documentId.ToString(), Constants.SystemRegistryDocumentId, StringComparison.Ordinal))
        {
            return Constants.SystemRegistryDocumentId;
        }

        return GetResolvedInner().GetShardId(documentId, documentType);
    }

    private IShardIdProvider GetResolvedInner()
    {
        return Volatile.Read(ref inner)
            ?? throw new InvalidOperationException("The inner IShardIdProvider has not been resolved yet.");
    }
}