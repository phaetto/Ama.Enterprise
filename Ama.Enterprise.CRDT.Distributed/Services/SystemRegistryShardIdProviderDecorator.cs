namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using Ama.CRDT.Models;
using Ama.CRDT.Services.Providers;
using Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Decorator for <see cref="IShardIdProvider"/> that isolates the system document registry
/// into its own dedicated shard (<c>system-document-registry</c>) while delegating all other
/// document types to the inner shard ID provider.
/// </summary>
public sealed class SystemRegistryShardIdProviderDecorator : IShardIdProvider
{
    private readonly IShardIdProvider inner;

    /// <summary>
    /// Initializes a new instance of the <see cref="SystemRegistryShardIdProviderDecorator"/> class.
    /// </summary>
    /// <param name="inner">The underlying shard ID provider.</param>
    public SystemRegistryShardIdProviderDecorator(IShardIdProvider inner)
    {
        this.inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <inheritdoc />
    public string GetShardId<T>(CrdtDocument<T> document) where T : class
    {
        if (typeof(T) == typeof(CrdtRegistryState) || document.Data is CrdtRegistryState)
        {
            return "system-document-registry";
        }

        return inner.GetShardId(document);
    }

    /// <inheritdoc />
    public string GetShardId<T>(IComparable documentId) where T : class
    {
        ArgumentNullException.ThrowIfNull(documentId);

        if (typeof(T) == typeof(CrdtRegistryState) || string.Equals(documentId.ToString(), "system-document-registry", StringComparison.Ordinal))
        {
            return "system-document-registry";
        }

        return inner.GetShardId<T>(documentId);
    }

    /// <inheritdoc />
    public string GetShardId(IComparable documentId, Type documentType)
    {
        ArgumentNullException.ThrowIfNull(documentId);
        ArgumentNullException.ThrowIfNull(documentType);

        if (documentType == typeof(CrdtRegistryState) || string.Equals(documentId.ToString(), "system-document-registry", StringComparison.Ordinal))
        {
            return "system-document-registry";
        }

        return inner.GetShardId(documentId, documentType);
    }
}