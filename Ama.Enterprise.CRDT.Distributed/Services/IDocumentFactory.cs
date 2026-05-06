namespace Ama.Enterprise.CRDT.Distributed.Services;

using Ama.CRDT.Services.Providers;
using Microsoft.Extensions.DependencyInjection;
using System;

/// <summary>
/// AOT-friendly generic factory interface for dynamically resolving distributed CRDT instances.
/// </summary>
public interface IDocumentFactory
{
    /// <summary>
    /// Creates an initialized distributed document.
    /// </summary>
    IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId);
}

/// <summary>
/// Strictly typed internal implementation for generating specific document generics.
/// </summary>
internal sealed class DocumentFactory<TState> : IDocumentFactory where TState : class, new()
{
    /// <inheritdoc />
    public IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId)
    {
        var documentIdProvider = serviceProvider.GetRequiredService<IDocumentIdProvider>();
        var state = documentIdProvider.CreateDocumentWithId<TState>(documentId);
        
        return ActivatorUtilities.CreateInstance<DistributedCrdtDocument<TState>>(serviceProvider, state);
    }
}