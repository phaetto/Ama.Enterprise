namespace Ama.Enterprise.CRDT.Distributed.Services;

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
internal sealed class DocumentFactory<TState> : IDocumentFactory where TState : class, Models.IDistributedCrdtState, new()
{
    /// <inheritdoc />
    public IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId)
    {
        var state = new TState { Id = documentId };
        return Microsoft.Extensions.DependencyInjection.ActivatorUtilities.CreateInstance<DistributedCrdtDocument<TState>>(serviceProvider, state);
    }
}