namespace Ama.Enterprise.CRDT.Distributed.Services;

using Ama.Enterprise.CRDT.Distributed.Models;
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
internal sealed class DocumentFactory<TState> : IDocumentFactory where TState : class, IDistributedCrdtState, new()
{
    /// <inheritdoc />
    public IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId)
    {
        var state = new TState { Id = documentId };
        return ActivatorUtilities.CreateInstance<DistributedCrdtDocument<TState>>(serviceProvider, state);
    }
}