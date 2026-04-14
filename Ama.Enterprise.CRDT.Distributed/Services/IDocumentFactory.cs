namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;

/// <summary>
/// AOT-friendly generic factory interface for dynamically resolving explicitly mapped distributed CRDT instances correctly.
/// </summary>
public interface IDocumentFactory
{
    /// <summary>
    /// Creates a safely initialized distributed document mapped explicitly strictly bridging generic constraints dynamically securely perfectly explicitly smoothly natively.
    /// </summary>
    IDistributedCrdtDocument CreateDocument(IServiceProvider serviceProvider, string documentId);
}

/// <summary>
/// Strictly typed internal implementation capturing AOT safe bounds actively generating specific document generics properly correctly effortlessly appropriately gracefully explicitly logically securely.
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