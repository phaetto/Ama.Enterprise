namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Ama.CRDT.Services.Pooling;

/// <summary>
/// Default singleton implementation of <see cref="IDocumentFactoryRegistry"/> built once from all registered <see cref="IDocumentFactory"/> instances.
/// </summary>
public sealed class DocumentFactoryRegistry : IDocumentFactoryRegistry
{
    private readonly Dictionary<Type, string> primaryAliasesByType = new();
    private readonly Dictionary<Type, string[]> allAliasesByType = new();
    private readonly Dictionary<string, IDocumentFactory> factoriesByAlias = new(StringComparer.Ordinal);

    /// <summary>
    /// Initializes a new instance of the <see cref="DocumentFactoryRegistry"/> class.
    /// </summary>
    /// <param name="factories">The collection of registered document factories.</param>
    /// <param name="stringPool">Optional string pool for interning type aliases.</param>
    public DocumentFactoryRegistry(IEnumerable<IDocumentFactory> factories, ICrdtStringPool? stringPool = null)
    {
        ArgumentNullException.ThrowIfNull(factories);

        var pool = stringPool ?? CrdtStringPool.Shared;
        var tempAllAliases = new Dictionary<Type, List<string>>();

        foreach (var factory in factories)
        {
            if (factory is null || string.IsNullOrWhiteSpace(factory.TypeAlias))
            {
                continue;
            }

            var pooledAlias = pool.GetOrAdd(factory.TypeAlias);
            factoriesByAlias[pooledAlias] = factory;
            primaryAliasesByType.TryAdd(factory.StateType, pooledAlias);

            if (!tempAllAliases.TryGetValue(factory.StateType, out var aliasList))
            {
                aliasList = new List<string>(1);
                tempAllAliases[factory.StateType] = aliasList;
            }

            if (!aliasList.Contains(pooledAlias))
            {
                aliasList.Add(pooledAlias);
            }
        }

        foreach (var kvp in tempAllAliases)
        {
            allAliasesByType[kvp.Key] = kvp.Value.ToArray();
        }
    }

    /// <inheritdoc />
    public bool TryGetPrimaryAlias(Type stateType, [NotNullWhen(true)] out string? primaryAlias)
    {
        ArgumentNullException.ThrowIfNull(stateType);
        return primaryAliasesByType.TryGetValue(stateType, out primaryAlias);
    }

    /// <inheritdoc />
    public bool TryGetAllAliases(Type stateType, [NotNullWhen(true)] out IReadOnlyList<string>? aliases)
    {
        ArgumentNullException.ThrowIfNull(stateType);
        if (allAliasesByType.TryGetValue(stateType, out var array))
        {
            aliases = array;
            return true;
        }

        aliases = null;
        return false;
    }

    /// <inheritdoc />
    public bool TryGetFactory(string typeAlias, [NotNullWhen(true)] out IDocumentFactory? factory)
    {
        if (string.IsNullOrWhiteSpace(typeAlias))
        {
            factory = null;
            return false;
        }

        return factoriesByAlias.TryGetValue(typeAlias, out factory);
    }
}