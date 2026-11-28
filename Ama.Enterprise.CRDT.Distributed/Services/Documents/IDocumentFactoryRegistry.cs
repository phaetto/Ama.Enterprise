namespace Ama.Enterprise.CRDT.Distributed.Services.Documents;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

/// <summary>
/// Singleton registry that maps CRDT document state types and string type aliases to their corresponding <see cref="IDocumentFactory"/> instances.
/// Avoids enumerating all registered factories or rebuilding lookup dictionaries on every dynamic scope creation.
/// </summary>
/// <example>
/// <code>
/// if (registry.TryGetFactory("user-profile", out var factory))
/// {
///     var doc = factory.CreateDocument(scopedProvider, "user-123");
/// }
/// </code>
/// </example>
public interface IDocumentFactoryRegistry
{
    /// <summary>
    /// Attempts to retrieve the primary registered string alias for the specified document state CLR type.
    /// </summary>
    /// <param name="stateType">The document state CLR type.</param>
    /// <param name="primaryAlias">When this method returns, contains the primary string alias if registered; otherwise, <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if an alias was found for <paramref name="stateType"/>; otherwise, <see langword="false"/>.</returns>
    bool TryGetPrimaryAlias(Type stateType, [NotNullWhen(true)] out string? primaryAlias);

    /// <summary>
    /// Attempts to retrieve all registered string aliases for the specified document state CLR type.
    /// </summary>
    /// <param name="stateType">The document state CLR type.</param>
    /// <param name="aliases">When this method returns, contains the read-only list of registered aliases if found; otherwise, <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if aliases were found for <paramref name="stateType"/>; otherwise, <see langword="false"/>.</returns>
    bool TryGetAllAliases(Type stateType, [NotNullWhen(true)] out IReadOnlyList<string>? aliases);

    /// <summary>
    /// Attempts to retrieve the <see cref="IDocumentFactory"/> registered under the specified string type alias.
    /// </summary>
    /// <param name="typeAlias">The string alias identifying the document state type.</param>
    /// <param name="factory">When this method returns, contains the matching factory if found; otherwise, <see langword="null"/>.</param>
    /// <returns><see langword="true"/> if a factory was found for <paramref name="typeAlias"/>; otherwise, <see langword="false"/>.</returns>
    bool TryGetFactory(string typeAlias, [NotNullWhen(true)] out IDocumentFactory? factory);
}