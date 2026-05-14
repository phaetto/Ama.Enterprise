namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Collections.Concurrent;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Tracks registered mesh components to ensure duplicate mesh setups are safely skipped or validated.
/// </summary>
public sealed class P2pMeshRegistrationTracker
{
    private readonly ConcurrentDictionary<string, object> registeredOptions = new();

    /// <summary>
    /// Gets or adds the tracker from the service collection.
    /// </summary>
    /// <param name="services">The service collection to evaluate.</param>
    /// <returns>The active registration tracker.</returns>
    /// <exception cref="ArgumentNullException">Thrown if the services collection is null.</exception>
    public static P2pMeshRegistrationTracker GetOrCreate(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        var descriptor = services.FirstOrDefault(d => d.ServiceType == typeof(P2pMeshRegistrationTracker));
        if (descriptor?.ImplementationInstance is P2pMeshRegistrationTracker existingTracker)
        {
            return existingTracker;
        }

        var newTracker = new P2pMeshRegistrationTracker();
        services.AddSingleton(newTracker);
        return newTracker;
    }

    /// <summary>
    /// Validates if a component with identical options is already registered for the specified mesh identifier.
    /// </summary>
    /// <typeparam name="TOptions">The configuration options type.</typeparam>
    /// <param name="meshId">The mesh identifier.</param>
    /// <param name="configure">The configuration action applied to the options.</param>
    /// <returns>True if uniquely registered, false if already securely registered with identical options.</returns>
    /// <exception cref="ArgumentException">Thrown if the mesh identifier is null or empty.</exception>
    /// <exception cref="InvalidOperationException">Thrown if a distinct setup configuration was previously registered.</exception>
    public bool TryRegister<TOptions>(string meshId, Action<TOptions>? configure) where TOptions : class, IEquatable<TOptions>, new()
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        var options = new TOptions();
        configure?.Invoke(options);

        var key = $"{meshId}_{typeof(TOptions).Name}";

        if (!registeredOptions.TryAdd(key, options))
        {
            var existing = (TOptions)registeredOptions[key];
            if (!existing.Equals(options))
            {
                throw new InvalidOperationException($"P2P Mesh '{meshId}' is already registered with a different {typeof(TOptions).Name} setup.");
            }
            
            return false;
        }

        return true;
    }

    /// <summary>
    /// Validates if a parameterless component is already registered for the specified mesh identifier.
    /// </summary>
    /// <param name="meshId">The mesh identifier.</param>
    /// <returns>True if uniquely registered, false if already securely registered.</returns>
    /// <exception cref="ArgumentException">Thrown if the mesh identifier is null or empty.</exception>
    public bool TryRegister(string meshId)
    {
        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Mesh ID cannot be null or empty.", nameof(meshId));
        }

        var key = $"{meshId}_Parameterless";

        return registeredOptions.TryAdd(key, true);
    }
}