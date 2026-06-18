namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

/// <summary>
/// Provides dependency injection extension methods to configure token-based session authentication in the P2P mesh.
/// </summary>
/// <example>
/// <code>
/// builder.Services.AddP2pMesh("EnterpriseMesh")
///     .AddSessionAuthentication&lt;CustomJwtValidator&gt;(options =&gt; 
///     {
///         options.RequireSignedTokens = true;
///     });
/// </code>
/// </example>
public static class SessionAuthenticationServiceCollectionExtensions
{
    /// <summary>
    /// Configures the mesh to use token-based authentication (such as JWTs) instead of mutual TLS certificates.
    /// </summary>
    /// <typeparam name="TValidator">The token validator implementation to use for resolving credentials.</typeparam>
    /// <param name="builder">The dependency injection builder for the P2P mesh.</param>
    /// <param name="configureOptions">An optional action to configure session behaviors.</param>
    /// <returns>The modified mesh builder to allow method chaining.</returns>
    public static IP2pMeshBuilder AddSessionAuthentication<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TValidator>(
        this IP2pMeshBuilder builder, 
        Action<SessionAuthenticatorOptions>? configureOptions = null)
        where TValidator : class, ISessionTokenValidator
    {
        ArgumentNullException.ThrowIfNull(builder);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        
        if (configureOptions is not null)
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        builder.Services.TryAddSingleton<IPeerSessionRegistry, InMemoryPeerSessionRegistry>();
        builder.Services.TryAddKeyedSingleton<ISessionTokenValidator, TValidator>(builder.MeshId);

        var existingAuth = builder.Services.FirstOrDefault(s => s.ServiceType == typeof(IPeerAuthenticator) && Equals(s.ServiceKey, builder.MeshId));
        if (existingAuth is not null)
        {
            builder.Services.Remove(existingAuth);
        }

        builder.Services.AddKeyedSingleton<IPeerAuthenticator>(builder.MeshId, (sp, key) =>
            new SessionPeerAuthenticator(
                (string)key!,
                sp.GetRequiredKeyedService<ISessionTokenValidator>(key),
                sp.GetRequiredService<IPeerSessionRegistry>(),
                sp.GetRequiredService<ILogger<SessionPeerAuthenticator>>()));

        return builder;
    }
}