namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Linq;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering certificate-based peer authenticators.
/// </summary>
public static class CertificateAuthenticatorServiceCollectionExtensions
{
    /// <summary>
    /// Configures the P2P mesh to authenticate peers using X.509 certificates to filter invalid requests.
    /// </summary>
    /// <param name="builder">The P2P mesh builder.</param>
    /// <param name="configureOptions">An action to configure the certificate authenticator options.</param>
    /// <returns>The P2P mesh builder for chaining.</returns>
    public static IP2pMeshBuilder AddCertificateAuthenticator(
        this IP2pMeshBuilder builder,
        Action<CertificateAuthenticatorOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister($"{builder.MeshId}_CertificateAuthenticator", configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<CertificateAuthenticatorOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        // Remove existing keyed registrations to prevent resolution collisions with defaults
        var existingDescriptors = builder.Services
            .Where(d => d.ServiceType == typeof(IPeerAuthenticator) && object.Equals(d.ServiceKey, builder.MeshId))
            .ToList();
            
        foreach (var descriptor in existingDescriptors)
        {
            builder.Services.Remove(descriptor);
        }

        // Apply the newly configured CertificatePeerAuthenticator
        builder.Services.AddKeyedSingleton<IPeerAuthenticator>(builder.MeshId, (sp, key) =>
            new CertificatePeerAuthenticator(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<CertificateAuthenticatorOptions>>(),
                sp.GetRequiredService<ILogger<CertificatePeerAuthenticator>>(),
                sp.GetService<System.Diagnostics.Metrics.IMeterFactory>()));

        return builder;
    }
}