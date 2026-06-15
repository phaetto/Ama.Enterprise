namespace Ama.Enterprise.P2p.Extensions;

using System;
using System.Linq;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Extension methods for registering structural wire encoders enabling optional data-in-transit encryption explicitly.
/// </summary>
public static class WireEncoderServiceCollectionExtensions
{
    /// <summary>
    /// Configures the P2P mesh to format generic wire structures securely enabling optional mapped End-to-End (E2E) AES-GCM data-in-transit encryption safely.
    /// </summary>
    /// <param name="builder">The P2P mesh builder.</param>
    /// <param name="configureOptions">An action to configure the wire encoding properties.</param>
    /// <returns>The P2P mesh builder for chaining.</returns>
    /// <remarks>
    /// SECURITY EXPECTATIONS AND LIMITATIONS:
    /// Enabling wire encryption here adds a "Defense in Depth" application-level AES-GCM encryption layer.
    /// It is NOT a replacement for a comprehensive secure transport protocol.
    /// 
    /// - Static Keys: It utilizes a single, static shared symmetric key (AES-GCM) across the entire mesh.
    /// - No Perfect Forward Secrecy (PFS): Because the key is static, compromise of the key allows historical payload decryption.
    /// - No Replay Protection: It lacks cryptographic nonces or monotonic sequence numbers required to drop duplicated packets.
    /// 
    /// Enterprise Usage:
    /// For enterprise-grade security, you MUST pair this with a secure transport layer such as HTTPS/mTLS or QUIC.
    /// When combined with mTLS, the transport layer handles Replay Protection, Perfect Forward Secrecy, and Identity Binding automatically.
    /// This custom wire encoder then serves as an additional zero-trust boundary, ensuring that payloads remain encrypted 
    /// against infrastructure-level packet inspection or intermediate multi-hop routing intercepts.
    /// </remarks>
    public static IP2pMeshBuilder AddWireEncoder(
        this IP2pMeshBuilder builder,
        Action<WireEncoderOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var tracker = P2pMeshRegistrationTracker.GetOrCreate(builder.Services);
        if (!tracker.TryRegister($"{builder.MeshId}_WireEncoder", configureOptions))
        {
            return builder;
        }

        if (configureOptions is null)
        {
            builder.Services.Configure<WireEncoderOptions>(builder.MeshId, _ => { });
        }
        else
        {
            builder.Services.Configure(builder.MeshId, configureOptions);
        }

        var existingDescriptors = builder.Services
            .Where(d => d.ServiceType == typeof(IMeshWireEncoder) && object.Equals(d.ServiceKey, builder.MeshId))
            .ToList();
            
        foreach (var descriptor in existingDescriptors)
        {
            builder.Services.Remove(descriptor);
        }

        builder.Services.AddKeyedSingleton<IMeshWireEncoder>(builder.MeshId, (sp, key) =>
            new MeshWireEncoder(
                (string)key!,
                sp.GetRequiredService<IOptionsMonitor<WireEncoderOptions>>(),
                sp.GetRequiredService<ICrdtSerializer>(),
                sp.GetRequiredService<ILogger<MeshWireEncoder>>()));

        return builder;
    }
}