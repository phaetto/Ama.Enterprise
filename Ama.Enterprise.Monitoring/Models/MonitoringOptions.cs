namespace Ama.Enterprise.Monitoring.Models;

using System;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Configuration options for establishing a monitoring mesh.
/// </summary>
public sealed class MonitoringOptions : IEquatable<MonitoringOptions>
{
    /// <summary>
    /// The local host address to bind the out-of-band standalone WebRTC signaling server.
    /// </summary>
    public string ListenHost { get; set; } = "127.0.0.1";

    /// <summary>
    /// The port to bind the out-of-band standalone WebRTC signaling server.
    /// </summary>
    public int ListenPort { get; set; } = 8080;
    
    /// <summary>
    /// The interval between failure detection heartbeats.
    /// </summary>
    public TimeSpan HeartbeatInterval { get; set; } = TimeSpan.FromSeconds(5);
    
    /// <summary>
    /// The timeout to wait for ICE gathering to complete.
    /// </summary>
    public TimeSpan IceGatheringTimeout { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>
    /// An optional parent signaling HTTP endpoint to actively bootstrap and join an existing monitoring cluster.
    /// </summary>
    public string? ParentSignalingUrl { get; set; }

    /// <summary>
    /// Explicit flag mapping configurations to bypass outbound SSL validation for development signaling servers.
    /// </summary>
    public bool IgnoreOutboundSslErrors { get; set; }

    /// <summary>
    /// A generic array of explicit ICE servers required for WebRTC NAT traversal boundaries.
    /// </summary>
    public string[] IceServers { get; set; } = Array.Empty<string>();

    /// <summary>
    /// Optional byte array containing the raw exported certificate data.
    /// </summary>
    public byte[]? CertificateBytes { get; set; }

    /// <summary>
    /// Optional thumbprint of the allowed certificate.
    /// </summary>
    public string? CertificateThumbprint { get; set; }

    /// <summary>
    /// Optional Base64 encoded symmetric key used for E2E wire encryption.
    /// </summary>
    public string? EncryptionKeyBase64 { get; set; }

    /// <summary>
    /// Explicit flag mapping generic intent to enable binary serialization mapping behaviors explicitly.
    /// </summary>
    public bool UseBinarySerialization { get; set; }

    /// <summary>
    /// Action to configure the MessagePack serialization pipeline natively, accepting the underlying generic DI container.
    /// </summary>
    public Action<IServiceCollection>? ConfigureBinarySerialization { get; set; }

    /// <summary>
    /// Evaluates structural equality against another options instance bounding explicit properties.
    /// </summary>
    /// <param name="other">The other instance to evaluate.</param>
    /// <returns>True if the structures match explicitly.</returns>
    public bool Equals(MonitoringOptions? other)
    {
        if (other is null)
        {
            return false;
        }

        if (ReferenceEquals(this, other))
        {
            return true;
        }

        var certsEqual = (CertificateBytes == null && other.CertificateBytes == null) ||
                         (CertificateBytes != null && other.CertificateBytes != null && CertificateBytes.SequenceEqual(other.CertificateBytes));

        return string.Equals(ListenHost, other.ListenHost, StringComparison.Ordinal) &&
               ListenPort == other.ListenPort &&
               HeartbeatInterval == other.HeartbeatInterval &&
               IceGatheringTimeout == other.IceGatheringTimeout &&
               string.Equals(ParentSignalingUrl, other.ParentSignalingUrl, StringComparison.Ordinal) &&
               IgnoreOutboundSslErrors == other.IgnoreOutboundSslErrors &&
               IceServers.SequenceEqual(other.IceServers) &&
               string.Equals(CertificateThumbprint, other.CertificateThumbprint, StringComparison.OrdinalIgnoreCase) &&
               string.Equals(EncryptionKeyBase64, other.EncryptionKeyBase64, StringComparison.Ordinal) &&
               UseBinarySerialization == other.UseBinarySerialization &&
               certsEqual;
    }

    /// <summary>
    /// Evaluates generic object equality explicitly mapping structural bounds.
    /// </summary>
    public override bool Equals(object? obj)
    {
        return Equals(obj as MonitoringOptions);
    }

    /// <summary>
    /// Generates a hash code traversing generic bounds and mapped properties structurally.
    /// </summary>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(ListenHost);
        hash.Add(ListenPort);
        hash.Add(HeartbeatInterval);
        hash.Add(IceGatheringTimeout);
        hash.Add(ParentSignalingUrl);
        hash.Add(IgnoreOutboundSslErrors);
        hash.Add(CertificateThumbprint, StringComparer.OrdinalIgnoreCase);
        hash.Add(EncryptionKeyBase64);
        hash.Add(UseBinarySerialization);

        foreach (var server in IceServers)
        {
            hash.Add(server);
        }

        if (CertificateBytes != null)
        {
            foreach (var b in CertificateBytes)
            {
                hash.Add(b);
            }
        }

        return hash.ToHashCode();
    }
}