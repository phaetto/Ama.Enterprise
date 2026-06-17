namespace Ama.Enterprise.P2p.Models.Transports;

using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

/// <summary>
/// Configuration options explicitly bound for configuring active QUIC transport connectivity.
/// </summary>
public sealed class QuicTransportOptions : IEquatable<QuicTransportOptions>
{
    /// <summary>
    /// Gets or sets the target listening hostname or IP address mapping. 
    /// Explicitly supports '+' to bind across all available local interfaces natively.
    /// </summary>
    public string ListenHost { get; set; } = "+";
    
    /// <summary>
    /// Gets or sets the local listening port bounding incoming decentralized P2P payload allocations.
    /// </summary>
    public int ListenPort { get; set; } = 0;
    
    /// <summary>
    /// Maximum structural length constraints preventing unbounded malicious payload streams dynamically. Default is 10MB.
    /// </summary>
    public int MaxMessageSize { get; set; } = 10 * 1024 * 1024;

    /// <summary>
    /// The mandatory X509 Certificate used to secure the QUIC TLS 1.3 listener bounds natively.
    /// </summary>
    public X509Certificate2? ServerCertificate { get; set; }

    /// <summary>
    /// Optional callback to explicitly validate remote certificates during outbound connections.
    /// Useful for trusting self-signed certificates in closed meshes.
    /// </summary>
    public RemoteCertificateValidationCallback? RemoteCertificateValidationCallback { get; set; }

    /// <summary>
    /// Application-Layer Protocol Negotiation (ALPN) identifier mapped for this QUIC mesh.
    /// </summary>
    public string AlpnProtocol { get; set; } = "ama-p2p-quic";

    /// <inheritdoc />
    public bool Equals(QuicTransportOptions? other)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;
        
        return ListenHost == other.ListenHost &&
               ListenPort == other.ListenPort &&
               MaxMessageSize == other.MaxMessageSize &&
               AlpnProtocol == other.AlpnProtocol &&
               Equals(ServerCertificate, other.ServerCertificate) &&
               Equals(RemoteCertificateValidationCallback, other.RemoteCertificateValidationCallback);
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => Equals(obj as QuicTransportOptions);

    /// <inheritdoc />
    public override int GetHashCode() => HashCode.Combine(ListenHost, ListenPort, MaxMessageSize, AlpnProtocol);
}