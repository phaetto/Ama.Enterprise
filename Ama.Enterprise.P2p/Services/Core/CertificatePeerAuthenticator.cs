namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation of <see cref="IPeerAuthenticator"/> that validates a provided X.509 certificate to filter invalid network requests.
/// </summary>
public sealed class CertificatePeerAuthenticator : IPeerAuthenticator, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<CertificateAuthenticatorOptions> optionsMonitor;
    private readonly ILogger<CertificatePeerAuthenticator> logger;

    private readonly Meter meter;
    private readonly Counter<long> authenticationsCounter;
    private readonly Counter<long> rejectionsCounter;

    /// <summary>
    /// Initializes a new instance of the <see cref="CertificatePeerAuthenticator"/> class.
    /// </summary>
    public CertificatePeerAuthenticator(
        string meshId, 
        IOptionsMonitor<CertificateAuthenticatorOptions> optionsMonitor,
        ILogger<CertificatePeerAuthenticator> logger, 
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.logger = logger;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.CertificatePeerAuthenticator") ?? new Meter("Ama.Enterprise.P2p.CertificatePeerAuthenticator");
        this.authenticationsCounter = this.meter.CreateCounter<long>("p2p.authenticator.certificate.authentications", "authentications", "Total authentications via certificate strategy");
        this.rejectionsCounter = this.meter.CreateCounter<long>("p2p.authenticator.certificate.rejections", "rejections", "Total rejections via certificate strategy");
    }

    /// <inheritdoc />
    public Task<ReadOnlyMemory<byte>> GetLocalHandshakeDataAsync(CancellationToken cancellationToken)
    {
        var options = optionsMonitor.Get(meshId);
        if (options.LocalCertificateBytes is { Length: > 0 } bytes)
        {
            return Task.FromResult<ReadOnlyMemory<byte>>(bytes);
        }

        return Task.FromResult(ReadOnlyMemory<byte>.Empty);
    }

    /// <inheritdoc />
    public Task<bool> AuthenticateAsync(PeerNode node, ReadOnlyMemory<byte> handshakeData, CancellationToken cancellationToken)
    {
        if (node.Id.Value == Guid.Empty)
        {
            throw new ArgumentException("Peer ID cannot be empty.", nameof(node));
        }

        if (handshakeData.IsEmpty)
        {
            logger.LogWarning("[{MeshId}] Peer {PeerId} provided empty handshake data. Authentication failed.", meshId, node.Id.Value);
            RecordRejection();
            return Task.FromResult(false);
        }

        var options = optionsMonitor.Get(meshId);

        try
        {
            using var certificate = X509CertificateLoader.LoadCertificate(handshakeData.Span);
            var now = DateTime.UtcNow;

            if (now < certificate.NotBefore || now > certificate.NotAfter)
            {
                logger.LogWarning("[{MeshId}] Peer {PeerId} provided an expired or not yet valid certificate. Authentication failed.", meshId, node.Id.Value);
                RecordRejection();
                return Task.FromResult(false);
            }

            if (options.AllowedThumbprints.Count > 0)
            {
                if (!options.AllowedThumbprints.Contains(certificate.Thumbprint))
                {
                    logger.LogWarning("[{MeshId}] Peer {PeerId} provided a certificate with an unapproved thumbprint ({Thumbprint}). Authentication failed.", meshId, node.Id.Value, certificate.Thumbprint);
                    RecordRejection();
                    return Task.FromResult(false);
                }
            }

            if (options.ValidateCertificateChain)
            {
                using var chain = new X509Chain();
                chain.ChainPolicy.RevocationMode = options.RevocationMode;
                
                if (options.AllowUnknownCertificateAuthorities)
                {
                    chain.ChainPolicy.VerificationFlags = X509VerificationFlags.AllowUnknownCertificateAuthority;
                }

                if (!chain.Build(certificate))
                {
                    logger.LogWarning("[{MeshId}] Peer {PeerId} provided a certificate that failed chain validation. Authentication failed.", meshId, node.Id.Value);
                    RecordRejection();
                    return Task.FromResult(false);
                }
            }

            authenticationsCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));
            logger.LogDebug("[{MeshId}] Authenticated peer {PeerId} successfully via certificate.", meshId, node.Id.Value);
            
            return Task.FromResult(true);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "[{MeshId}] Failed to process certificate for peer {PeerId}.", meshId, node.Id.Value);
            RecordRejection();
            return Task.FromResult(false);
        }
    }

    private void RecordRejection()
    {
        rejectionsCounter.Add(1, new KeyValuePair<string, object?>("mesh_id", meshId));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        meter.Dispose();
    }
}