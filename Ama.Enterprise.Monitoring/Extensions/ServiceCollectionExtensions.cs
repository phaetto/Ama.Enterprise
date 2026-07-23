namespace Ama.Enterprise.Monitoring.Extensions;

using System;
using Ama.CRDT.Extensions;
using Ama.Enterprise.Monitoring.Models;
using Ama.Enterprise.Monitoring.Services;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Telemetry.Extensions;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Extensions;
using Ama.Enterprise.P2p.WebRTC.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

/// <summary>
/// Service collection extensions for configuring the out-of-the-box WebRTC monitoring mesh architectures.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers a complete WebRTC-based telemetry monitoring mesh acting as a central host aggregator.
    /// </summary>
    /// <param name="services">Target IServiceCollection container evaluating parameters implicitly.</param>
    /// <param name="configure">Explicit action configuring underlying monitoring behaviors natively.</param>
    /// <returns>Transitive IServiceCollection ensuring cascaded registrations.</returns>
    public static IServiceCollection AddMonitoringHostMesh(this IServiceCollection services, Action<MonitoringOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MonitoringOptions();
        configure.Invoke(options);

        services.AddP2pTelemetryForwarder(fwdOptions => {
            fwdOptions.TargetMeshId = Constants.DefaultMonitoringMeshId;
            fwdOptions.FlushInterval = TimeSpan.FromSeconds(1);
            // TODO: setup also fwdOptions.IncludedMeterNames on options
        });

        return services.AddMonitoringMeshInternal(options);
    }

    /// <summary>
    /// Registers a complete WebRTC-based telemetry monitoring mesh acting as a forwarding client.
    /// </summary>
    /// <param name="services">Target IServiceCollection container evaluating parameters implicitly.</param>
    /// <param name="configure">Explicit action configuring underlying monitoring behaviors natively.</param>
    /// <returns>Transitive IServiceCollection ensuring cascaded registrations.</returns>
    public static IServiceCollection AddMonitoringClientMesh(this IServiceCollection services, Action<MonitoringOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MonitoringOptions();
        configure.Invoke(options);

        services.AddP2pTelemetryAggregator(Constants.DefaultMonitoringMeshId);

        if (!string.IsNullOrWhiteSpace(options.ParentSignalingUrl))
        {
            services.AddHostedService(sp => new WebRtcMonitoringBootstrapper(
                sp,
                sp.GetRequiredService<ILogger<WebRtcMonitoringBootstrapper>>(),
                Constants.DefaultMonitoringMeshId,
                options.ParentSignalingUrl));
        }

        return services.AddMonitoringMeshInternal(options);
    }

    private static IServiceCollection AddMonitoringMeshInternal(this IServiceCollection services, MonitoringOptions options)
    {
        var p2pBuilder = services
            .AddCrdt()
            .AddP2pMesh(Constants.DefaultMonitoringMeshId)
            .ConfigureFailureDetector(fdOptions =>
            {
                fdOptions.HeartbeatInterval = options.HeartbeatInterval;
            })
            .AddWebRtcTransport(rtcOptions =>
            {
                rtcOptions.IceServers = options.IceServers;
                rtcOptions.IceGatheringTimeout = options.IceGatheringTimeout;
            })
            .AddAspNetCoreWebRtcSignaling(sigOptions =>
            {
                sigOptions.HostingMode = AspNetCoreHostingMode.Standalone;
                sigOptions.StandaloneListenHost = options.ListenHost;
                sigOptions.StandaloneListenPort = options.ListenPort;
                sigOptions.IgnoreOutboundSslErrors = options.IgnoreOutboundSslErrors;
            })
            .AddWebRtcHttpPeerDiscovery();

        if (options.CertificateBytes is not null && options.CertificateBytes.Length > 0)
        {
            p2pBuilder.AddCertificateAuthenticator(certOptions =>
            {
                certOptions.LocalCertificateBytes = options.CertificateBytes;
                if (!string.IsNullOrWhiteSpace(options.CertificateThumbprint))
                {
                    certOptions.AllowedThumbprints.Add(options.CertificateThumbprint);
                }
                certOptions.ValidateCertificateChain = false;
            });
        }

        if (!string.IsNullOrWhiteSpace(options.EncryptionKeyBase64))
        {
            p2pBuilder.AddWireEncoder(wireOptions =>
            {
                wireOptions.IsEncryptionEnabled = true;
                wireOptions.EncryptionKeyBase64 = options.EncryptionKeyBase64;
            });
        }

        if (options.UseBinarySerialization && options.ConfigureBinarySerialization is not null)
        {
            options.ConfigureBinarySerialization(services);
        }

        return services;
    }
}