namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.WebSockets;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.Licensing.Services;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation managing isolated ASP.NET Core WebSocket out-of-band WebRTC signaling streams evaluating Integrated and Standalone modes explicitly.
/// </summary>
public sealed class WebRtcSignalingServer : IHostedService, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<WebRtcSignalingOptions> optionsMonitor;
    private readonly IWebRtcInvitationService invitationService;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcSignalingServer> logger;
    private readonly ICertificateLoader? certificateLoader;

    private readonly Meter meter;
    private readonly Counter<long> requestsReceivedCounter;
    private readonly Histogram<long> payloadOutHistogram;
    private readonly Histogram<long> payloadInHistogram;

    private IHost? webHost;
    private bool isDisposed;

    public WebRtcSignalingServer(
        string meshId,
        IOptionsMonitor<WebRtcSignalingOptions> optionsMonitor,
        IWebRtcInvitationService invitationService,
        ICrdtSerializer serializer,
        ILogger<WebRtcSignalingServer> logger,
        ICertificateLoader? certificateLoader = null,
        IMeterFactory? meterFactory = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(meshId);
        ArgumentNullException.ThrowIfNull(optionsMonitor);
        ArgumentNullException.ThrowIfNull(invitationService);
        ArgumentNullException.ThrowIfNull(serializer);
        ArgumentNullException.ThrowIfNull(logger);

        this.meshId = meshId;
        this.optionsMonitor = optionsMonitor;
        this.invitationService = invitationService;
        this.serializer = serializer;
        this.logger = logger;
        this.certificateLoader = certificateLoader;

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.WebRtcSignalingServer") ?? new Meter("Ama.Enterprise.P2p.WebRtcSignalingServer");
        this.requestsReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.webrtc.signaling.server.requests_received", 
            "requests", 
            "Total mapped explicit inbound WebRTC WebSocket signaling sessions safely orchestrated natively");
        this.payloadOutHistogram = this.meter.CreateHistogram<long>(
            "p2p.webrtc.signaling.server.outbound_bytes", 
            "bytes", 
            "Size of outbound bounds dynamically mapped WebRTC signaling payloads explicitly in bytes");
        this.payloadInHistogram = this.meter.CreateHistogram<long>(
            "p2p.webrtc.signaling.server.inbound_bytes", 
            "bytes", 
            "Size of inbound natively isolated WebRTC signaling payloads explicitly in bytes");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(isDisposed, this);
        var options = optionsMonitor.Get(meshId);

        if (options.HostingMode == AspNetCoreHostingMode.Standalone)
        {
            var builder = new HostBuilder()
                .ConfigureWebHost(webBuilder =>
                {
                    webBuilder.UseKestrel(serverOptions =>
                    {
                        Action<ListenOptions> configureListenOptions = listenOptions =>
                        {
                            if (options.UseHttpsStandalone)
                            {
                                listenOptions.UseHttps(httpsOptions =>
                                {
                                    var cert = LoadCertificate(options);
                                    if (cert is not null)
                                    {
                                        httpsOptions.ServerCertificate = cert;
                                    }
                                });
                            }
                        };

                        if (string.IsNullOrWhiteSpace(options.StandaloneListenHost) || options.StandaloneListenHost == "+" || options.StandaloneListenHost == "0.0.0.0")
                        {
                            serverOptions.ListenAnyIP(options.StandaloneListenPort, configureListenOptions);
                        }
                        else if (options.StandaloneListenHost.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                        {
                            serverOptions.ListenLocalhost(options.StandaloneListenPort, configureListenOptions);
                        }
                        else if (IPAddress.TryParse(options.StandaloneListenHost, out var ipAddress))
                        {
                            serverOptions.Listen(ipAddress, options.StandaloneListenPort, configureListenOptions);
                        }
                        else
                        {
                            logger.LogWarning("[{MeshId}] Invalid Kestrel ListenHost '{Host}', falling back to Any IP explicitly.", meshId, options.StandaloneListenHost);
                            serverOptions.ListenAnyIP(options.StandaloneListenPort, configureListenOptions);
                        }
                    });

                    webBuilder.Configure(app =>
                    {
                        app.UseWebSockets();

                        var basePath = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/ama-enterprise/webrtc-signaling" : options.PathPrefix.TrimEnd('/');
                        if (!basePath.StartsWith("/", StringComparison.Ordinal)) basePath = "/" + basePath;
                        
                        var wsPath = $"{basePath}/{meshId}/ws";

                        app.Run(async context =>
                        {
                            if (context.Request.Path == wsPath)
                            {
                                if (context.WebSockets.IsWebSocketRequest)
                                {
                                    using var ws = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);
                                    await HandleWebSocketSignalingAsync(ws, context.RequestAborted).ConfigureAwait(false);
                                }
                                else
                                {
                                    context.Response.StatusCode = StatusCodes.Status400BadRequest;
                                }
                            }
                            else
                            {
                                context.Response.StatusCode = StatusCodes.Status404NotFound;
                            }
                        });
                    });
                });

            webHost = builder.Build();
            await webHost.StartAsync(cancellationToken).ConfigureAwait(false);
            logger.LogInformation("[{MeshId}] ASP.NET Core Standalone WebRTC Signaling started actively evaluating WS traffic explicitly on {Host}:{Port}", meshId, options.StandaloneListenHost, options.StandaloneListenPort);
        }
        else
        {
            logger.LogInformation("[{MeshId}] ASP.NET Core Integrated WebRTC Signaling bound explicitly. Relying on host pipeline evaluating MapP2pWebRtcSignalingEndpoints().", meshId);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (webHost is not null)
        {
            await webHost.StopAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task HandleWebSocketSignalingAsync(WebSocket webSocket, CancellationToken cancellationToken)
    {
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("transport", "websocket") };
        requestsReceivedCounter.Add(1, tags);

        try
        {
            var (action, reqPayload) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, cancellationToken).ConfigureAwait(false);
            payloadInHistogram.Record(reqPayload.Length, tags);

            if (action == WebRtcSignalingAction.RequestOffer)
            {
                var offer = await invitationService.CreateInvitationAsync(cancellationToken).ConfigureAwait(false);
                var offerPayload = serializer.SerializeToBytes(offer);
                
                payloadOutHistogram.Record(offerPayload.Length, tags);
                await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.Offer, offerPayload, cancellationToken).ConfigureAwait(false);
                
                var (ansAction, ansPayload) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, cancellationToken).ConfigureAwait(false);
                payloadInHistogram.Record(ansPayload.Length, tags);

                if (ansAction == WebRtcSignalingAction.Answer)
                {
                    var answer = serializer.DeserializeFromBytes<WebRtcInvitationAnswer>(ansPayload);
                    await invitationService.FinalizeInvitationAsync(answer.ConnectionId, answer.SdpAnswer, cancellationToken).ConfigureAwait(false);
                    
                    await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.FinalizeAck, Array.Empty<byte>(), cancellationToken).ConfigureAwait(false);
                    
                    if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Negotiation Complete", cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            else if (action == WebRtcSignalingAction.Offer)
            {
                var offer = serializer.DeserializeFromBytes<WebRtcInvitationOffer>(reqPayload);
                var localAnswer = await invitationService.AcceptInvitationAsync(offer.SdpOffer, cancellationToken).ConfigureAwait(false);
                var answerDto = new WebRtcInvitationAnswer(offer.ConnectionId, localAnswer.SdpAnswer);
                var ansPayload = serializer.SerializeToBytes(answerDto);
                
                payloadOutHistogram.Record(ansPayload.Length, tags);
                await WebRtcSignalingWsHelper.SendMessageAsync(webSocket, WebRtcSignalingAction.Answer, ansPayload, cancellationToken).ConfigureAwait(false);
                
                var (ackAction, ackPayload) = await WebRtcSignalingWsHelper.ReceiveMessageAsync(webSocket, cancellationToken).ConfigureAwait(false);
                payloadInHistogram.Record(ackPayload.Length, tags);

                if (ackAction == WebRtcSignalingAction.FinalizeAck)
                {
                    if (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseReceived)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Negotiation Complete", cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            else
            {
                if (webSocket.State == WebSocketState.Open)
                {
                    await webSocket.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "Invalid Action", cancellationToken).ConfigureAwait(false);
                }
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Exception encountered evaluating Standalone WebRTC signaling WebSockets explicitly natively.", meshId);
            if (webSocket.State == WebSocketState.Open)
            {
                await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "Error", cancellationToken).ConfigureAwait(false);
            }
        }
    }

    public void Dispose()
    {
        if (isDisposed) return;
        webHost?.Dispose();
        meter.Dispose();
        isDisposed = true;
    }

    private X509Certificate2? LoadCertificate(WebRtcSignalingOptions options)
    {
        if (certificateLoader == null)
        {
            logger.LogWarning("[{MeshId}] ICertificateLoader is not registered. Cannot configure HTTPS explicitly.", meshId);
            return null;
        }

        if (!string.IsNullOrWhiteSpace(options.CertificateThumbprint))
        {
            var cert = certificateLoader.LoadFromStore(options.CertificateThumbprint);
            if (cert != null) return cert;
        }

        if (!string.IsNullOrWhiteSpace(options.CertificateFilePath))
        {
            var cert = certificateLoader.LoadFromFile(options.CertificateFilePath, options.CertificatePassword);
            if (cert != null) return cert;
        }

        logger.LogWarning("[{MeshId}] Failed to resolve valid X509 certificate configurations explicitly for HTTPS binding.", meshId);
        return null;
    }
}