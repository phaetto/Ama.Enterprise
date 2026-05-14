namespace Ama.Enterprise.P2p.WebRTC.AspNetCore.Services;

using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.AspNetCore.Models;
using Ama.Enterprise.P2p.WebRTC.Models;
using Ama.Enterprise.P2p.WebRTC.Services;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation managing isolated ASP.NET Core HTTP out-of-band WebRTC signaling streams evaluating Integrated and Standalone modes explicitly.
/// </summary>
public sealed class WebRtcSignalingServer : IHostedService, IDisposable
{
    private readonly string meshId;
    private readonly IOptionsMonitor<WebRtcSignalingOptions> optionsMonitor;
    private readonly IWebRtcInvitationService invitationService;
    private readonly ICrdtSerializer serializer;
    private readonly ILogger<WebRtcSignalingServer> logger;

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

        this.meter = meterFactory?.Create("Ama.Enterprise.P2p.WebRtcSignalingServer") ?? new Meter("Ama.Enterprise.P2p.WebRtcSignalingServer");
        this.requestsReceivedCounter = this.meter.CreateCounter<long>(
            "p2p.webrtc.signaling.server.requests_received", 
            "requests", 
            "Total mapped explicit inbound WebRTC signaling requests safely orchestrated natively");
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
                        if (string.IsNullOrWhiteSpace(options.StandaloneListenHost) || options.StandaloneListenHost == "+" || options.StandaloneListenHost == "0.0.0.0")
                        {
                            serverOptions.ListenAnyIP(options.StandaloneListenPort);
                        }
                        else if (options.StandaloneListenHost.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                        {
                            serverOptions.ListenLocalhost(options.StandaloneListenPort);
                        }
                        else if (IPAddress.TryParse(options.StandaloneListenHost, out var ipAddress))
                        {
                            serverOptions.Listen(ipAddress, options.StandaloneListenPort);
                        }
                        else
                        {
                            logger.LogWarning("[{MeshId}] Invalid Kestrel ListenHost '{Host}', falling back to Any IP explicitly.", meshId, options.StandaloneListenHost);
                            serverOptions.ListenAnyIP(options.StandaloneListenPort);
                        }
                    });

                    webBuilder.Configure(app =>
                    {
                        var basePath = string.IsNullOrWhiteSpace(options.PathPrefix) ? "/ama-enterprise/webrtc-signaling" : options.PathPrefix.TrimEnd('/');
                        if (!basePath.StartsWith("/", StringComparison.Ordinal)) basePath = "/" + basePath;
                        
                        var offerPath = $"{basePath}/{meshId}/offer";
                        var answerPath = $"{basePath}/{meshId}/answer";
                        var finalizePath = $"{basePath}/{meshId}/finalize";

                        app.Run(async context =>
                        {
                            if (context.Request.Method != HttpMethods.Post)
                            {
                                context.Response.StatusCode = StatusCodes.Status404NotFound;
                                return;
                            }

                            if (context.Request.Path == offerPath)
                            {
                                await HandleOfferAsync(context).ConfigureAwait(false);
                            }
                            else if (context.Request.Path == answerPath)
                            {
                                await HandleAnswerAsync(context).ConfigureAwait(false);
                            }
                            else if (context.Request.Path == finalizePath)
                            {
                                await HandleFinalizeAsync(context).ConfigureAwait(false);
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
            logger.LogInformation("[{MeshId}] ASP.NET Core Standalone WebRTC Signaling started actively evaluating traffic explicitly on {Host}:{Port}", meshId, options.StandaloneListenHost, options.StandaloneListenPort);
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

    private async Task HandleOfferAsync(HttpContext context)
    {
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "offer") };
        requestsReceivedCounter.Add(1, tags);

        try
        {
            var offer = await invitationService.CreateInvitationAsync(context.RequestAborted).ConfigureAwait(false);
            var responseBytes = serializer.SerializeToBytes(offer);
            
            payloadOutHistogram.Record(responseBytes.Length, tags);

            context.Response.StatusCode = StatusCodes.Status200OK;
            context.Response.ContentType = "application/json";
            context.Response.ContentLength = responseBytes.Length;
            await context.Response.Body.WriteAsync(responseBytes, context.RequestAborted).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to generate explicit WebRTC invitation.", meshId);
            context.Response.StatusCode = StatusCodes.Status500InternalServerError;
        }
    }

    private async Task HandleAnswerAsync(HttpContext context)
    {
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "answer") };
        requestsReceivedCounter.Add(1, tags);

        try
        {
            using var ms = new MemoryStream();
            await context.Request.Body.CopyToAsync(ms, context.RequestAborted).ConfigureAwait(false);
            
            payloadInHistogram.Record(ms.Length, tags);
            var offer = serializer.DeserializeFromBytes<WebRtcInvitationOffer>(ms.ToArray());

            var answer = await invitationService.AcceptInvitationAsync(offer.SdpOffer, context.RequestAborted).ConfigureAwait(false);
            var responseBytes = serializer.SerializeToBytes(answer);

            payloadOutHistogram.Record(responseBytes.Length, tags);

            context.Response.StatusCode = StatusCodes.Status200OK;
            context.Response.ContentType = "application/json";
            context.Response.ContentLength = responseBytes.Length;
            await context.Response.Body.WriteAsync(responseBytes, context.RequestAborted).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to accept inbound WebRTC SDP offer.", meshId);
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
        }
    }

    private async Task HandleFinalizeAsync(HttpContext context)
    {
        var tags = new KeyValuePair<string, object?>[] { new("mesh_id", meshId), new("action", "finalize") };
        requestsReceivedCounter.Add(1, tags);

        try
        {
            using var ms = new MemoryStream();
            await context.Request.Body.CopyToAsync(ms, context.RequestAborted).ConfigureAwait(false);
            
            payloadInHistogram.Record(ms.Length, tags);
            var answer = serializer.DeserializeFromBytes<WebRtcInvitationAnswer>(ms.ToArray());

            await invitationService.FinalizeInvitationAsync(answer.ConnectionId, answer.SdpAnswer, context.RequestAborted).ConfigureAwait(false);
            context.Response.StatusCode = StatusCodes.Status202Accepted;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "[{MeshId}] Failed to finalize explicit WebRTC invitation.", meshId);
            context.Response.StatusCode = StatusCodes.Status400BadRequest;
        }
    }

    public void Dispose()
    {
        if (isDisposed) return;
        webHost?.Dispose();
        meter.Dispose();
        isDisposed = true;
    }
}