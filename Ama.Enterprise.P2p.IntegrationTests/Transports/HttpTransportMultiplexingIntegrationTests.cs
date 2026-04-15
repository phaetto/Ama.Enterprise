namespace Ama.Enterprise.P2p.IntegrationTests.Transports;

using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.UnitTests.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Shouldly;
using Ama.Enterprise.P2p.Services.Core;

public sealed class HttpTransportMultiplexingIntegrationTests : IDisposable
{
    private readonly ServiceCollection services;
    private ServiceProvider? serviceProvider;

    public HttpTransportMultiplexingIntegrationTests()
    {
        services = new ServiceCollection();
        
        // Add real CRDT serializer correctly via the framework's native DI registration
        services.AddCrdt();
        
        services.AddLogging(builder => builder.ClearProviders());
    }

    [IntegrationFact]
    public async Task HttpTransportListener_ShouldMultiplexCorrectly_AndIsolatePortsAndVersions()
    {
        // Arrange
        var portA = GetFreePort();
        var portB = GetFreePort();

        services.AddP2pMesh("MeshA", opt => opt.LocalPeerId = Guid.NewGuid())
            .AddGossipNetwork() // Adds necessary AOT JSON Contexts for GossipMessage
            .AddHttpTransport(opt => 
            { 
                opt.ListenPort = portA; 
                opt.ListenHost = "localhost"; 
                opt.PathPrefix = "/p2p/messages/"; 
            });

        services.AddP2pMesh("MeshB", opt => opt.LocalPeerId = Guid.NewGuid())
            .AddGossipNetwork()
            .AddHttpTransport(opt => 
            { 
                opt.ListenPort = portB; 
                opt.ListenHost = "localhost"; 
                opt.PathPrefix = "/p2p/messages/"; 
            });

        serviceProvider = services.BuildServiceProvider();

        var listener = serviceProvider.GetRequiredService<ITransportListener>();
        var serializer = serviceProvider.GetRequiredService<ICrdtSerializer>();
        var receivedMessages = new ConcurrentBag<GossipMessage>();

        await listener.StartListeningAsync(msg => 
        {
            if (msg is GossipMessage gmsg)
            {
                receivedMessages.Add(gmsg);
            }
            return Task.CompletedTask;
        }, CancellationToken.None);

        try
        {
            using var client = new HttpClient();
            
            // 1. Send Valid Message to MeshA
            var msgA = new GossipMessage("MeshA", Guid.NewGuid(), new PeerId(Guid.NewGuid()), 10, new byte[] { 1, 2, 3 });
            var resA = await SendTestMessageAsync(client, portA, msgA, serializer, Constants.ProtocolVersion);
            resA.StatusCode.ShouldBe(HttpStatusCode.Accepted);

            // 2. Send Valid Message to MeshB
            var msgB = new GossipMessage("MeshB", Guid.NewGuid(), new PeerId(Guid.NewGuid()), 10, new byte[] { 4, 5, 6 });
            var resB = await SendTestMessageAsync(client, portB, msgB, serializer, Constants.ProtocolVersion);
            resB.StatusCode.ShouldBe(HttpStatusCode.Accepted);

            // 3. Port Isolation: Send Message targeted for MeshB to MeshA's port
            var resCross = await SendTestMessageAsync(client, portA, msgB, serializer, Constants.ProtocolVersion);
            resCross.StatusCode.ShouldBe(HttpStatusCode.Forbidden); // Port isolation rejection

            // 4. Version Mismatch: Send Message targeted for MeshA to MeshA's port but wrong major version
            var resVersion = await SendTestMessageAsync(client, portA, msgA, serializer, "99.0.0");
            resVersion.StatusCode.ShouldBe(HttpStatusCode.HttpVersionNotSupported);

            // Allow the background queue to drain
            await Task.Delay(250);

            // Assert
            receivedMessages.Count.ShouldBe(2);
            receivedMessages.ShouldContain(m => m.MeshId == "MeshA" && m.MessageId == msgA.MessageId);
            receivedMessages.ShouldContain(m => m.MeshId == "MeshB" && m.MessageId == msgB.MessageId);
        }
        finally
        {
            await listener.StopListeningAsync(CancellationToken.None);
        }
    }

    public void Dispose()
    {
        serviceProvider?.Dispose();
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task<HttpResponseMessage> SendTestMessageAsync(
        HttpClient client, 
        int port, 
        IMeshMessage message, 
        ICrdtSerializer serializer, 
        string protocolVersion)
    {
        // Parameter 'message' acts as IMeshMessage correctly leveraging polymorphic STJ mappings
        var payloadBytes = serializer.SerializeToBytes(message);
        using var content = new ByteArrayContent(payloadBytes);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/octet-stream");
            
        using var request = new HttpRequestMessage(HttpMethod.Post, $"http://localhost:{port}/p2p/messages/")
        {
            Content = content
        };
        request.Headers.Add("X-P2P-Protocol-Version", protocolVersion);
            
        return await client.SendAsync(request);
    }
}