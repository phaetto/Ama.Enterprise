namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Models.Gossip;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Services.Transports;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using Shouldly;
using Xunit;

public sealed class HttpTransportTests
{
    private const string TestMeshId = "TestMesh";
    private readonly Mock<IHttpClientFactory> httpClientFactoryMock;
    private readonly Mock<ICrdtSerializer> serializerMock;
    private readonly Mock<IPeerRegistry> peerRegistryMock;
    private readonly Mock<ILogger<HttpTransport>> loggerMock;
    private readonly Mock<HttpMessageHandler> httpMessageHandlerMock;

    public HttpTransportTests()
    {
        httpClientFactoryMock = new Mock<IHttpClientFactory>();
        serializerMock = new Mock<ICrdtSerializer>();
        peerRegistryMock = new Mock<IPeerRegistry>();
        loggerMock = new Mock<ILogger<HttpTransport>>();
        httpMessageHandlerMock = new Mock<HttpMessageHandler>();
    }

    [Fact]
    public async Task SendAsync_ShouldPostSerializedMessageToEndpoint()
    {
        // Arrange
        var transport = new HttpTransport(TestMeshId, httpClientFactoryMock.Object, serializerMock.Object, peerRegistryMock.Object, loggerMock.Object);
        var endpoint = new HttpPeerEndpoint("192.168.1.10", 9000);
        var message = new GossipMessage(Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, new byte[] { 42 });
        var serializedBytes = new byte[] { 0x01, 0x02 };

        serializerMock.Setup(s => s.SerializeToBytes(message)).Returns(serializedBytes);

        httpMessageHandlerMock.Protected()
            .Setup<Task<HttpResponseMessage>>(
                "SendAsync",
                ItExpr.Is<HttpRequestMessage>(req => 
                    req.Method == HttpMethod.Post && 
                    req.RequestUri != null &&
                    req.RequestUri.ToString() == "http://192.168.1.10:9000/p2p/gossip"),
                ItExpr.IsAny<CancellationToken>()
            )
            .ReturnsAsync(new HttpResponseMessage(HttpStatusCode.OK));

        var httpClient = new HttpClient(httpMessageHandlerMock.Object);
        httpClientFactoryMock.Setup(f => f.CreateClient("P2pTransport")).Returns(httpClient);

        // Act
        await transport.SendAsync(endpoint, message, CancellationToken.None);

        // Assert
        httpMessageHandlerMock.Protected().Verify(
            "SendAsync",
            Times.Once(),
            ItExpr.Is<HttpRequestMessage>(req => req.Method == HttpMethod.Post),
            ItExpr.IsAny<CancellationToken>()
        );
    }
    
    [Fact]
    public async Task SendAsync_ShouldThrow_WhenEndpointHostIsEmpty()
    {
        // Arrange
        var transport = new HttpTransport(TestMeshId, httpClientFactoryMock.Object, serializerMock.Object, peerRegistryMock.Object, loggerMock.Object);
        var endpoint = new HttpPeerEndpoint(string.Empty, 9000);
        var message = new GossipMessage(Guid.NewGuid(), new PeerId(Guid.NewGuid()), 5, new byte[] { 42 });

        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(() => transport.SendAsync(endpoint, message, CancellationToken.None));
    }
}