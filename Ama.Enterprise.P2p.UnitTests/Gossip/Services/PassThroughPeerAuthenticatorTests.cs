namespace Ama.Enterprise.P2p.UnitTests.Gossip.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

public sealed class PassThroughPeerAuthenticatorTests
{
    private const string TestMeshId = "TestMesh";
    private readonly PassThroughPeerAuthenticator authenticator;

    public PassThroughPeerAuthenticatorTests()
    {
        var loggerMock = new Mock<ILogger<PassThroughPeerAuthenticator>>();
        authenticator = new PassThroughPeerAuthenticator(TestMeshId, loggerMock.Object);
    }

    [Fact]
    public async Task AuthenticateAsync_ValidPeerId_ShouldReturnTrue()
    {
        // Arrange
        var node = new PeerNode(new PeerId(Guid.NewGuid()), new HttpPeerEndpoint("127.0.0.1", 5000));
        var handshakeData = new byte[] { 1, 2, 3 }.AsMemory();

        // Act
        var result = await authenticator.AuthenticateAsync(node, handshakeData, CancellationToken.None);

        // Assert
        result.ShouldBeTrue();
    }

    [Fact]
    public async Task AuthenticateAsync_EmptyPeerId_ShouldThrowArgumentException()
    {
        // Arrange
        var node = new PeerNode(new PeerId(Guid.Empty), new HttpPeerEndpoint("127.0.0.1", 5000));
        
        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(async () => 
            await authenticator.AuthenticateAsync(node, ReadOnlyMemory<byte>.Empty, CancellationToken.None));
    }
}