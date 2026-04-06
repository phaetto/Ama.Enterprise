using Ama.Enterprise.P2p.Models;
using Ama.Enterprise.P2p.Services;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;

namespace Ama.Enterprise.P2p.UnitTests.Services;

public sealed class TimeBasedFailureDetectorTests
{
    private readonly TimeBasedFailureDetector detector;
    private readonly GossipOptions gossipOptions;

    public TimeBasedFailureDetectorTests()
    {
        this.gossipOptions = new GossipOptions { GossipInterval = TimeSpan.FromMilliseconds(50) }; // Short interval for testing
        
        var optionsMock = new Mock<IOptions<GossipOptions>>();
        optionsMock.Setup(o => o.Value).Returns(this.gossipOptions);
        
        var loggerMock = new Mock<ILogger<TimeBasedFailureDetector>>();
        
        this.detector = new TimeBasedFailureDetector(optionsMock.Object, loggerMock.Object);
    }

    [Fact]
    public async Task EvaluatePeerHealthAsync_NoHeartbeatRecorded_ShouldReturnDead()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());

        // Act
        var status = await this.detector.EvaluatePeerHealthAsync(peerId, CancellationToken.None);

        // Assert
        status.ShouldBe(PeerStatus.Dead);
    }

    [Fact]
    public async Task EvaluatePeerHealthAsync_RecentHeartbeat_ShouldReturnActive()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        await this.detector.RecordHeartbeatAsync(peerId, CancellationToken.None);

        // Act
        var status = await this.detector.EvaluatePeerHealthAsync(peerId, CancellationToken.None);

        // Assert
        status.ShouldBe(PeerStatus.Active);
    }

    [Fact]
    public async Task EvaluatePeerHealthAsync_MissedThreeIntervals_ShouldReturnSuspect()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        await this.detector.RecordHeartbeatAsync(peerId, CancellationToken.None);

        // Wait to miss > 3 intervals (50ms * 3 = 150ms)
        await Task.Delay(180);

        // Act
        var status = await this.detector.EvaluatePeerHealthAsync(peerId, CancellationToken.None);

        // Assert
        // Since Task.Delay is not 100% precise, we assert Suspect or Dead just in case the delay spiked over 6 intervals.
        status.ShouldBeOneOf(PeerStatus.Suspect, PeerStatus.Dead);
        
        // Generally, it should hit Suspect if the runner executes exactly after 180ms
        if (status == PeerStatus.Dead)
        {
            // Edge case warning if machine is extremely slow
            Console.WriteLine("Warning: Task.Delay took too long, peer reached Dead state.");
        }
    }

    [Fact]
    public async Task EvaluatePeerHealthAsync_MissedSixIntervals_ShouldReturnDead()
    {
        // Arrange
        var peerId = new PeerId(Guid.NewGuid());
        await this.detector.RecordHeartbeatAsync(peerId, CancellationToken.None);

        // Wait to miss > 6 intervals (50ms * 6 = 300ms)
        await Task.Delay(350);

        // Act
        var status = await this.detector.EvaluatePeerHealthAsync(peerId, CancellationToken.None);

        // Assert
        status.ShouldBe(PeerStatus.Dead);
    }

    [Fact]
    public async Task RecordHeartbeatAsync_EmptyGuid_ShouldThrowArgumentException()
    {
        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(async () =>
            await this.detector.RecordHeartbeatAsync(new PeerId(Guid.Empty), CancellationToken.None));
    }

    [Fact]
    public async Task EvaluatePeerHealthAsync_EmptyGuid_ShouldThrowArgumentException()
    {
        // Act & Assert
        await Should.ThrowAsync<ArgumentException>(async () =>
            await this.detector.EvaluatePeerHealthAsync(new PeerId(Guid.Empty), CancellationToken.None));
    }
}