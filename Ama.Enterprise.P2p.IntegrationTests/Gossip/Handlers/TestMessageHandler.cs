namespace Ama.Enterprise.P2p.IntegrationTests.Gossip.Handlers;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;

/// <summary>
/// A data record representing the completely unwrapped application payload effectively decoupled from network envelopes.
/// </summary>
public readonly record struct TestPayloadRecord(string MeshId, PeerId SenderId, byte[] Payload);

/// <summary>
/// A test handler that simply records all incoming unwrapped application payloads for later assertions seamlessly.
/// </summary>
public sealed class TestMessageHandler : IApplicationPayloadHandler
{
    /// <summary>
    /// Gets the collection of payloads received by this handler instance.
    /// </summary>
    public ConcurrentBag<TestPayloadRecord> ReceivedMessages { get; } = new ConcurrentBag<TestPayloadRecord>();

    /// <inheritdoc />
    public Task HandlePayloadAsync(string meshId, PeerId senderId, ReadOnlyMemory<byte> payload, CancellationToken cancellationToken)
    {
        ReceivedMessages.Add(new TestPayloadRecord(meshId, senderId, payload.ToArray()));
        return Task.CompletedTask;
    }
}