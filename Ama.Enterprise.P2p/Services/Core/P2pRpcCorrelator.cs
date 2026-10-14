namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services.Serialization;

/// <summary>
/// Singleton implementation managing pending generic RPC requests and evaluating timeouts utilizing standard cancellation tokens natively decoupled from protocol constraints.
/// </summary>
public sealed class P2pRpcCorrelator : IP2pRpcCorrelator
{
    private readonly ICrdtSerializer serializer;
    private readonly ConcurrentDictionary<Guid, IRpcPendingRequest> pendingRequests = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="P2pRpcCorrelator"/> class.
    /// </summary>
    /// <param name="serializer">The explicit serializer handling AOT network payload bindings.</param>
    public P2pRpcCorrelator(ICrdtSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);
        this.serializer = serializer;
    }

    /// <inheritdoc />
    public async Task<TResponse> RegisterRequestAsync<TResponse>(Guid correlationId, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource<ReadOnlyMemory<byte>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = new RpcPendingRequest(tcs);

        if (!this.pendingRequests.TryAdd(correlationId, request))
        {
            throw new InvalidOperationException($"A network request with correlation ID {correlationId} is already actively registered.");
        }

        using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(timeout);

        await using var registration = cts.Token.Register(() =>
        {
            tcs.TrySetCanceled(cts.Token);
        }).ConfigureAwait(false);

        try
        {
            var payload = await tcs.Task.ConfigureAwait(false);

            var response = this.serializer.DeserializeFromBytes<TResponse>(payload.Span);
            return response ?? throw new InvalidOperationException($"Failed to deserialize the RPC response payload into {typeof(TResponse).Name}.");
        }
        finally
        {
            this.pendingRequests.TryRemove(correlationId, out _);
        }
    }

    /// <inheritdoc />
    public void ResolveRequest(Guid correlationId, ReadOnlyMemory<byte> payload)
    {
        if (this.pendingRequests.TryRemove(correlationId, out var request))
        {
            request.SetResult(payload);
        }
    }

    /// <summary>
    /// Internal interface bridging the generic task completion source without strictly typing the collection.
    /// </summary>
    private interface IRpcPendingRequest
    {
        void SetResult(ReadOnlyMemory<byte> payload);
    }

    /// <summary>
    /// Internal structure encapsulating the task boundary.
    /// </summary>
    private sealed class RpcPendingRequest(TaskCompletionSource<ReadOnlyMemory<byte>> tcs) : IRpcPendingRequest
    {
        public void SetResult(ReadOnlyMemory<byte> payload)
        {
            tcs.TrySetResult(payload);
        }
    }
}