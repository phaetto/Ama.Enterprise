namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Singleton striped command execution engine that multiplexes <see cref="CrdtDocumentOrchestrator"/> commands across a fixed number of background worker stripes.
/// </summary>
public sealed class OrchestratorCommandStripePool : IOrchestratorCommandStripePool, IDisposable
{
    private const int MaxPoolSize = 16_384;

    private static readonly AsyncLocal<int> CurrentStripeIndex = new() { Value = -1 };
    private static readonly AsyncLocal<IOrchestratorCommandTarget?> CurrentProcessingTarget = new();

    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ILogger<OrchestratorCommandStripePool> logger;
    private readonly Channel<PooledOrchestratorCommand>[] stripes;
    private readonly Task[] processingTasks;
    private readonly ConcurrentQueue<PooledOrchestratorCommand> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private readonly IDisposable telemetryRegistration;
    private int poolCount;
    private int activeCommandCount;

    /// <inheritdoc />
    public int StripeCount { get; }

    /// <inheritdoc />
    public bool IsCurrentThreadStripeProcessor => CurrentStripeIndex.Value >= 0;

    /// <inheritdoc />
    public int PendingCommandCount => GetTotalQueuedCount() + Volatile.Read(ref activeCommandCount);

    /// <summary>
    /// Initializes a new instance of the <see cref="OrchestratorCommandStripePool"/> class.
    /// </summary>
    /// <param name="options">The distributed CRDT options.</param>
    /// <param name="telemetry">The centralized telemetry service.</param>
    /// <param name="logger">The logger instance.</param>
    public OrchestratorCommandStripePool(
        IOptions<DistributedCrdtOptions> options,
        IDistributedCrdtTelemetry telemetry,
        ILogger<OrchestratorCommandStripePool> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.StripeCount = Math.Max(4, Environment.ProcessorCount * 2);
        this.stripes = new Channel<PooledOrchestratorCommand>[this.StripeCount];
        this.processingTasks = new Task[this.StripeCount];

        var capacity = options.Value.ChannelCapacity;
        for (int i = 0; i < this.StripeCount; i++)
        {
            if (capacity > 0)
            {
                this.stripes[i] = Channel.CreateBounded<PooledOrchestratorCommand>(new BoundedChannelOptions(capacity)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });
            }
            else
            {
                this.stripes[i] = Channel.CreateUnbounded<PooledOrchestratorCommand>(new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = false
                });
            }

            var stripeIndex = i;
            this.processingTasks[i] = Task.Run(() => ProcessStripeChannelAsync(stripeIndex));
        }

        this.telemetryRegistration = this.telemetry.RegisterOrchestratorPipelineMetrics(
            GetTotalQueuedCount,
            () => Volatile.Read(ref this.poolCount));
    }

    /// <inheritdoc />
    PooledOrchestratorCommand IOrchestratorCommandStripePool.RentCommand() => RentCommand();

    /// <inheritdoc />
    void IOrchestratorCommandStripePool.ReturnCommand(PooledOrchestratorCommand command) => ReturnCommand(command);

    /// <inheritdoc />
    bool IOrchestratorCommandStripePool.IsCurrentThreadProcessingTarget(IOrchestratorCommandTarget target)
    {
        ArgumentNullException.ThrowIfNull(target);
        if (ReferenceEquals(CurrentProcessingTarget.Value, target))
        {
            return true;
        }

        var currentStripe = CurrentStripeIndex.Value;
        return currentStripe >= 0 && currentStripe == ComputeStripeIndex(target.ScopeReplicaId);
    }

    /// <inheritdoc />
    async ValueTask IOrchestratorCommandStripePool.EnqueueAndWaitAsync(PooledOrchestratorCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        var target = command.Target ?? throw new InvalidOperationException("Orchestrator command Target must be assigned before enqueuing.");

        if (((IOrchestratorCommandStripePool)this).IsCurrentThreadProcessingTarget(target))
        {
            telemetry.RecordOrchestratorCommandEnqueued();
            try
            {
                command.CancellationToken.ThrowIfCancellationRequested();
                await target.ExecuteOrchestratorCommandInternalAsync(command).ConfigureAwait(false);
                telemetry.RecordOrchestratorCommandProcessed();
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                telemetry.RecordOrchestratorCommandFailure();
                logger.LogError(ex, "Error executing inline orchestrator command {CommandType} for document {DocumentId}.", command.Type, command.DocumentId);
                throw;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        command.CancellationToken.ThrowIfCancellationRequested();

        command.PrepareExecution();
        var stripeIndex = ComputeStripeIndex(target.ScopeReplicaId);
        try
        {
            await stripes[stripeIndex].Writer.WriteAsync(command, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            command.Reset();
            throw;
        }

        telemetry.RecordOrchestratorCommandEnqueued();
        await command.ExecuteAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    bool IOrchestratorCommandStripePool.TryEnqueueFireAndForget(PooledOrchestratorCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        var target = command.Target ?? throw new InvalidOperationException("Orchestrator command Target must be assigned before enqueuing.");

        command.IsFireAndForget = true;
        command.CancellationToken = CancellationToken.None;

        var stripeIndex = ComputeStripeIndex(target.ScopeReplicaId);
        if (stripes[stripeIndex].Writer.TryWrite(command))
        {
            telemetry.RecordOrchestratorCommandEnqueued();
            return true;
        }

        return false;
    }

    /// <inheritdoc />
    async ValueTask IOrchestratorCommandStripePool.EnqueueFireAndForgetAsync(PooledOrchestratorCommand command, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        var target = command.Target ?? throw new InvalidOperationException("Orchestrator command Target must be assigned before enqueuing.");

        if (((IOrchestratorCommandStripePool)this).IsCurrentThreadProcessingTarget(target))
        {
            telemetry.RecordOrchestratorCommandEnqueued();
            try
            {
                await target.ExecuteOrchestratorCommandInternalAsync(command).ConfigureAwait(false);
                telemetry.RecordOrchestratorCommandProcessed();
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                telemetry.RecordOrchestratorCommandFailure();
                logger.LogError(ex, "Error executing inline fire-and-forget orchestrator command {CommandType} for document {DocumentId}.", command.Type, command.DocumentId);
            }
            finally
            {
                ReturnCommand(command);
            }

            return;
        }

        command.IsFireAndForget = true;
        command.CancellationToken = CancellationToken.None;

        var stripeIndex = ComputeStripeIndex(target.ScopeReplicaId);
        var writer = stripes[stripeIndex].Writer;
        if (writer.TryWrite(command))
        {
            telemetry.RecordOrchestratorCommandEnqueued();
            return;
        }

        await writer.WriteAsync(command, cancellationToken).ConfigureAwait(false);
        telemetry.RecordOrchestratorCommandEnqueued();
    }

    /// <inheritdoc />
    public async Task FlushScopeAsync(string scopeReplicaId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scopeReplicaId);

        if (disposeCts.IsCancellationRequested)
        {
            return;
        }

        var stripeIndex = ComputeStripeIndex(scopeReplicaId);
        if (CurrentStripeIndex.Value == stripeIndex)
        {
            return;
        }

        var cmd = RentCommand();
        cmd.Type = OrchestratorCommandType.FlushBarrier;
        cmd.CancellationToken = cancellationToken;
        cmd.PrepareExecution();

        var written = false;
        try
        {
            var stripe = stripes[stripeIndex];
            if (stripe.Writer.TryWrite(cmd))
            {
                written = true;
            }
            else
            {
                await stripe.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
                written = true;
            }

            telemetry.RecordOrchestratorCommandEnqueued();
            await cmd.ExecuteAsync().ConfigureAwait(false);
        }
        catch (ChannelClosedException)
        {
            if (!written)
            {
                cmd.Reset();
            }
        }
        catch
        {
            if (!written)
            {
                cmd.Reset();
            }

            throw;
        }
        finally
        {
            ReturnCommand(cmd);
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        telemetryRegistration.Dispose();
        disposeCts.Cancel();

        for (int i = 0; i < StripeCount; i++)
        {
            stripes[i].Writer.TryComplete();
        }

        disposeCts.Dispose();
    }

    private PooledOrchestratorCommand RentCommand()
    {
        if (commandPool.TryDequeue(out var cmd))
        {
            Interlocked.Decrement(ref poolCount);
            return cmd;
        }

        return new PooledOrchestratorCommand();
    }

    private void ReturnCommand(PooledOrchestratorCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        command.Reset();

        if (Interlocked.Increment(ref poolCount) <= MaxPoolSize)
        {
            commandPool.Enqueue(command);
        }
        else
        {
            Interlocked.Decrement(ref poolCount);
        }
    }

    private int ComputeStripeIndex(string scopeReplicaId)
    {
        var hash = StringComparer.Ordinal.GetHashCode(scopeReplicaId ?? string.Empty);
        return (hash & 0x7FFFFFFF) % StripeCount;
    }

    private int GetTotalQueuedCount()
    {
        var total = 0;
        for (int i = 0; i < StripeCount; i++)
        {
            var reader = stripes[i].Reader;
            if (reader.CanCount)
            {
                total += reader.Count;
            }
        }

        return total;
    }

    private async Task ProcessStripeChannelAsync(int stripeIndex)
    {
        var reader = stripes[stripeIndex].Reader;
        try
        {
            await foreach (var cmd in reader.ReadAllAsync(disposeCts.Token).ConfigureAwait(false))
            {
                var target = cmd.Target;

                Interlocked.Increment(ref activeCommandCount);
                try
                {
                    cmd.CancellationToken.ThrowIfCancellationRequested();

                    if (target is not null)
                    {
                        CurrentStripeIndex.Value = stripeIndex;
                        CurrentProcessingTarget.Value = target;
                        try
                        {
                            await target.ExecuteOrchestratorCommandInternalAsync(cmd).ConfigureAwait(false);
                        }
                        finally
                        {
                            CurrentProcessingTarget.Value = null;
                            CurrentStripeIndex.Value = -1;
                        }
                    }

                    telemetry.RecordOrchestratorCommandProcessed();

                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetResult();
                    }
                }
                catch (OperationCanceledException)
                {
                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetException(new TaskCanceledException("Orchestrator operation cancelled."));
                    }
                }
                catch (Exception ex)
                {
                    telemetry.RecordOrchestratorCommandFailure();
                    logger.LogError(ex, "Error executing orchestrator command {CommandType} for document {DocumentId} on stripe {StripeIndex}.", cmd.Type, cmd.DocumentId, stripeIndex);
                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetException(ex);
                    }
                }
                finally
                {
                    Interlocked.Decrement(ref activeCommandCount);
                }
            }
        }
        catch (OperationCanceledException)
        {
        }
        catch (ObjectDisposedException)
        {
        }
        catch (Exception ex)
        {
            logger.LogCritical(ex, "Orchestrator command stripe {StripeIndex} failed.", stripeIndex);
        }
    }
}