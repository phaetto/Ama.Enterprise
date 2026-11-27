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
/// Singleton striped command execution engine that multiplexes document state machine commands across a fixed number of background worker stripes.
/// </summary>
public sealed class DocumentCommandStripePool : IDocumentCommandStripePool, IDisposable
{
    private const int MaxPoolSize = 32_768;

    private static readonly AsyncLocal<int> CurrentStripeIndex = new() { Value = -1 };

    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ILogger<DocumentCommandStripePool> logger;
    private readonly Channel<PooledDocumentCommand>[] stripes;
    private readonly Task[] processingTasks;
    private readonly ConcurrentQueue<PooledDocumentCommand> commandPool = new();
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
    /// Initializes a new instance of the <see cref="DocumentCommandStripePool"/> class.
    /// </summary>
    /// <param name="options">The distributed CRDT options.</param>
    /// <param name="telemetry">The centralized telemetry service.</param>
    /// <param name="logger">The logger instance.</param>
    public DocumentCommandStripePool(
        IOptions<DistributedCrdtOptions> options,
        IDistributedCrdtTelemetry telemetry,
        ILogger<DocumentCommandStripePool> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        this.StripeCount = Math.Max(4, Environment.ProcessorCount * 2);
        this.stripes = new Channel<PooledDocumentCommand>[this.StripeCount];
        this.processingTasks = new Task[this.StripeCount];

        var capacity = options.Value.ChannelCapacity;
        for (int i = 0; i < this.StripeCount; i++)
        {
            if (capacity > 0)
            {
                this.stripes[i] = Channel.CreateBounded<PooledDocumentCommand>(new BoundedChannelOptions(capacity)
                {
                    SingleReader = true,
                    SingleWriter = false,
                    FullMode = BoundedChannelFullMode.Wait
                });
            }
            else
            {
                this.stripes[i] = Channel.CreateUnbounded<PooledDocumentCommand>(new UnboundedChannelOptions
                {
                    SingleReader = true,
                    SingleWriter = false
                });
            }

            var stripeIndex = i;
            this.processingTasks[i] = Task.Run(() => ProcessStripeChannelAsync(stripeIndex));
        }

        this.telemetryRegistration = this.telemetry.RegisterDocumentPipelineMetrics(
            GetTotalQueuedCount,
            () => Volatile.Read(ref this.poolCount));
    }

    /// <inheritdoc />
    public PooledDocumentCommand RentCommand()
    {
        if (commandPool.TryDequeue(out var cmd))
        {
            Interlocked.Decrement(ref poolCount);
            return cmd;
        }

        return new PooledDocumentCommand();
    }

    /// <inheritdoc />
    public void ReturnCommand(PooledDocumentCommand command)
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

    /// <inheritdoc />
    public async ValueTask EnqueueAndWaitAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        var target = command.Target ?? throw new InvalidOperationException("Command Target must be assigned before enqueuing.");

        if (IsCurrentThreadStripeProcessor)
        {
            var inlineDocId = target.DocumentId;
            telemetry.RecordDocumentCommandEnqueued(inlineDocId);
            try
            {
                command.CancellationToken.ThrowIfCancellationRequested();
                await target.ExecuteCommandInternalAsync(command).ConfigureAwait(false);
                telemetry.RecordDocumentCommandProcessed(inlineDocId);
                return;
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (Exception ex)
            {
                telemetry.RecordDocumentCommandFailure(inlineDocId);
                logger.LogError(ex, "Error executing inline document command {CommandType} for document {DocumentId}.", command.Type, inlineDocId);
                throw;
            }
        }

        cancellationToken.ThrowIfCancellationRequested();
        command.CancellationToken.ThrowIfCancellationRequested();

        command.PrepareExecution();
        var stripeIndex = ComputeStripeIndex(target);
        var documentId = target.DocumentId;

        try
        {
            await stripes[stripeIndex].Writer.WriteAsync(command, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            command.Reset();
            throw;
        }

        telemetry.RecordDocumentCommandEnqueued(documentId);
        await command.ExecuteAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask EnqueueFireAndForgetAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        var target = command.Target ?? throw new InvalidOperationException("Command Target must be assigned before enqueuing.");

        if (IsCurrentThreadStripeProcessor)
        {
            var inlineDocId = target.DocumentId;
            telemetry.RecordDocumentCommandEnqueued(inlineDocId);
            try
            {
                await target.ExecuteCommandInternalAsync(command).ConfigureAwait(false);
                telemetry.RecordDocumentCommandProcessed(inlineDocId);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                telemetry.RecordDocumentCommandFailure(inlineDocId);
                logger.LogError(ex, "Error executing inline fire-and-forget document command {CommandType} for document {DocumentId}.", command.Type, inlineDocId);
            }
            finally
            {
                ReturnCommand(command);
            }

            return;
        }

        var stripeIndex = ComputeStripeIndex(target);
        var documentId = target.DocumentId;
        command.IsFireAndForget = true;
        command.CancellationToken = CancellationToken.None;

        var writer = stripes[stripeIndex].Writer;
        if (writer.TryWrite(command))
        {
            telemetry.RecordDocumentCommandEnqueued(documentId);
            return;
        }

        await writer.WriteAsync(command, cancellationToken).ConfigureAwait(false);
        telemetry.RecordDocumentCommandEnqueued(documentId);
    }

    /// <inheritdoc />
    public async Task FlushScopeAsync(string scopeReplicaId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scopeReplicaId);

        if (disposeCts.IsCancellationRequested || IsCurrentThreadStripeProcessor)
        {
            return;
        }

        var barriers = new PooledDocumentCommand[StripeCount];
        var tasks = new Task[StripeCount];

        for (int i = 0; i < StripeCount; i++)
        {
            var cmd = RentCommand();
            cmd.Type = DocumentCommandType.FlushBarrier;
            cmd.CancellationToken = cancellationToken;
            barriers[i] = cmd;
            tasks[i] = FlushStripeAsync(stripes[i], cmd, cancellationToken);
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);
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

    private static async Task FlushStripeAsync(
        Channel<PooledDocumentCommand> stripe,
        PooledDocumentCommand cmd,
        CancellationToken cancellationToken)
    {
        cmd.PrepareExecution();
        var written = false;
        try
        {
            if (stripe.Writer.TryWrite(cmd))
            {
                written = true;
            }
            else
            {
                await stripe.Writer.WriteAsync(cmd, cancellationToken).ConfigureAwait(false);
                written = true;
            }

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
    }

    private int ComputeStripeIndex(IDocumentCommandTarget target)
    {
        var hash = HashCode.Combine(
            StringComparer.Ordinal.GetHashCode(target.ScopeReplicaId ?? string.Empty),
            StringComparer.Ordinal.GetHashCode(target.DocumentId ?? string.Empty));
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
                var documentId = target?.DocumentId ?? string.Empty;

                Interlocked.Increment(ref activeCommandCount);
                try
                {
                    cmd.CancellationToken.ThrowIfCancellationRequested();

                    if (target is not null)
                    {
                        CurrentStripeIndex.Value = stripeIndex;
                        try
                        {
                            await target.ExecuteCommandInternalAsync(cmd).ConfigureAwait(false);
                        }
                        finally
                        {
                            CurrentStripeIndex.Value = -1;
                        }
                    }

                    telemetry.RecordDocumentCommandProcessed(documentId);

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
                        cmd.SetException(new OperationCanceledException("Document operation cancelled."));
                    }
                }
                catch (Exception ex)
                {
                    telemetry.RecordDocumentCommandFailure(documentId);
                    logger.LogError(ex, "Error executing document command {CommandType} for document {DocumentId} on stripe {StripeIndex}.", cmd.Type, documentId, stripeIndex);
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
            logger.LogCritical(ex, "Document command stripe {StripeIndex} failed.", stripeIndex);
        }
    }
}