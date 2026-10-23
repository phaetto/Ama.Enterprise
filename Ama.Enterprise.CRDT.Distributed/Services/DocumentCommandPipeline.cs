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
/// Shared scoped state machine pipeline that processes all CRDT document commands through a single channel and object pool.
/// </summary>
public sealed class DocumentCommandPipeline : IDocumentCommandPipeline, IDisposable
{
    private readonly IDistributedCrdtTelemetry telemetry;
    private readonly ILogger<DocumentCommandPipeline> logger;
    private readonly Channel<PooledDocumentCommand> commandChannel;
    private readonly ConcurrentQueue<PooledDocumentCommand> commandPool = new();
    private readonly CancellationTokenSource disposeCts = new();
    private readonly IDisposable telemetryRegistration;
    private readonly Task processingTask;

    /// <summary>
    /// Initializes a new instance of the <see cref="DocumentCommandPipeline"/> class.
    /// </summary>
    /// <param name="options">The distributed CRDT options.</param>
    /// <param name="telemetry">The centralized telemetry service.</param>
    /// <param name="logger">The logger instance.</param>
    public DocumentCommandPipeline(
        IOptions<DistributedCrdtOptions> options,
        IDistributedCrdtTelemetry telemetry,
        ILogger<DocumentCommandPipeline> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        this.telemetry = telemetry ?? throw new ArgumentNullException(nameof(telemetry));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));

        var capacity = options.Value.ChannelCapacity;
        if (capacity > 0)
        {
            this.commandChannel = Channel.CreateBounded<PooledDocumentCommand>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait
            });
        }
        else
        {
            this.commandChannel = Channel.CreateUnbounded<PooledDocumentCommand>(new UnboundedChannelOptions
            {
                SingleReader = true,
                SingleWriter = false
            });
        }

        this.telemetryRegistration = this.telemetry.RegisterDocumentPipelineMetrics(
            () => this.commandChannel.Reader.CanCount ? this.commandChannel.Reader.Count : 0,
            () => this.commandPool.Count);

        this.processingTask = Task.Run(ProcessChannelAsync);
    }

    /// <inheritdoc />
    public PooledDocumentCommand RentCommand()
    {
        return commandPool.TryDequeue(out var cmd) ? cmd : new PooledDocumentCommand();
    }

    /// <inheritdoc />
    public void ReturnCommand(PooledDocumentCommand command)
    {
        ArgumentNullException.ThrowIfNull(command);
        command.Reset();
        commandPool.Enqueue(command);
    }

    /// <inheritdoc />
    public async ValueTask EnqueueAndWaitAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (command.Target is null)
        {
            throw new InvalidOperationException("Command Target must be assigned before enqueuing.");
        }

        var documentId = command.Target.DocumentId;
        await commandChannel.Writer.WriteAsync(command, cancellationToken).ConfigureAwait(false);
        telemetry.RecordDocumentCommandEnqueued(documentId);
        await command.ExecuteAsync().ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask EnqueueFireAndForgetAsync(PooledDocumentCommand command, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(command);
        if (command.Target is null)
        {
            throw new InvalidOperationException("Command Target must be assigned before enqueuing.");
        }

        var documentId = command.Target.DocumentId;
        command.IsFireAndForget = true;
        command.CancellationToken = CancellationToken.None;

        if (commandChannel.Writer.TryWrite(command))
        {
            telemetry.RecordDocumentCommandEnqueued(documentId);
            return;
        }

        await commandChannel.Writer.WriteAsync(command, cancellationToken).ConfigureAwait(false);
        telemetry.RecordDocumentCommandEnqueued(documentId);
    }

    /// <inheritdoc />
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        commandChannel.Writer.TryComplete();
        await processingTask.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        telemetryRegistration.Dispose();
        disposeCts.Cancel();
        commandChannel.Writer.TryComplete();
        disposeCts.Dispose();
    }

    private async Task ProcessChannelAsync()
    {
        try
        {
            await foreach (var cmd in commandChannel.Reader.ReadAllAsync(disposeCts.Token).ConfigureAwait(false))
            {
                var target = cmd.Target;
                var documentId = target?.DocumentId ?? string.Empty;

                try
                {
                    cmd.CancellationToken.ThrowIfCancellationRequested();

                    if (target is not null)
                    {
                        await target.ExecuteCommandInternalAsync(cmd).ConfigureAwait(false);
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
                    logger.LogError(ex, "Error executing document command {CommandType} for document {DocumentId}.", cmd.Type, documentId);
                    if (cmd.IsFireAndForget)
                    {
                        ReturnCommand(cmd);
                    }
                    else
                    {
                        cmd.SetException(ex);
                    }
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
            logger.LogCritical(ex, "Shared document command pipeline failed.");
        }
    }
}