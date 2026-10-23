namespace Ama.Enterprise.CRDT.Distributed.Services;

using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Models;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Hosted lifecycle service that registers customizable OS POSIX signals, console cancel events, and cloud shutdown sentinel files,
/// ensuring all distributed CRDT scopes flush before background services and P2P transports stop.
/// </summary>
public sealed class CrdtShutdownService : IHostedLifecycleService, IDisposable
{
    private const string AzureWebJobsShutdownFileEnvVar = "WEBJOBS_SHUTDOWN_FILE";

    private readonly ICrdtShutdownCoordinator shutdownCoordinator;
    private readonly IOptions<DistributedCrdtOptions> options;
    private readonly ILogger<CrdtShutdownService> logger;
    private readonly IHostApplicationLifetime? hostApplicationLifetime;
    private readonly List<IDisposable> signalRegistrations = new();

    private FileSystemWatcher? azureShutdownWatcher;
    private CancellationTokenRegistration applicationStoppingRegistration;
    private bool consoleCancelRegistered;
    private int isInitialized;
    private int isDisposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="CrdtShutdownService"/> class.
    /// </summary>
    /// <param name="shutdownCoordinator">The shutdown coordinator.</param>
    /// <param name="options">The distributed CRDT options.</param>
    /// <param name="logger">The logger instance.</param>
    /// <param name="hostApplicationLifetime">The optional host application lifetime.</param>
    public CrdtShutdownService(
        ICrdtShutdownCoordinator shutdownCoordinator,
        IOptions<DistributedCrdtOptions> options,
        ILogger<CrdtShutdownService> logger,
        IHostApplicationLifetime? hostApplicationLifetime = null)
    {
        this.shutdownCoordinator = shutdownCoordinator ?? throw new ArgumentNullException(nameof(shutdownCoordinator));
        this.options = options ?? throw new ArgumentNullException(nameof(options));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        this.hostApplicationLifetime = hostApplicationLifetime;
    }

    /// <inheritdoc />
    public Task StartingAsync(CancellationToken cancellationToken)
    {
        EnsureInitialized();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        EnsureInitialized();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public Task StoppingAsync(CancellationToken cancellationToken)
    {
        return FlushOnStoppingAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken)
    {
        return FlushOnStoppingAsync(cancellationToken);
    }

    /// <inheritdoc />
    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref isDisposed, 1) == 1)
        {
            return;
        }

        applicationStoppingRegistration.Dispose();

        if (consoleCancelRegistered && !OperatingSystem.IsBrowser() && !OperatingSystem.IsIOS() && !OperatingSystem.IsTvOS() && !OperatingSystem.IsWatchOS())
        {
            try
            {
                Console.CancelKeyPress -= OnConsoleCancelKeyPress;
            }
            catch (PlatformNotSupportedException)
            {
            }
        }

        foreach (var registration in signalRegistrations)
        {
            try
            {
                registration.Dispose();
            }
            catch (Exception)
            {
            }
        }

        signalRegistrations.Clear();
        azureShutdownWatcher?.Dispose();
    }

    private void EnsureInitialized()
    {
        if (!options.Value.EnableGracefulShutdown)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref isInitialized, 1, 0) != 0)
        {
            return;
        }

        if (!OperatingSystem.IsBrowser() && !OperatingSystem.IsIOS() && !OperatingSystem.IsTvOS() && !OperatingSystem.IsWatchOS())
        {
            RegisterPosixSignals();
            RegisterConsoleCancelKeyPress();
        }

        if (options.Value.WatchAzureWebJobsShutdownFile && !OperatingSystem.IsBrowser())
        {
            RegisterAzureWebJobsShutdownWatcher();
        }

        if (hostApplicationLifetime != null)
        {
            applicationStoppingRegistration = hostApplicationLifetime.ApplicationStopping.Register(() =>
            {
                _ = Task.Run(() => shutdownCoordinator.FlushAllScopesAsync("ApplicationStopping", CancellationToken.None));
            });
        }
    }

    private async Task FlushOnStoppingAsync(CancellationToken cancellationToken)
    {
        if (!options.Value.EnableGracefulShutdown)
        {
            return;
        }

        await shutdownCoordinator.FlushAllScopesAsync("HostStopping", cancellationToken).ConfigureAwait(false);
    }

    [UnsupportedOSPlatform("browser")]
    [UnsupportedOSPlatform("ios")]
    [UnsupportedOSPlatform("tvos")]
    [UnsupportedOSPlatform("watchos")]
    private void RegisterPosixSignals()
    {
        var configuredSignals = options.Value.ShutdownPosixSignals;
        if (configuredSignals == null || configuredSignals.Count == 0)
        {
            return;
        }

        var uniqueSignals = new HashSet<PosixSignal>();
        foreach (var signalName in configuredSignals)
        {
            if (TryParsePosixSignal(signalName, out var posixSignal) && uniqueSignals.Add(posixSignal))
            {
                try
                {
                    var registration = PosixSignalRegistration.Create(posixSignal, OnPosixSignalReceived);
                    signalRegistrations.Add(registration);
                    logger.LogDebug("Registered shutdown handler for POSIX signal {Signal}.", signalName);
                }
                catch (Exception ex) when (ex is PlatformNotSupportedException or ArgumentException or IOException)
                {
                    logger.LogDebug(ex, "POSIX signal {Signal} is not supported on the current platform.", signalName);
                }
            }
        }
    }

    [UnsupportedOSPlatform("browser")]
    [UnsupportedOSPlatform("ios")]
    [UnsupportedOSPlatform("tvos")]
    [UnsupportedOSPlatform("watchos")]
    private void OnPosixSignalReceived(PosixSignalContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        context.Cancel = true;
        var signalName = context.Signal.ToString();
        logger.LogInformation("Intercepted POSIX signal {Signal}. Initiating CRDT flush and shutdown.", signalName);

        _ = Task.Run(async () =>
        {
            try
            {
                await shutdownCoordinator.RequestShutdownAsync($"PosixSignal:{signalName}", CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unhandled error while processing POSIX shutdown signal {Signal}.", signalName);
            }
        });
    }

    [UnsupportedOSPlatform("browser")]
    [UnsupportedOSPlatform("ios")]
    [UnsupportedOSPlatform("tvos")]
    [UnsupportedOSPlatform("watchos")]
    private void RegisterConsoleCancelKeyPress()
    {
        try
        {
            Console.CancelKeyPress += OnConsoleCancelKeyPress;
            consoleCancelRegistered = true;
        }
        catch (PlatformNotSupportedException)
        {
        }
    }

    private void OnConsoleCancelKeyPress(object? sender, ConsoleCancelEventArgs e)
    {
        e.Cancel = true;
        var reason = $"ConsoleCancelKeyPress:{e.SpecialKey}";
        logger.LogInformation("Intercepted console cancel key ({SpecialKey}). Initiating CRDT flush and shutdown.", e.SpecialKey);

        _ = Task.Run(async () =>
        {
            try
            {
                await shutdownCoordinator.RequestShutdownAsync(reason, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unhandled error while processing console cancel key shutdown.");
            }
        });
    }

    private void RegisterAzureWebJobsShutdownWatcher()
    {
        var shutdownFilePath = Environment.GetEnvironmentVariable(AzureWebJobsShutdownFileEnvVar);
        if (string.IsNullOrWhiteSpace(shutdownFilePath))
        {
            return;
        }

        try
        {
            if (File.Exists(shutdownFilePath))
            {
                TriggerAzureShutdownFileDetected();
                return;
            }

            var directory = Path.GetDirectoryName(shutdownFilePath);
            var fileName = Path.GetFileName(shutdownFilePath);
            if (string.IsNullOrWhiteSpace(directory) || string.IsNullOrWhiteSpace(fileName) || !Directory.Exists(directory))
            {
                return;
            }

            azureShutdownWatcher = new FileSystemWatcher(directory, fileName)
            {
                NotifyFilter = NotifyFilters.CreationTime | NotifyFilters.FileName | NotifyFilters.LastWrite
            };

            azureShutdownWatcher.Created += (_, _) => TriggerAzureShutdownFileDetected();
            azureShutdownWatcher.Changed += (_, _) => TriggerAzureShutdownFileDetected();
            azureShutdownWatcher.EnableRaisingEvents = true;

            logger.LogDebug("Monitoring Azure WebJobs shutdown sentinel file at '{ShutdownFilePath}'.", shutdownFilePath);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to configure FileSystemWatcher for Azure WebJobs shutdown file '{ShutdownFilePath}'.", shutdownFilePath);
        }
    }

    private void TriggerAzureShutdownFileDetected()
    {
        logger.LogInformation("Detected Azure WebJobs shutdown sentinel file. Initiating CRDT flush and shutdown.");
        _ = Task.Run(async () =>
        {
            try
            {
                await shutdownCoordinator.RequestShutdownAsync("AzureWebJobsShutdownFile", CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unhandled error while processing Azure WebJobs shutdown sentinel file.");
            }
        });
    }

    [UnsupportedOSPlatform("browser")]
    [UnsupportedOSPlatform("ios")]
    [UnsupportedOSPlatform("tvos")]
    [UnsupportedOSPlatform("watchos")]
    private static bool TryParsePosixSignal(string? signalName, out PosixSignal signal)
    {
        signal = default;
        if (string.IsNullOrWhiteSpace(signalName))
        {
            return false;
        }

        var normalized = signalName.Trim().ToUpperInvariant();
        switch (normalized)
        {
            case "SIGTERM":
            case "TERM":
                signal = PosixSignal.SIGTERM;
                return true;
            case "SIGINT":
            case "INT":
                signal = PosixSignal.SIGINT;
                return true;
            case "SIGQUIT":
            case "QUIT":
                signal = PosixSignal.SIGQUIT;
                return true;
            case "SIGHUP":
            case "HUP":
                signal = PosixSignal.SIGHUP;
                return true;
            default:
                return Enum.TryParse(normalized, ignoreCase: true, out signal);
        }
    }
}