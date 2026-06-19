namespace Ama.Enterprise.FeatureFlags.ShowCase;

using Ama.Enterprise.CRDT.Distributed.Extensions;
using Ama.Enterprise.CRDT.Distributed.Services;
using Ama.Enterprise.FeatureFlags.Extensions;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.Licensing.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// Showcase entry point for demonstrating P2P Feature Flags across local console processes.
/// </summary>
public static class Program
{
    private static readonly object ConsoleLock = new();
    private static int currentPort;
    private static int currentHandshakePort;

    public static async Task Main(string[] args)
    {
        // Parse port from arguments, or assign a random free-ish port for showcase
        if (args.Length == 0 || !int.TryParse(args[0], out currentPort))
        {
            currentPort = 8080 + Random.Shared.Next(0, 1000);
        }

        currentHandshakePort = 8080 + Random.Shared.Next(0, 1000);

        // Generate or load a shared self-signed certificate for the local showcase cluster
        using var clusterCert = GetOrCreateClusterCertificate();
        var certBytes = clusterCert.Export(X509ContentType.Cert);

        // Generate or load a shared symmetric encryption key for wire encryption
        var encryptionKey = GetOrCreateEncryptionKey();

        var replicaId = $"node-{currentPort}";
        var services = new ServiceCollection();

        // Configure Logging - Using a custom lock-aware logger so background tasks
        // (like our UDP discovery logs) don't shred the interactive console UI.
        services.AddLogging(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddFilter("Microsoft", LogLevel.Warning);
            builder.AddFilter("System", LogLevel.Warning);
            
            // Optionally clear default providers and use our synchronized console logger
            builder.ClearProviders();
            builder.AddProvider(new LockedConsoleLoggerProvider());
        });

        // Register licensing explicitly specifying Community mode natively matching explicit structural generic mappings
        services.ConfigureAmaCommunityLicense();

        // 1. Explicitly register the distributed CRDT topological core bounds
        services.AddDistributedCrdtReplica(replicaId);
        
        services.AddDistributedCrdtCore(options =>
        {
            options.ActiveSyncEnabled = true;
            options.CheckpointIntervalSeconds = (int)TimeSpan.FromHours(1).TotalSeconds;
            options.AntiEntropyIntervalSeconds = 1;
            options.AntiEntropyInitialDelaySeconds = 1;
        });

        // 2. Delegate routing orchestrations and background services completely to the distributed core explicitly bounded
        services.AddDistributedCrdtP2p("feature-flags-internal-mesh", replicaId);

        // 3. Add Feature Flags Product domain abstractions natively tracking internal explicit scopes mapped dynamically 
        services.AddFeatureFlags();

        // 4. Wire up the generic P2P mesh network specifically configured for this feature's underlying topology
        services.AddP2pMesh("feature-flags-internal-mesh")
                .AddGossipNetwork(options =>
                {
                    options.GossipInterval = TimeSpan.FromMilliseconds(1500);
                    options.Fanout = 3;
                    options.DefaultTimeToLive = 3;
                })
                .AddTcpTransport(options =>
                {
                    options.ListenPort = currentPort;
                    options.ListenHost = "127.0.0.1";
                })
                .AddUdpPeerDiscovery(options =>
                {
                    options.MulticastAddress = "239.255.0.1";
                    options.MulticastPort = 8035;
                    options.DiscoveryInterval = TimeSpan.FromSeconds(1);
                    options.DiscoveryTimeout = TimeSpan.FromSeconds(10);
                })
                .AddUdpPeerHandshake(options =>
                {
                    options.ListenPort = currentHandshakePort;
                })
                .AddCertificateAuthenticator(options =>
                {
                    options.LocalCertificateBytes = certBytes;
                    options.AllowedThumbprints.Add(clusterCert.Thumbprint);
                    options.ValidateCertificateChain = false;
                })
                .AddWireEncoder(options =>
                {
                    options.IsEncryptionEnabled = true;
                    options.EncryptionKeyBase64 = encryptionKey;
                });

        await using var provider = services.BuildServiceProvider();
        var logger = provider.GetRequiredService<ILoggerFactory>().CreateLogger("ShowCase");
        
        // Background services typically remain resolved from the root provider as singletons
        var hostedServices = provider.GetServices<IHostedService>().ToList();

        // Resolve the scoped structural boundaries targeting explicit ReplicaId to extract isolated generic scopes safely
        var scopeFactory = provider.GetRequiredService<DistributedCrdtScopeManager>();
        var crdtScope = scopeFactory.GetOrCreateScope(replicaId);
        
        // Resolve the cluster manager natively via the transparent forwarder implicitly mapped to the internal state scope
        var clusterManager = crdtScope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();
        
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true; // Prevent immediate shutdown
            cts.Cancel();
        };

        // Wire up the event handler to react to cluster changes rather than polling
        clusterManager.StateChanged += (sender, eventArgs) =>
        {
            var flags = clusterManager.GetFlags();
            DrawFlags(flags);
        };

        try
        {
            logger.LogInformation("Starting showcase node on port {Port}...", currentPort);

            // Start all background hosted services (Gossip loop, Anti-Entropy, UDP Listener, Bootstrapper)
            foreach (var service in hostedServices)
            {
                await service.StartAsync(cts.Token).ConfigureAwait(false);
            }

            DrawMenu();

            // Initial UI draw
            var initialFlags = clusterManager.GetFlags();
            DrawFlags(initialFlags);

            // Interactive Console Loop
            while (!cts.Token.IsCancellationRequested)
            {
                var input = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(input))
                {
                    continue;
                }

                var parts = input.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                var command = parts[0].ToLowerInvariant();

                try
                {
                    switch (command)
                    {
                        case "set":
                            if (parts.Length >= 3 && bool.TryParse(parts[2], out var isEnabled))
                            {
                                await clusterManager.SetFlagAsync(parts[1], isEnabled, cancellationToken: cts.Token).ConfigureAwait(false);
                            }
                            else
                            {
                                WriteLineLocked("Usage: set <name> <true|false>");
                            }
                            break;

                        case "del":
                            if (parts.Length >= 2)
                            {
                                await clusterManager.RemoveFlagAsync(parts[1], cts.Token).ConfigureAwait(false);
                            }
                            else
                            {
                                WriteLineLocked("Usage: del <name>");
                            }
                            break;

                        case "clone":
                            CloneProcess(logger);
                            break;

                        case "exit":
                            cts.Cancel();
                            break;

                        default:
                            WriteLineLocked("Unknown command.");
                            DrawMenu();
                            break;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error processing command.");
                }
            }
        }
        finally
        {
            logger.LogInformation("Shutting down services. Please wait...");
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Spawns a new instance of this console application on a new random port to join the cluster.
    /// </summary>
    private static void CloneProcess(ILogger logger)
    {
        var nextPort = 8080 + Random.Shared.Next(0, 1000);
        var processPath = Environment.ProcessPath;

        if (string.IsNullOrEmpty(processPath))
        {
            logger.LogWarning("Unable to determine process path for cloning.");
            return;
        }

        Process.Start(new ProcessStartInfo
        {
            FileName = processPath,
            Arguments = nextPort.ToString(),
            UseShellExecute = true // Spawns a new independent console window
        });

        logger.LogInformation("Cloned new cluster node instance on port {NextPort}.", nextPort);
    }

    private static void DrawMenu()
    {
        lock (ConsoleLock)
        {
            Console.WriteLine("=================================================");
            Console.WriteLine($" Feature Flags Peer Node - Listening on Port {currentPort}");
            Console.WriteLine("=================================================");
            Console.WriteLine("Commands:");
            Console.WriteLine(" set <flagName> <true|false>  - Adds or Updates a flag");
            Console.WriteLine(" del <flagName>               - Removes a flag entirely");
            Console.WriteLine(" clone                        - Spawns a new cluster node in a new window");
            Console.WriteLine(" exit                         - Shuts down this node gracefully");
            Console.WriteLine("=================================================\n");
        }
    }

    private static void DrawFlags(IReadOnlyDictionary<string, FeatureFlag> flags)
    {
        lock (ConsoleLock)
        {
            Console.WriteLine("\n--- [Cluster State Synchronized] ---");
            if (flags.Count == 0)
            {
                Console.WriteLine(" (No flags currently defined)");
            }
            else
            {
                foreach (var flag in flags.OrderBy(f => f.Key))
                {
                    Console.WriteLine($" => [{flag.Key}]: {(flag.Value.IsEnabled ? "ENABLED" : "DISABLED")}");
                }
            }
            Console.WriteLine("------------------------------------\n> ");
        }
    }

    private static void WriteLineLocked(string message)
    {
        lock (ConsoleLock)
        {
            Console.WriteLine(message);
        }
    }

    /// <summary>
    /// Checks for an existing local cluster certificate to ensure all spawned clones share the same valid thumbprint.
    /// Generates a new self-signed certificate if one doesn't exist.
    /// </summary>
    private static X509Certificate2 GetOrCreateClusterCertificate()
    {
        const string certPath = "showcase-cluster.cer";
        if (File.Exists(certPath))
        {
            return X509CertificateLoader.LoadCertificateFromFile(certPath);
        }

        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=ShowcaseCluster", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var expire = DateTimeOffset.UtcNow.AddDays(7);
        var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), expire);

        File.WriteAllBytes(certPath, cert.Export(X509ContentType.Cert));
        return cert;
    }

    /// <summary>
    /// Checks for an existing local encryption key to ensure all spawned clones share the same valid AES-GCM key.
    /// Generates a new 32-byte cryptographically secure key if one doesn't exist.
    /// </summary>
    private static string GetOrCreateEncryptionKey()
    {
        const string keyPath = "showcase-encryption.key";
        if (File.Exists(keyPath))
        {
            return File.ReadAllText(keyPath).Trim();
        }

        var keyBytes = RandomNumberGenerator.GetBytes(32);
        var base64Key = Convert.ToBase64String(keyBytes);
        
        File.WriteAllText(keyPath, base64Key);
        return base64Key;
    }

    // Custom lock-aware logger provider ensuring dependency injection traces format synchronously
    // without scrambling the showcase input console.
    private sealed class LockedConsoleLoggerProvider : ILoggerProvider
    {
        public ILogger CreateLogger(string categoryName) => new LockedConsoleLogger(categoryName);
        public void Dispose() { }
    }

    private sealed class LockedConsoleLogger(string categoryName) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Information;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
            {
                return;
            }

            var message = formatter(state, exception);
            if (string.IsNullOrEmpty(message) && exception is null)
            {
                return;
            }

            var shortCategory = categoryName.Split('.').LastOrDefault() ?? categoryName;
            var logLine = $"[{logLevel.ToString().ToUpperInvariant()}] {shortCategory}: {message}";
            
            if (exception is not null)
            {
                logLine += Environment.NewLine + exception;
            }

            // Route standard background logs through the locked console sync block
            WriteLineLocked(logLine);
        }
    }
}