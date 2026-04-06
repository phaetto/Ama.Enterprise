namespace Ama.Enterprise.FeatureFlags.ShowCase;

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Services;
using Ama.Enterprise.FeatureFlags.Extensions;
using Ama.Enterprise.FeatureFlags.Models;
using Ama.Enterprise.FeatureFlags.Services;
using Ama.Enterprise.P2p.Extensions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

/// <summary>
/// Showcase entry point for demonstrating P2P Feature Flags across local console processes.
/// </summary>
public static class Program
{
    private static readonly object ConsoleLock = new();
    private static int currentPort;

    public static async Task Main(string[] args)
    {
        // Parse port from arguments, or assign a random free-ish port for showcase
        if (args.Length == 0 || !int.TryParse(args[0], out currentPort))
        {
            currentPort = 8080 + Random.Shared.Next(0, 1000);
        }

        var replicaId = $"node-{currentPort}";
        var services = new ServiceCollection();

        // Configure Logging - Disable lower levels so the UI isn't destroyed by background Gossip traces
        services.AddLogging(builder =>
        {
            builder.SetMinimumLevel(LogLevel.Error);
            builder.AddConsole();
        });

        // 1. Add CRDT and Feature Flags with the assigned ReplicaId for the Showcase process node
        services.AddFeatureFlags(options =>
        {
            options.ReplicaId = replicaId;
        });
        services.AddFeatureFlagsP2p();

        // 2. Add Gossip Network for P2P transport
        services.AddP2pGossipNetwork(options =>
        {
            options.ListenPort = currentPort;
            options.ListenHost = "localhost";
            options.GossipInterval = TimeSpan.FromSeconds(2);
        });

        // 3. Add UDP Peer Discovery so nodes automatically find each other in the local network
        services.AddUdpPeerDiscovery(options =>
        {
            options.LocalEndpointPort = currentPort;
            options.MulticastAddress = "239.255.0.1";
            options.MulticastPort = 8035;
        });

        await using var provider = services.BuildServiceProvider();
        
        // Resolve the scope factory and create a dedicated scope for this replica node
        var scopeFactory = provider.GetRequiredService<ICrdtScopeFactory>();
        using var scope = scopeFactory.CreateScope(replicaId);
        
        var clusterManager = scope.ServiceProvider.GetRequiredService<IFeatureFlagClusterManager>();
        
        // Background services typically remain resolved from the root provider as singletons
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true; // Prevent immediate shutdown
            cts.Cancel();
        };

        try
        {
            // Start all background hosted services (Gossip loop, Anti-Entropy, UDP Listener)
            foreach (var service in hostedServices)
            {
                await service.StartAsync(cts.Token).ConfigureAwait(false);
            }

            // Launch the background UI updater
            _ = Task.Run(() => MonitorChangesAsync(clusterManager, cts.Token), cts.Token);

            DrawMenu();

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
                                await clusterManager.SetFlagAsync(parts[1], isEnabled, cts.Token).ConfigureAwait(false);
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
                            CloneProcess();
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
                    WriteLineLocked($"Error: {ex.Message}");
                }
            }
        }
        finally
        {
            WriteLineLocked("Shutting down services. Please wait...");
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Spawns a new instance of this console application on a new random port to join the cluster.
    /// </summary>
    private static void CloneProcess()
    {
        var nextPort = 8080 + Random.Shared.Next(0, 1000);
        var processPath = Environment.ProcessPath;

        if (string.IsNullOrEmpty(processPath))
        {
            WriteLineLocked("Unable to determine process path for cloning.");
            return;
        }

        Process.Start(new ProcessStartInfo
        {
            FileName = processPath,
            Arguments = nextPort.ToString(),
            UseShellExecute = true // Spawns a new independent console window
        });

        WriteLineLocked($"Cloned new cluster node instance on port {nextPort}.");
    }

    /// <summary>
    /// Polls the cluster manager to observe state changes and redraws the UI.
    /// </summary>
    private static async Task MonitorChangesAsync(IFeatureFlagClusterManager clusterManager, CancellationToken token)
    {
        var lastHash = 0;

        while (!token.IsCancellationRequested)
        {
            var flags = clusterManager.GetFlags();
            var currentHash = ComputeHash(flags);

            if (currentHash != lastHash)
            {
                lastHash = currentHash;
                DrawFlags(flags);
            }

            await Task.Delay(500, token).ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Deterministic hash within the current process lifetime to track dict mutations.
    /// </summary>
    private static int ComputeHash(IReadOnlyDictionary<string, FeatureFlag> flags)
    {
        var hash = new HashCode();
        foreach (var kvp in flags.OrderBy(k => k.Key))
        {
            hash.Add(kvp.Key);
            hash.Add(kvp.Value.IsEnabled);
        }
        return hash.ToHashCode();
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
}