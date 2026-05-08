namespace Ama.Enterprise.P2p.Telemetry.Cli;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Telemetry.Extensions;
using Ama.Enterprise.P2p.Telemetry.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

internal class Program
{
    private static readonly Dictionary<Guid, NodeTelemetryState> NetworkState = new();

    public static async Task Main(string[] args)
    {
        var services = new ServiceCollection();

        // Suppress logging to avoid overwriting our in-place console UI
        services.AddLogging(builder =>
        {
            builder.ClearProviders();
        });

        // Set up the centralized telemetry aggregator explicitly targeting the "admin" mesh
        services.AddP2pTelemetryAggregator("admin");

        // Use random ports so the CLI doesn't conflict with any active host application nodes
        var httpPort = GetNextAvailablePort(9000);
        var handshakePort = GetNextAvailablePort(httpPort + 1);

        // Need base services and serialization
        services.AddCrdt();

        // Map identically configured "admin" network boundaries to join the telemetry cluster natively
        services
            .AddP2pMesh("admin")
            .AddHttpTransport(options =>
            {
                options.ListenPort = httpPort;
                options.ListenHost = "localhost";
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = "239.255.0.3"; // Explicitly matches the ShowCase "admin" telemetry mesh bounds
                options.MulticastPort = 8036;
                options.DiscoveryInterval = TimeSpan.FromSeconds(1);
                options.DiscoveryTimeout = TimeSpan.FromSeconds(1);
            })
            .AddUdpPeerHandshake(options =>
            {
                options.ListenPort = handshakePort;
            })
            .ConfigureFailureDetector(options =>
            {
                options.HeartbeatInterval = TimeSpan.FromSeconds(5);
            });

        await using var provider = services.BuildServiceProvider();
        var hostedServices = provider.GetServices<IHostedService>().ToList();
        using var cts = new CancellationTokenSource();

        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true;
            cts.Cancel();
        };

        try
        {
            foreach (var service in hostedServices)
            {
                await service.StartAsync(cts.Token).ConfigureAwait(false);
            }

            var aggregator = provider.GetRequiredService<ITelemetryAggregator>();
            
            PrepareConsole();

            while (!cts.Token.IsCancellationRequested)
            {
                DrawDashboard(aggregator);
                
                try
                {
                    await Task.Delay(1000, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        finally
        {
            RestoreConsole();
            
            Console.WriteLine("Shutting down Telemetry CLI...");
            
            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private static void DrawDashboard(ITelemetryAggregator aggregator)
    {
        if (aggregator is null)
        {
            return;
        }

        bool canUseCursor = true;
        int windowHeight = 24;
        try
        {
            windowHeight = Console.WindowHeight;
        }
        catch
        {
            canUseCursor = false;
        }
        
        var screenWidth = GetConsoleWidth();
        int currentLine = 0;
        
        void WriteLinePadded(string message)
        {
            if (canUseCursor && currentLine >= windowHeight)
            {
                return; // Prevent writing past the visible console area and scrolling
            }

            string formattedMessage = message.Length < screenWidth 
                ? message.PadRight(screenWidth) 
                : message.Substring(0, screenWidth);

            if (canUseCursor)
            {
                try
                {
                    Console.SetCursorPosition(0, currentLine);
                    Console.Write(formattedMessage);
                }
                catch
                {
                    Console.WriteLine(formattedMessage);
                    canUseCursor = false;
                }
            }
            else
            {
                Console.WriteLine(formattedMessage);
            }

            currentLine++;
        }

        WriteLinePadded("======================================================================");
        WriteLinePadded(" P2P CLUSTER TELEMETRY DASHBOARD ('admin' mesh)");
        WriteLinePadded(" Press CTRL+C to exit.");
        WriteLinePadded("======================================================================");
        WriteLinePadded("");

        var rawMetrics = aggregator.GetAllNodeMetrics().ToList();
        
        // Remove nodes that are no longer present in the aggregated payload state utilizing Guid tracking
        var currentNodes = rawMetrics.Select(m => m.NodeId).ToHashSet();
        var departedNodes = NetworkState.Keys.Where(k => !currentNodes.Contains(k)).ToList();
        foreach (var departed in departedNodes)
        {
            NetworkState.Remove(departed);
        }

        // Update local timeseries histories aligning DateTimeOffset bounds
        foreach (var node in rawMetrics)
        {
            if (!NetworkState.TryGetValue(node.NodeId, out var state))
            {
                state = new NodeTelemetryState { NodeId = node.NodeId };
                NetworkState[node.NodeId] = state;
            }

            state.LastSeen = node.Timestamp.ToLocalTime();

            foreach (var metric in node.Metrics)
            {
                var tagDict = new Dictionary<string, string>();
                foreach (var tag in metric.Tags)
                {
                    tagDict[tag.Key?.ToString() ?? ""] = tag.Value?.ToString() ?? "";
                }

                var tagSuffix = string.Join("|", tagDict.OrderBy(t => t.Key).Select(t => $"{t.Key}:{t.Value}"));
                var metricKey = $"{metric.Name}[{tagSuffix}]";

                if (!state.Metrics.TryGetValue(metricKey, out var stats))
                {
                    stats = new MetricStats 
                    { 
                        Name = metric.Name, 
                        Type = metric.Type, 
                        Tags = tagDict 
                    };
                    state.Metrics[metricKey] = stats;
                }

                stats.Update(Convert.ToDouble(metric.Value), node.Timestamp);
            }
        }

        // Project and compute global cluster aggregations (Sum, Max, Min, Per Second, Per Minute)
        var clusterMetrics = new Dictionary<string, ClusterMetricAggregation>();

        foreach (var nodeState in NetworkState.Values)
        {
            foreach (var kvp in nodeState.Metrics)
            {
                var metricKey = kvp.Key;
                var stats = kvp.Value;

                if (!clusterMetrics.TryGetValue(metricKey, out var agg))
                {
                    agg = new ClusterMetricAggregation
                    {
                        Name = stats.Name,
                        Type = stats.Type,
                        Tags = stats.Tags
                    };
                    clusterMetrics[metricKey] = agg;
                }

                agg.NodeCount++;
                agg.Sum += stats.CurrentValue;
                if (stats.CurrentValue < agg.Min) agg.Min = stats.CurrentValue;
                if (stats.CurrentValue > agg.Max) agg.Max = stats.CurrentValue;
                
                agg.RatePerSecond += stats.GetPerSecond();
                agg.RatePerMinute += stats.GetPerMinute();
            }
        }
        
        if (NetworkState.Count == 0)
        {
            WriteLinePadded(" Waiting for incoming network telemetry payloads...");
        }
        else
        {
            var activeNodesStr = string.Join(", ", NetworkState.Keys.OrderBy(k => k).Select(k => k.ToString("N").Substring(0, 8)));
            WriteLinePadded($" Active Nodes ({NetworkState.Count}): {activeNodesStr}");
            WriteLinePadded(" ----------------------------------------------------------------------");
            WriteLinePadded("");

            if (clusterMetrics.Count == 0)
            {
                WriteLinePadded("   (No metrics available)");
            }
            else
            {
                foreach (var agg in clusterMetrics.Values.OrderBy(m => m.Name))
                {
                    var min = agg.Min == double.MaxValue ? 0 : agg.Min;
                    var max = agg.Max == double.MinValue ? 0 : agg.Max;
                    
                    WriteLinePadded($" -> [{agg.Type}] {agg.Name}");
                    WriteLinePadded($"    * Cluster Sum: {agg.Sum:0.##} | Node Min: {min:0.##} | Node Max: {max:0.##}");
                    WriteLinePadded($"    * Global Rate: {agg.RatePerSecond:0.##}/sec | {agg.RatePerMinute:0.##}/min");

                    if (agg.Tags.Count > 0)
                    {
                        var tagsStr = string.Join(", ", agg.Tags.Select(t => $"{t.Key}: {t.Value}"));
                        WriteLinePadded($"    * Tags:        {tagsStr}");
                    }
                    WriteLinePadded("");
                }
            }
        }

        // Pad the remainder of the view to clear any stale rows preventing display artifacts
        if (canUseCursor)
        {
            while (currentLine < windowHeight)
            {
                WriteLinePadded("");
            }
        }
    }

    private static int GetNextAvailablePort(int startingPort)
    {
        var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
        
        var activeTcpPorts = ipGlobalProperties.GetActiveTcpListeners().Select(l => l.Port);
        var activeUdpPorts = ipGlobalProperties.GetActiveUdpListeners().Select(l => l.Port);

        var activePorts = activeTcpPorts.Concat(activeUdpPorts).ToHashSet();

        var port = startingPort;
        while (activePorts.Contains(port))
        {
            port++;
        }

        return port;
    }

    private static int GetConsoleWidth()
    {
        try
        {
            return Console.WindowWidth > 1 ? Console.WindowWidth - 1 : 80;
        }
        catch
        {
            return 80;
        }
    }

    private static void PrepareConsole()
    {
        try
        {
            Console.Clear();
            Console.CursorVisible = false;
        }
        catch
        {
            // Ignore if console manipulation isn't supported (e.g. redirected or no TTY)
        }
    }

    private static void RestoreConsole()
    {
        try
        {
            Console.CursorVisible = true;
            Console.Clear();
        }
        catch
        {
            // Fallback for terminal restoration failures
        }
    }

    private class NodeTelemetryState
    {
        public Guid NodeId { get; set; }
        public DateTimeOffset LastSeen { get; set; }
        public Dictionary<string, MetricStats> Metrics { get; } = new();
    }

    private class MetricStats
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public Dictionary<string, string> Tags { get; set; } = new();

        public double CurrentValue { get; private set; }

        private readonly Queue<(DateTimeOffset LocalTime, DateTimeOffset ServerTime, double Value)> history = new();

        public void Update(double value, DateTimeOffset serverTime)
        {
            var now = DateTimeOffset.UtcNow;

            // Stop compounding empty entries if the origin server timestamp remains identical
            if (history.Count > 0 && history.Last().ServerTime >= serverTime)
            {
                Prune(now);
                return;
            }

            CurrentValue = value;
            history.Enqueue((now, serverTime, value));
            Prune(now);
        }

        private void Prune(DateTimeOffset now)
        {
            // Retain up to 60 seconds rolling window for rate extrapolations
            while (history.Count > 0 && (now - history.Peek().LocalTime).TotalMinutes > 10)
            {
                history.Dequeue();
            }
        }

        public double GetPerSecond()
        {
            var now = DateTimeOffset.UtcNow;
            Prune(now);

            if (history.Count < 2) return 0;
            var first = history.Peek();
            var last = history.Last();
            
            var seconds = Math.Max(1.0, (now - first.LocalTime).TotalSeconds);
            var diff = last.Value - first.Value;
            
            // Re-alignment for standard counter resets discarding temporary negative slopes
            if (Type.Contains("Counter", StringComparison.OrdinalIgnoreCase) && diff < 0) diff = 0;
            
            return diff / seconds;
        }

        public double GetPerMinute()
        {
            var now = DateTimeOffset.UtcNow;
            Prune(now);

            if (history.Count < 2) return 0;
            var first = history.Peek();
            var last = history.Last();

            var minutes = Math.Max(1.0 / 60.0, (now - first.LocalTime).TotalMinutes);
            var diff = last.Value - first.Value;
            
            if (Type.Contains("Counter", StringComparison.OrdinalIgnoreCase) && diff < 0) diff = 0;

            return diff / minutes;
        }
    }

    private class ClusterMetricAggregation
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public Dictionary<string, string> Tags { get; set; } = new();

        public int NodeCount { get; set; }
        public double Sum { get; set; }
        public double Min { get; set; } = double.MaxValue;
        public double Max { get; set; } = double.MinValue;
        public double RatePerSecond { get; set; }
        public double RatePerMinute { get; set; }
    }
}