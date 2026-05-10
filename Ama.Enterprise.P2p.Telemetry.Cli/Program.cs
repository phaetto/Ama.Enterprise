namespace Ama.Enterprise.P2p.Telemetry.Cli;

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.NetworkInformation;
using System.Threading;
using System.Threading.Tasks;
using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Telemetry.Extensions;
using Ama.Enterprise.P2p.Telemetry.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Terminal.Gui;

internal sealed class Program
{
    private static readonly Dictionary<Guid, NodeTelemetryState> NetworkState = new();
    private static readonly List<string> CachedPeerList = new() { "All" };
    private static string SelectedPeerId = "All";

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
            Application.RequestStop();
        };

        try
        {
            foreach (var service in hostedServices)
            {
                await service.StartAsync(cts.Token).ConfigureAwait(false);
            }

            var aggregator = provider.GetRequiredService<ITelemetryAggregator>();
            var peerRegistry = provider.GetRequiredService<IPeerRegistry>();

            Application.Init();
            var top = Application.Top;

            var window = new Window("P2P Cluster Telemetry Dashboard ('admin' mesh)")
            {
                X = 0,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill()
            };

            var peersFrame = new FrameView("Active Peers (0)")
            {
                X = 0,
                Y = 0,
                Width = Dim.Percent(25),
                Height = Dim.Fill()
            };

            var peersListView = new ListView(CachedPeerList)
            {
                X = 0,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill()
            };

            // Track selection persistently bypassing list resets bridging UI selections natively
            peersListView.SelectedItemChanged += (e) =>
            {
                if (e.Item >= 0 && e.Item < CachedPeerList.Count)
                {
                    SelectedPeerId = CachedPeerList[e.Item];
                }
            };

            peersFrame.Add(peersListView);

            var metricsFrame = new FrameView("Cluster Metrics Aggregation")
            {
                X = Pos.Right(peersFrame),
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill()
            };

            var dataTable = new DataTable();
            dataTable.Columns.Add("Metric", typeof(string));
            dataTable.Columns.Add("Type", typeof(string));
            dataTable.Columns.Add("Sum", typeof(string));
            dataTable.Columns.Add("Min", typeof(string));
            dataTable.Columns.Add("Max", typeof(string));
            dataTable.Columns.Add("Rate/Sec", typeof(string));
            dataTable.Columns.Add("Rate/Min", typeof(string));

            var tableView = new TableView()
            {
                X = 0,
                Y = 0,
                Width = Dim.Fill(),
                Height = Dim.Fill(),
                Table = dataTable,
                FullRowSelect = true
            };
            metricsFrame.Add(tableView);

            window.Add(peersFrame, metricsFrame);
            top.Add(window);

            // Ensure CTRL+Q, CTRL+C or ESC exits the Terminal.Gui loop cleanly
            top.KeyPress += (e) =>
            {
                if (e.KeyEvent.Key == (Key.Q | Key.CtrlMask) ||
                    e.KeyEvent.Key == (Key.C | Key.CtrlMask) ||
                    e.KeyEvent.Key == Key.Esc)
                {
                    cts.Cancel();
                    Application.RequestStop();
                    e.Handled = true;
                }
            };

            _ = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        await Task.Delay(1000, cts.Token).ConfigureAwait(false);
                        await UpdateDataAsync(aggregator, peerRegistry, dataTable, peersListView, window, peersFrame, tableView, cts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                }
            }, cts.Token);

            Application.Run();
            Application.Shutdown();
        }
        finally
        {
            Console.WriteLine("Shutting down Telemetry CLI...");

            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private static async Task UpdateDataAsync(
        ITelemetryAggregator aggregator,
        IPeerRegistry peerRegistry,
        DataTable dataTable,
        ListView peersListView,
        Window window,
        FrameView peersFrame,
        TableView tableView,
        CancellationToken cancellationToken)
    {
        // 1. Fetch valid, connected active peers explicitly mapping bounds preventing phantom nodes evaluation natively
        var activePeers = await peerRegistry.GetPeersByStatusAsync("admin", PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        var activePeerIds = activePeers.Select(p => p.Id.Value).ToHashSet();
        
        var rawMetrics = aggregator.GetAllNodeMetrics().ToList();
        var currentTelemetryNodes = rawMetrics.Select(m => m.NodeId).ToHashSet();

        // 2. Remove nodes only if they are no longer reported by the aggregator, bypassing transient active state drops ensuring deltas are preserved
        var departedNodes = NetworkState.Keys
            .Where(k => !currentTelemetryNodes.Contains(k))
            .ToList();
            
        foreach (var departed in departedNodes)
        {
            NetworkState.Remove(departed);
        }

        // 3. Update local timeseries histories aligning DateTimeOffset bounds for all incoming payloads unconditionally
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

        // Capture current selected peer ID explicitly preventing thread overlaps
        var activeSelection = SelectedPeerId;

        // Map subset evaluating global vs distinct target peers strictly enforcing active topology mappings isolated purely for UI evaluation
        var targetNodes = activeSelection == "All"
            ? NetworkState.Values.Where(n => activePeerIds.Contains(n.NodeId))
            : NetworkState.Values.Where(n => activePeerIds.Contains(n.NodeId) && n.NodeId.ToString("N").StartsWith(activeSelection));

        // Project and compute local/global cluster aggregations (Sum, Max, Min, Per Second, Per Minute)
        var clusterMetrics = new Dictionary<string, ClusterMetricAggregation>();

        foreach (var nodeState in targetNodes)
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

        Application.MainLoop.Invoke(() =>
        {
            // Evaluate generic bounds tracking "All" element alongside strictly active dynamic peer evaluations seamlessly
            var updatedPeerList = new List<string> { "All" };
            updatedPeerList.AddRange(NetworkState.Keys.Where(k => activePeerIds.Contains(k)).Select(k => k.ToString("N")[..8]).OrderBy(k => k));

            bool listChanged = CachedPeerList.Count != updatedPeerList.Count || !CachedPeerList.SequenceEqual(updatedPeerList);

            if (listChanged)
            {
                CachedPeerList.Clear();
                CachedPeerList.AddRange(updatedPeerList);
                peersListView.SetSource(CachedPeerList);

                // Reapply mapped bounds tracking natively preserving item focus strictly
                var targetIndex = CachedPeerList.IndexOf(activeSelection);
                if (targetIndex >= 0)
                {
                    peersListView.SelectedItem = targetIndex;
                }
                else
                {
                    peersListView.SelectedItem = 0;
                    SelectedPeerId = "All";
                    activeSelection = "All";
                }
            }

            var activeStateCount = NetworkState.Keys.Count(k => activePeerIds.Contains(k));
            peersFrame.Title = $"Active Peers ({activeStateCount})";
            window.Title = $"P2P Cluster Telemetry Dashboard ('admin' mesh) - Connections: {activeStateCount} | View: {activeSelection}";

            dataTable.Rows.Clear();

            foreach (var agg in clusterMetrics.Values.OrderBy(m => m.Name))
            {
                var min = agg.Min == double.MaxValue ? 0 : agg.Min;
                var max = agg.Max == double.MinValue ? 0 : agg.Max;

                dataTable.Rows.Add(
                    agg.Name,
                    agg.Type,
                    agg.Sum.ToString("0.##"),
                    min.ToString("0.##"),
                    max.ToString("0.##"),
                    agg.RatePerSecond.ToString("0.##"),
                    agg.RatePerMinute.ToString("0.##")
                );

                // Add a distinct indented row for each tag individually using a tree layout mapping explicitly
                if (agg.Tags.Count > 0)
                {
                    var tagsList = agg.Tags.ToList();
                    for (int i = 0; i < tagsList.Count; i++)
                    {
                        var tag = tagsList[i];
                        var prefix = i == tagsList.Count - 1 ? "  └─ " : "  ├─ ";
                        
                        dataTable.Rows.Add(
                            $"{prefix}{tag.Key}={tag.Value}",
                            string.Empty,
                            string.Empty,
                            string.Empty,
                            string.Empty,
                            string.Empty,
                            string.Empty
                        );
                    }
                }
            }

            tableView.SetNeedsDisplay();
        });
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

    private readonly record struct MetricHistoryEntry(DateTimeOffset LocalTime, DateTimeOffset ServerTime, double Value);

    private sealed class NodeTelemetryState
    {
        public Guid NodeId { get; set; }
        public DateTimeOffset LastSeen { get; set; }
        public Dictionary<string, MetricStats> Metrics { get; } = new();
    }

    private sealed class MetricStats
    {
        public string Name { get; set; } = string.Empty;
        public string Type { get; set; } = string.Empty;
        public Dictionary<string, string> Tags { get; set; } = new();

        public double CurrentValue { get; private set; }

        private readonly Queue<MetricHistoryEntry> history = new();

        public void Update(double value, DateTimeOffset serverTime)
        {
            var now = DateTimeOffset.UtcNow;

            // Stop compounding identical payload entries if the origin server timestamp remains identical avoiding P2P network duplications natively
            if (history.Count > 0 && history.Last().ServerTime >= serverTime)
            {
                Prune(now);
                return;
            }

            bool isObservable = Type.Contains("Observable", StringComparison.OrdinalIgnoreCase);
            bool isCounter = Type.Contains("Counter", StringComparison.OrdinalIgnoreCase);
            bool isDelta = isCounter && !isObservable;

            // Natively resolve raw delta measurements accumulating bounds for absolute structural UI tracking
            if (isDelta)
            {
                CurrentValue += value;
            }
            else
            {
                CurrentValue = value;
            }

            history.Enqueue(new MetricHistoryEntry(now, serverTime, CurrentValue));
            Prune(now);
        }

        private void Prune(DateTimeOffset now)
        {
            // Retain up to 10 minutes rolling window for rate extrapolations locally
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

            bool isUpDown = Type.Contains("UpDown", StringComparison.OrdinalIgnoreCase);

            // Re-alignment for standard counter resets discarding temporary negative slopes
            if (Type.Contains("Counter", StringComparison.OrdinalIgnoreCase) && !isUpDown && diff < 0) diff = 0;

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

            bool isUpDown = Type.Contains("UpDown", StringComparison.OrdinalIgnoreCase);

            if (Type.Contains("Counter", StringComparison.OrdinalIgnoreCase) && !isUpDown && diff < 0) diff = 0;

            return diff / minutes;
        }
    }

    private sealed class ClusterMetricAggregation
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