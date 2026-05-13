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

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<IClusterMetricsAggregator, ClusterMetricsAggregator>();

        // Map identically configured "admin" network boundaries to join the telemetry cluster natively
        services
            .AddP2pMesh("admin")
            .AddTcpTransport(options =>
            {
                options.ListenPort = httpPort;
                options.ListenHost = "127.0.0.1";
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
            var metricsAggregator = provider.GetRequiredService<IClusterMetricsAggregator>();
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

            var hideTelemetryMeshCheckbox = new CheckBox("Hide Telemetry Mesh ('admin') Metrics")
            {
                X = 0,
                Y = 0,
                Checked = true
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
                Y = Pos.Bottom(hideTelemetryMeshCheckbox),
                Width = Dim.Fill(),
                Height = Dim.Fill(),
                Table = dataTable,
                FullRowSelect = true
            };
            
            metricsFrame.Add(hideTelemetryMeshCheckbox, tableView);

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
                        await UpdateDataAsync(aggregator, metricsAggregator, peerRegistry, dataTable, peersListView, window, peersFrame, tableView, hideTelemetryMeshCheckbox, cts.Token).ConfigureAwait(false);
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
        IClusterMetricsAggregator metricsAggregator,
        IPeerRegistry peerRegistry,
        DataTable dataTable,
        ListView peersListView,
        Window window,
        FrameView peersFrame,
        TableView tableView,
        CheckBox hideTelemetryMeshCheckbox,
        CancellationToken cancellationToken)
    {
        // 1. Fetch valid, connected active peers explicitly mapping bounds preventing phantom nodes evaluation natively
        var activePeers = await peerRegistry.GetPeersByStatusAsync("admin", PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        var activePeerIds = activePeers.Select(p => p.Id.Value).ToHashSet();
        
        var rawMetrics = aggregator.GetAllNodeMetrics().ToList();
        
        // 2. Delegate internal historic mathematical projections explicitly to centralized domain limits
        metricsAggregator.ProcessPayloads(rawMetrics);

        // Capture current selected peer ID explicitly preventing thread overlaps
        var activeSelection = SelectedPeerId;

        // Map subset evaluating global vs distinct target peers strictly enforcing active topology mappings isolated purely for UI evaluation
        var targetNodeIds = activeSelection == "All"
            ? activePeerIds
            : activePeerIds.Where(id => id.ToString("N").StartsWith(activeSelection)).ToHashSet();

        // Project and compute local/global cluster aggregations (Sum, Max, Min, Per Second, Per Minute)
        var clusterAggregations = metricsAggregator.AggregateClusterMetrics(targetNodeIds);
        var trackedNodeIds = metricsAggregator.GetTrackedNodeIds();

        Application.MainLoop.Invoke(() =>
        {
            // Evaluate generic bounds tracking "All" element alongside strictly active dynamic peer evaluations seamlessly
            var updatedPeerList = new List<string> { "All" };
            updatedPeerList.AddRange(trackedNodeIds.Where(k => activePeerIds.Contains(k)).Select(k => k.ToString("N")[..8]).OrderBy(k => k));

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

            var activeStateCount = trackedNodeIds.Count(k => activePeerIds.Contains(k));
            peersFrame.Title = $"Active Peers ({activeStateCount})";
            window.Title = $"P2P Cluster Telemetry Dashboard ('admin' mesh) - Connections: {activeStateCount} | View: {activeSelection}";

            dataTable.Rows.Clear();

            bool hideAdmin = hideTelemetryMeshCheckbox.Checked;
            var visibleAggregations = clusterAggregations.AsEnumerable();

            if (hideAdmin)
            {
                visibleAggregations = visibleAggregations.Where(agg =>
                    !agg.Tags.Any(t => t.Key.Equals("mesh_id", StringComparison.OrdinalIgnoreCase) && 
                                       t.Value.Equals("admin", StringComparison.OrdinalIgnoreCase)));
            }

            foreach (var agg in visibleAggregations.OrderBy(m => m.Name))
            {
                dataTable.Rows.Add(
                    agg.Name,
                    agg.Type,
                    FormatNumber(agg.Sum),
                    FormatNumber(agg.Min),
                    FormatNumber(agg.Max),
                    FormatNumber(agg.RatePerSecond),
                    FormatNumber(agg.RatePerMinute)
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

            tableView.Update();
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

    private static string FormatNumber(double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value))
        {
            return "0";
        }

        var absValue = Math.Abs(value);

        if (absValue >= 1_000_000_000_000)
        {
            return (value / 1_000_000_000_000D).ToString("0.##") + "T";
        }
        
        if (absValue >= 1_000_000_000)
        {
            return (value / 1_000_000_000D).ToString("0.##") + "B";
        }
        
        if (absValue >= 1_000_000)
        {
            return (value / 1_000_000D).ToString("0.##") + "M";
        }
        
        if (absValue >= 1_000)
        {
            return (value / 1_000D).ToString("0.##") + "K";
        }

        return value.ToString("0.##");
    }
}