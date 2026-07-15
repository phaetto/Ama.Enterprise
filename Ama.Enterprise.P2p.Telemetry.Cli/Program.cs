namespace Ama.Enterprise.P2p.Telemetry.Cli;

using Ama.CRDT.Extensions;
using Ama.Enterprise.CRDT.MessagePack.Extensions;
using Ama.Enterprise.P2p.Extensions;
using Ama.Enterprise.P2p.Models.Core;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Telemetry.Extensions;
using Ama.Enterprise.P2p.Telemetry.Models;
using Ama.Enterprise.P2p.Telemetry.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Terminal.Gui.App;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

internal sealed class Program
{
    private static int selectedPeerIndex = 0;
    private static string currentSelection = "All";
    private static bool hideAdmin = true;
    private static string statusMessage = "Running normally.";

    // Shared thread-safe state ensuring decoupled UI bounds
    private static readonly object stateLock = new object();
    private static HashSet<Guid> latestActivePeerIds = new();
    private static IReadOnlyCollection<ClusterMetricAggregation> latestAggregations = Array.Empty<ClusterMetricAggregation>();
    private static IReadOnlyCollection<Guid> latestTrackedNodeIds = Array.Empty<Guid>();

    private static IList<string> peersList = new List<string>();
    private static IList<string> metricsList = new List<string>();

    // UI Controls
    private static ListView? peersListView;
    private static ListView? metricsListView;
    private static FrameView? leftFrame;
    private static FrameView? rightFrame;
    private static Label? statusLabel;
    private static Label? headerLabel;

    public static async Task Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);

        var services = new ServiceCollection();

        // Suppress logging to avoid overwriting our in-place console UI
        services.AddLogging(builder =>
        {
            builder.ClearProviders();
        });

        // Use random ports so the CLI doesn't conflict with any active host application nodes
        var httpPort = GetNextAvailablePort(9000);
        var handshakePort = GetNextAvailablePort(httpPort + 1);

        // Need base services and serialization
        services.AddCrdt()
                .AddCrdtSystemTextJson(useBrotliCompression: true);

        services.AddCrdtMessagePack(
            CRDT.MessagePack.Resolvers.Ama_Enterprise_CRDT_MessagePack_MessagePackResolver.Instance,
            CRDT.MessagePack.Resolvers.Ama_Enterprise_P2p_Telemetry_Cli_MessagePackResolver.Instance
        );

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<IClusterMetricsAggregator, ClusterMetricsAggregator>();

        // Map identically configured "admin" network boundaries to join the telemetry cluster
        services
            .AddP2pTelemetryAggregator("admin")
            .AddP2pMesh("admin")
            .AddTcpTransport(options =>
            {
                options.ListenPort = httpPort;
                options.ListenHost = "127.0.0.1";
            })
            .AddUdpPeerDiscovery(options =>
            {
                options.MulticastAddress = "239.255.0.3"; // Matches the ShowCase "admin" telemetry mesh bounds
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
            Application.Invoke(() => Application.RequestStop());
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

            peersList.Add("All");

            // Distinct background processor detaching networking, locking, and mathematics from the UI thread
            _ = Task.Run(async () =>
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    try
                    {
                        var activePeers = await peerRegistry.GetPeersByStatusAsync("admin", PeerStatus.Active, cts.Token).ConfigureAwait(false);
                        var activePeerIds = activePeers.Select(p => p.Id.Value).ToHashSet();

                        var rawMetrics = aggregator.GetAllNodeMetrics().ToList();
                        metricsAggregator.ProcessPayloads(rawMetrics);

                        string activeSelection = currentSelection;

                        var targetNodeIds = activeSelection == "All"
                            ? activePeerIds
                            : activePeerIds.Where(id => id.ToString("N").StartsWith(activeSelection)).ToHashSet();

                        var clusterAggregations = metricsAggregator.AggregateClusterMetrics(targetNodeIds).ToList();
                        var trackedNodeIds = metricsAggregator.GetTrackedNodeIds().ToList();

                        lock (stateLock)
                        {
                            latestActivePeerIds = activePeerIds;
                            latestAggregations = clusterAggregations;
                            latestTrackedNodeIds = trackedNodeIds;
                        }

                        // Shift heavy UI string building to the background thread avoiding UI stuttering
                        bool hideAdminLocal = hideAdmin;
                        
                        var updatedPeerList = new List<string> { "All" };
                        updatedPeerList.AddRange(trackedNodeIds.Where(k => activePeerIds.Contains(k)).Select(k => k.ToString("N")[..8]).OrderBy(k => k));

                        var metricsSource = BuildMetricsList(clusterAggregations, hideAdminLocal);

                        Application.Invoke(() => {
                            UpdateUI(updatedPeerList, metricsSource, hideAdminLocal);
                        });
                    }
                    catch (OperationCanceledException) { }
                    catch (Exception ex)
                    {
                        statusMessage = $"Background error: {ex.Message}";
                    }

                    await Task.Delay(500, cts.Token).ConfigureAwait(false);
                }
            }, cts.Token);

            Application.Init();
            
            var top = new Window { 
                Title = "Telemetry CLI", 
                Width = Dim.Fill(), 
                Height = Dim.Fill() 
            };

            var headerFrame = new FrameView {
                Title = "Status",
                X = 0, 
                Y = 0, 
                Width = Dim.Fill(), 
                Height = 4
            };
            
            headerLabel = new Label {
                Text = "Shortcuts: Q Quit | E Export | H Toggle Admin | Tab Switch Panel",
                X = 0, 
                Y = 0, 
                Width = Dim.Fill(), 
                Height = 1
            };
            
            statusLabel = new Label {
                Text = "Status: " + statusMessage,
                X = 0, 
                Y = 1, 
                Width = Dim.Fill(), 
                Height = 1
            };
            
            headerFrame.Add(headerLabel, statusLabel);

            leftFrame = new FrameView {
                Title = "Active Peers (0)",
                X = 0, 
                Y = Pos.Bottom(headerFrame), 
                Width = Dim.Percent(25), 
                Height = Dim.Fill()
            };
            
            peersListView = new ListView {
                X = 0, 
                Y = 0, 
                Width = Dim.Fill(), 
                Height = Dim.Fill()
            };
            peersListView.SetSource(new ObservableCollection<string>(peersList));
            leftFrame.Add(peersListView);

            rightFrame = new FrameView {
                Title = "Metrics View: All",
                X = Pos.Right(leftFrame), 
                Y = Pos.Bottom(headerFrame), 
                Width = Dim.Fill(), 
                Height = Dim.Fill()
            };
            
            metricsListView = new ListView {
                X = 0, 
                Y = 0, 
                Width = Dim.Fill(), 
                Height = Dim.Fill()
            };
            metricsListView.SetSource(new ObservableCollection<string>(metricsList));
            rightFrame.Add(metricsListView);

            top.Add(headerFrame, leftFrame, rightFrame);

            top.KeyDown += (sender, e) => {
                var keyStr = e.ToString()?.ToUpperInvariant() ?? "";
                
                if (keyStr == "Q" || keyStr == "SHIFT+Q") {
                    cts.Cancel();
                    Application.RequestStop();
                    e.Handled = true;
                } else if (keyStr == "H" || keyStr == "SHIFT+H") {
                    hideAdmin = !hideAdmin;
                    // Will update automatically on the next background tick avoiding UI thread blocks
                    e.Handled = true;
                } else if (keyStr == "E" || keyStr == "SHIFT+E") {
                    _ = ExportToMarkdownAsync(hideAdmin, cts.Token);
                    e.Handled = true;
                }
            };

            Application.Run(top);
            Application.Shutdown();
        }
        catch (TaskCanceledException) { }
        finally
        {
            Console.WriteLine("Shutting down Telemetry CLI...");

            foreach (var service in hostedServices)
            {
                await service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private static void UpdateUI(IList<string> updatedPeerList, IList<string> metricsSource, bool hideAdminLocal)
    {
        ArgumentNullException.ThrowIfNull(updatedPeerList);
        ArgumentNullException.ThrowIfNull(metricsSource);

        if (statusLabel != null)
        {
            statusLabel.Text = "Status: " + statusMessage;
        }

        if (peersListView != null)
        {
            // Extracted value verifying nullable bounds evaluating int
            var currentSelectedItem = peersListView.SelectedItem;
            if (currentSelectedItem.HasValue && currentSelectedItem.Value >= 0 && currentSelectedItem.Value < peersList.Count)
            {
                selectedPeerIndex = currentSelectedItem.Value;
            }
        }

        if (selectedPeerIndex >= peersList.Count && peersList.Count > 0)
        {
            selectedPeerIndex = Math.Max(0, peersList.Count - 1);
        }
        
        var activeSelection = peersList.Count > 0 && selectedPeerIndex >= 0 ? peersList[selectedPeerIndex] : "All";
        currentSelection = activeSelection;

        bool listChanged = peersList.Count != updatedPeerList.Count || !peersList.SequenceEqual(updatedPeerList);

        if (listChanged)
        {
            peersList = updatedPeerList;
            
            var targetIndex = peersList.IndexOf(activeSelection);
            if (targetIndex >= 0)
            {
                selectedPeerIndex = targetIndex;
            }
            else
            {
                selectedPeerIndex = 0;
                activeSelection = "All";
                currentSelection = activeSelection;
            }

            if (peersListView != null)
            {
                // Replaces the source atomically bypassing layout events caused by ObservableCollection
                peersListView.SetSource(new ObservableCollection<string>(peersList));
                if (selectedPeerIndex >= 0 && selectedPeerIndex < peersList.Count)
                {
                    peersListView.SelectedItem = selectedPeerIndex;
                }
                
                peersListView.SetNeedsDraw();
            }
        }

        var activeStateCount = Math.Max(0, updatedPeerList.Count - 1);
        
        if (leftFrame != null)
            leftFrame.Title = $"Active Peers ({activeStateCount})";
            
        if (rightFrame != null)
            rightFrame.Title = $"Metrics View: {activeSelection} " + (hideAdminLocal ? "(Admin Hidden)" : "(Admin Shown)");

        if (metricsListView != null)
        {
            bool metricsChanged = metricsList.Count != metricsSource.Count || !metricsList.SequenceEqual(metricsSource);
            
            if (metricsChanged)
            {
                metricsList = metricsSource;
                
                // Track scrolling positions preserving UI layout across atomic source assignments
                var selectedItem = metricsListView.SelectedItem;
                
                // Atomic List updates avoid rendering calculation per added element maximizing frame rates
                metricsListView.SetSource(new ObservableCollection<string>(metricsList));
                
                if (selectedItem.HasValue && selectedItem.Value < metricsList.Count)
                {
                    metricsListView.SelectedItem = selectedItem.Value;
                }
                    
                metricsListView.SetNeedsDraw();
            }
        }
    }

    private static IList<string> BuildMetricsList(IEnumerable<ClusterMetricAggregation> aggregations, bool adminHidden)
    {
        ArgumentNullException.ThrowIfNull(aggregations);

        var list = new List<string>
        {
            string.Format("{0,-35} | {1,-10} | {2,12} | {3,12} | {4,12} | {5,12} | {6,12}",
                "Metric", "Type", "Sum", "Min", "Max", "Rate/Sec", "Rate/Min"),
            new string('-', 125)
        };

        var visibleAggregations = aggregations;

        if (adminHidden)
        {
            visibleAggregations = visibleAggregations.Where(agg =>
                !agg.Tags.Any(t => t.Key.Equals("mesh_id", StringComparison.OrdinalIgnoreCase) && 
                                   t.Value.Equals("admin", StringComparison.OrdinalIgnoreCase)));
        }

        foreach (var agg in visibleAggregations.OrderBy(m => m.Name))
        {
            list.Add(string.Format("{0,-35} | {1,-10} | {2,12} | {3,12} | {4,12} | {5,12} | {6,12}",
                Truncate(agg.Name, 35),
                Truncate(agg.Type, 10),
                FormatNumber(agg.Sum),
                FormatNumber(agg.Min),
                FormatNumber(agg.Max),
                FormatNumber(agg.RatePerSecond),
                FormatNumber(agg.RatePerMinute)));

            if (agg.Tags.Count > 0)
            {
                var tagsList = agg.Tags.ToList();
                for (int i = 0; i < tagsList.Count; i++)
                {
                    var prefix = (i == tagsList.Count - 1) ? "  └─ " : "  ├─ ";
                    var tagText = $"{prefix}{tagsList[i].Key}={tagsList[i].Value}";
                    list.Add(tagText);
                }
            }
        }

        return list;
    }

    private static string Truncate(string value, int maxChars)
    {
        if (string.IsNullOrEmpty(value)) return string.Empty;
        if (maxChars <= 3) return value;
        return value.Length <= maxChars ? value : value.Substring(0, maxChars - 3) + "...";
    }

    private static async Task ExportToMarkdownAsync(bool adminHidden, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            HashSet<Guid> activePeerIds;
            IReadOnlyCollection<ClusterMetricAggregation> clusterAggregations;

            lock (stateLock)
            {
                activePeerIds = latestActivePeerIds;
                clusterAggregations = latestAggregations;
            }

            var activeSelection = currentSelection;
            var targetNodeIds = activeSelection == "All"
                ? activePeerIds
                : activePeerIds.Where(id => id.ToString("N").StartsWith(activeSelection)).ToHashSet();

            var visibleAggregations = clusterAggregations.AsEnumerable();

            if (adminHidden)
            {
                visibleAggregations = visibleAggregations.Where(agg =>
                    !agg.Tags.Any(t => t.Key.Equals("mesh_id", StringComparison.OrdinalIgnoreCase) && 
                                       t.Value.Equals("admin", StringComparison.OrdinalIgnoreCase)));
            }

            var sb = new StringBuilder();
            sb.AppendLine($"# Telemetry Export - {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
            sb.AppendLine($"**View:** {activeSelection} | **Active Peers:** {targetNodeIds.Count}");
            sb.AppendLine();
            sb.AppendLine("| Metric | Type | Sum | Min | Max | Rate/Sec | Rate/Min | Tags |");
            sb.AppendLine("|---|---|---|---|---|---|---|---|");

            foreach (var agg in visibleAggregations.OrderBy(m => m.Name))
            {
                var tagsStr = string.Join(", ", agg.Tags.Select(t => $"{t.Key}={t.Value}"));
                sb.AppendLine($"| {agg.Name} | {agg.Type} | {FormatNumber(agg.Sum)} | {FormatNumber(agg.Min)} | {FormatNumber(agg.Max)} | {FormatNumber(agg.RatePerSecond)} | {FormatNumber(agg.RatePerMinute)} | {tagsStr} |");
            }

            var fileName = $"telemetry_export_{DateTime.Now:yyyyMMdd_HHmmss}.md";
            var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, fileName);

            await File.WriteAllTextAsync(filePath, sb.ToString(), cancellationToken).ConfigureAwait(false);

            statusMessage = $"Exported successfully to {fileName}";
        }
        catch (Exception ex)
        {
            statusMessage = $"Export failed: {ex.Message}";
        }
    }

    private static int GetNextAvailablePort(int startingPort)
    {
        if (startingPort < 0 || startingPort > 65535)
        {
            startingPort = 9000;
        }

        var ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();

        var activeTcpPorts = ipGlobalProperties.GetActiveTcpListeners().Select(l => l.Port);
        var activeUdpPorts = ipGlobalProperties.GetActiveUdpListeners().Select(l => l.Port);

        var activePorts = activeTcpPorts.Concat(activeUdpPorts).ToHashSet();

        var port = startingPort;
        while (activePorts.Contains(port) && port < 65535)
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