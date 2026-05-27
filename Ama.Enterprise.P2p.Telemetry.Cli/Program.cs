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
using Spectre.Console;
using Spectre.Console.Rendering;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

internal sealed class Program
{
    private static readonly List<string> CachedPeerList = new() { "All" };
    private static int SelectedPeerIndex = 0;
    private static bool HideAdmin = true;
    private static string StatusMessage = "Running normally.";
    private static int MetricsScrollOffset = 0;

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
        services.AddCrdt()
                .AddCrdtSystemTextJson(useBrotliCompression: true);

        services.AddCrdtMessagePack(
            CRDT.MessagePack.Formatters.Ama_Enterprise_CRDT_MessagePack_MessagePackResolver.Instance,
            CRDT.MessagePack.Formatters.Ama_Enterprise_P2p_Telemetry_Cli_MessagePackResolver.Instance
        );

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

            AnsiConsole.Clear();

            var mainLayout = new Layout("Main")
                .SplitRows(
                    new Layout("Header").Size(3),
                    new Layout("Content")
                );

            mainLayout["Content"].SplitColumns(
                new Layout("Left").Ratio(1),
                new Layout("Right").Ratio(7)
            );

            await AnsiConsole.Live(mainLayout)
                .AutoClear(false)
                .Overflow(VerticalOverflow.Ellipsis)
                .Cropping(VerticalOverflowCropping.Bottom)
                .StartAsync(async ctx =>
                {
                    var lastUpdate = DateTime.MinValue;

                    while (!cts.Token.IsCancellationRequested)
                    {
                        var forceUpdate = false;

                        // Non-blocking input loop explicitly avoiding overlapping UI bounds
                        if (Console.KeyAvailable)
                        {
                            var keyInfo = Console.ReadKey(intercept: true);
                            switch (keyInfo.Key)
                            {
                                case ConsoleKey.Q:
                                    cts.Cancel();
                                    break;
                                case ConsoleKey.H:
                                    HideAdmin = !HideAdmin;
                                    MetricsScrollOffset = 0;
                                    forceUpdate = true;
                                    break;
                                case ConsoleKey.UpArrow:
                                    if (SelectedPeerIndex > 0) SelectedPeerIndex--;
                                    MetricsScrollOffset = 0;
                                    forceUpdate = true;
                                    break;
                                case ConsoleKey.DownArrow:
                                    if (SelectedPeerIndex < CachedPeerList.Count - 1) SelectedPeerIndex++;
                                    MetricsScrollOffset = 0;
                                    forceUpdate = true;
                                    break;
                                case ConsoleKey.PageUp:
                                    MetricsScrollOffset -= 10;
                                    forceUpdate = true;
                                    break;
                                case ConsoleKey.PageDown:
                                    MetricsScrollOffset += 10;
                                    forceUpdate = true;
                                    break;
                                case ConsoleKey.Home:
                                    MetricsScrollOffset = 0;
                                    forceUpdate = true;
                                    break;
                                case ConsoleKey.End:
                                    MetricsScrollOffset = int.MaxValue; // Safely clamped in the render function
                                    forceUpdate = true;
                                    break;
                                case ConsoleKey.E:
                                    await ExportToMarkdownAsync(aggregator, metricsAggregator, peerRegistry, HideAdmin, cts.Token).ConfigureAwait(false);
                                    forceUpdate = true;
                                    break;
                            }
                        }

                        if (forceUpdate || DateTime.UtcNow - lastUpdate > TimeSpan.FromSeconds(1))
                        {
                            await UpdateDataAsync(aggregator, metricsAggregator, peerRegistry, mainLayout, cts.Token).ConfigureAwait(false);
                            ctx.Refresh();
                            lastUpdate = DateTime.UtcNow;
                        }

                        // Short delay to keep the input loop highly responsive natively
                        await Task.Delay(50, cts.Token).ConfigureAwait(false);
                    }
                }).ConfigureAwait(false);
        }
        catch (TaskCanceledException) { }
        finally
        {
            AnsiConsole.MarkupLine("[bold yellow]Shutting down Telemetry CLI...[/]");

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
        Layout layout,
        CancellationToken cancellationToken)
    {
        // 1. Fetch valid, connected active peers explicitly mapping bounds preventing phantom nodes evaluation natively
        var activePeers = await peerRegistry.GetPeersByStatusAsync("admin", PeerStatus.Active, cancellationToken).ConfigureAwait(false);
        var activePeerIds = activePeers.Select(p => p.Id.Value).ToHashSet();

        var rawMetrics = aggregator.GetAllNodeMetrics().ToList();

        // 2. Delegate internal historic mathematical projections explicitly to centralized domain limits
        metricsAggregator.ProcessPayloads(rawMetrics);

        // Sanitize selection natively
        if (SelectedPeerIndex >= CachedPeerList.Count)
        {
            SelectedPeerIndex = CachedPeerList.Count - 1;
        }
        var activeSelection = CachedPeerList[SelectedPeerIndex];

        // Map subset evaluating global vs distinct target peers strictly enforcing active topology mappings isolated purely for UI evaluation
        var targetNodeIds = activeSelection == "All"
            ? activePeerIds
            : activePeerIds.Where(id => id.ToString("N").StartsWith(activeSelection)).ToHashSet();

        // Project and compute local/global cluster aggregations (Sum, Max, Min, Per Second, Per Minute)
        var clusterAggregations = metricsAggregator.AggregateClusterMetrics(targetNodeIds);
        var trackedNodeIds = metricsAggregator.GetTrackedNodeIds();

        // Evaluate generic bounds tracking "All" element alongside strictly active dynamic peer evaluations seamlessly
        var updatedPeerList = new List<string> { "All" };
        updatedPeerList.AddRange(trackedNodeIds.Where(k => activePeerIds.Contains(k)).Select(k => k.ToString("N")[..8]).OrderBy(k => k));

        bool listChanged = CachedPeerList.Count != updatedPeerList.Count || !CachedPeerList.SequenceEqual(updatedPeerList);

        if (listChanged)
        {
            CachedPeerList.Clear();
            CachedPeerList.AddRange(updatedPeerList);

            var targetIndex = CachedPeerList.IndexOf(activeSelection);
            if (targetIndex >= 0)
            {
                SelectedPeerIndex = targetIndex;
            }
            else
            {
                SelectedPeerIndex = 0;
                activeSelection = "All";
            }
        }

        var activeStateCount = trackedNodeIds.Count(k => activePeerIds.Contains(k));
        var headerText = $"[bold yellow]Shortcuts:[/] [green]Q[/] Quit | [green]E[/] Export | [green]H[/] Toggle Admin ({(HideAdmin ? "On" : "Off")}) | [green]↑/↓[/] Select Peer | [green]PgUp/PgDn[/] Scroll Metrics\n[bold blue]Status:[/] {StatusMessage}";

        layout["Header"].Update(
            new Panel(new Markup(headerText))
                .Expand()
                .Border(BoxBorder.Rounded)
        );

        layout["Left"].Update(
            new Panel(RenderPeersList(activeStateCount))
                .Header($"Active Peers ({activeStateCount})")
                .Expand()
                .Border(BoxBorder.Rounded)
        );

        layout["Right"].Update(
            new Panel(RenderMetricsTable(clusterAggregations, HideAdmin))
                .Header($"Metrics View: {activeSelection}")
                .Expand()
                .Border(BoxBorder.Rounded)
        );
    }

    private static IRenderable RenderPeersList(int activeStateCount)
    {
        var grid = new Grid();
        grid.AddColumn(new GridColumn().NoWrap());

        for (int i = 0; i < CachedPeerList.Count; i++)
        {
            var peer = CachedPeerList[i];
            if (i == SelectedPeerIndex)
            {
                grid.AddRow(new Markup($"[bold green]> {peer}[/]"));
            }
            else
            {
                grid.AddRow(new Markup($"  {peer}"));
            }
        }

        return grid;
    }

    private static IRenderable RenderMetricsTable(IEnumerable<ClusterMetricAggregation> aggregations, bool hideAdmin)
    {
        var table = new Table()
            .Expand()
            .Border(TableBorder.Minimal)
            .AddColumn("[bold]Metric[/]")
            .AddColumn("[bold]Type[/]")
            .AddColumn(new TableColumn("[bold]Sum[/]").RightAligned())
            .AddColumn(new TableColumn("[bold]Min[/]").RightAligned())
            .AddColumn(new TableColumn("[bold]Max[/]").RightAligned())
            .AddColumn(new TableColumn("[bold]Rate/Sec[/]").RightAligned())
            .AddColumn(new TableColumn("[bold]Rate/Min[/]").RightAligned());

        var visibleAggregations = aggregations;

        if (hideAdmin)
        {
            visibleAggregations = visibleAggregations.Where(agg =>
                !agg.Tags.Any(t => t.Key.Equals("mesh_id", StringComparison.OrdinalIgnoreCase) && 
                                   t.Value.Equals("admin", StringComparison.OrdinalIgnoreCase)));
        }

        // Flatten all generated visual bounds natively tracking generic mapping outputs
        var allRows = new List<string[]>();

        foreach (var agg in visibleAggregations.OrderBy(m => m.Name))
        {
            allRows.Add(new[]
            {
                $"[bold white]{Markup.Escape(agg.Name)}[/]",
                Markup.Escape(agg.Type),
                FormatNumber(agg.Sum),
                FormatNumber(agg.Min),
                FormatNumber(agg.Max),
                FormatNumber(agg.RatePerSecond),
                FormatNumber(agg.RatePerMinute)
            });

            if (agg.Tags.Count > 0)
            {
                var tagsList = agg.Tags.ToList();
                for (int i = 0; i < tagsList.Count; i++)
                {
                    var tag = tagsList[i];
                    var prefix = i == tagsList.Count - 1 ? "  └─ " : "  ├─ ";

                    allRows.Add(new[]
                    {
                        $"[grey]{prefix}{Markup.Escape(tag.Key)}={Markup.Escape(tag.Value)}[/]",
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty,
                        string.Empty
                    });
                }
            }
        }

        // Calculate generic viewport constraints based on runtime terminal scale organically
        var windowHeight = 24;
        try
        {
            windowHeight = Console.WindowHeight;
        }
        catch
        {
            // Fallback for headless environments explicitly ignoring constraints
        }

        var maxVisibleRows = Math.Max(5, windowHeight - 14);

        // Safely bound and clamp scroll vectors naturally resolving distinct page projections
        if (allRows.Count <= maxVisibleRows)
        {
            MetricsScrollOffset = 0;
        }
        else if (MetricsScrollOffset > allRows.Count - maxVisibleRows)
        {
            MetricsScrollOffset = allRows.Count - maxVisibleRows;
        }

        if (MetricsScrollOffset < 0) 
        {
            MetricsScrollOffset = 0;
        }

        var pagedRows = allRows.Skip(MetricsScrollOffset).Take(maxVisibleRows).ToList();

        foreach (var row in pagedRows)
        {
            table.AddRow(row);
        }

        if (allRows.Count > maxVisibleRows)
        {
            var startDisplay = MetricsScrollOffset + 1;
            var endDisplay = MetricsScrollOffset + pagedRows.Count;
            table.Caption($"[grey]Showing rows {startDisplay}-{endDisplay} of {allRows.Count}. Use PgUp/PgDn to scroll.[/]");
        }

        return table;
    }

    private static async Task ExportToMarkdownAsync(
        ITelemetryAggregator aggregator,
        IClusterMetricsAggregator metricsAggregator,
        IPeerRegistry peerRegistry,
        bool hideAdmin,
        CancellationToken cancellationToken)
    {
        try
        {
            var activePeers = await peerRegistry.GetPeersByStatusAsync("admin", PeerStatus.Active, cancellationToken).ConfigureAwait(false);
            var activePeerIds = activePeers.Select(p => p.Id.Value).ToHashSet();

            var rawMetrics = aggregator.GetAllNodeMetrics().ToList();
            metricsAggregator.ProcessPayloads(rawMetrics);

            var activeSelection = CachedPeerList[SelectedPeerIndex];
            var targetNodeIds = activeSelection == "All"
                ? activePeerIds
                : activePeerIds.Where(id => id.ToString("N").StartsWith(activeSelection)).ToHashSet();

            var clusterAggregations = metricsAggregator.AggregateClusterMetrics(targetNodeIds);

            var visibleAggregations = clusterAggregations.AsEnumerable();

            if (hideAdmin)
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

            StatusMessage = $"[bold green]Exported successfully to {fileName}[/]";
        }
        catch (Exception ex)
        {
            StatusMessage = $"[bold red]Export failed: {ex.Message}[/]";
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