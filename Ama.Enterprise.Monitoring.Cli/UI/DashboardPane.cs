namespace Ama.Enterprise.Monitoring.Cli.UI;

using System;
using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

internal sealed class DashboardPane : FrameView
{
    public DashboardPane(View leftPane)
    {
        ArgumentNullException.ThrowIfNull(leftPane);

        Title = "Dashboards";
        X = Pos.Right(leftPane);
        Y = 0;
        Width = Dim.Fill();
        Height = Dim.Fill();

        var tabsLabel = new Label
        {
            Text = "[ Cluster Nodes ]   [ Metrics ]   [ Logs ]   [ Settings ]",
            X = 1,
            Y = 0,
            Width = Dim.Fill()
        };
        
        var tabContentPlaceholder = new View
        {
            X = 0,
            Y = 1,
            Width = Dim.Fill(),
            Height = Dim.Fill()
        };

        Add(tabsLabel, tabContentPlaceholder);
    }
}