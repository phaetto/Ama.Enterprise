namespace Ama.Enterprise.Monitoring.Cli.UI;

using Terminal.Gui.ViewBase;
using Terminal.Gui.Views;

internal sealed class MainWindow : Window
{
    public MainWindow()
    {
        Title = "Monitoring CLI";
        Width = Dim.Fill();
        Height = Dim.Fill();

        var leftPane = new CommandPane();
        var rightPane = new DashboardPane(leftPane);

        Add(leftPane, rightPane);
    }
}