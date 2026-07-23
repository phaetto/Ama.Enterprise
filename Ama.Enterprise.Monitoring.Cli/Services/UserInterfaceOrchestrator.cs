namespace Ama.Enterprise.Monitoring.Cli.Services;

using Ama.Enterprise.Monitoring.Cli.UI;

internal sealed class UserInterfaceOrchestrator : IUserInterfaceOrchestrator
{
    public void Run()
    {
        using var app = Terminal.Gui.App.Application.Create();
        app.Init();

        var mainWindow = new MainWindow();
        app.Run(mainWindow);
    }
}