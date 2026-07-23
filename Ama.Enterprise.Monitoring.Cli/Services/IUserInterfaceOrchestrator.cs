namespace Ama.Enterprise.Monitoring.Cli.Services;

/// <summary>
/// Orchestrates the execution of the command-line user interface.
/// </summary>
public interface IUserInterfaceOrchestrator
{
    /// <summary>
    /// Starts and runs the interactive terminal UI.
    /// </summary>
    void Run();
}