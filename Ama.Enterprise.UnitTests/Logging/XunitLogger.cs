namespace Ama.Enterprise.UnitTests.Logging;

using Microsoft.Extensions.Logging;
using System;

/// <summary>
/// A custom logger that writes messages to xUnit's ITestOutputHelper.
/// </summary>
public sealed class XunitLogger : ILogger
{
    private readonly ITestOutputHelper testOutputHelper;
    private readonly string categoryName;

    public XunitLogger(ITestOutputHelper testOutputHelper, string categoryName)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
        
        if (string.IsNullOrEmpty(categoryName))
        {
            throw new ArgumentException("Category name cannot be null or empty.", nameof(categoryName));
        }
        
        this.categoryName = categoryName;
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull
    {
        return null;
    }

    public bool IsEnabled(LogLevel logLevel)
    {
        return true;
    }

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        ArgumentNullException.ThrowIfNull(formatter);

        try
        {
            var message = formatter(state, exception);
            testOutputHelper.WriteLine($"[{logLevel}] {categoryName}: {message}");
            
            if (exception is not null)
            {
                testOutputHelper.WriteLine(exception.ToString());
            }
        }
        catch (InvalidOperationException)
        {
            // xUnit throws InvalidOperationException if an attempt is made to write to output after the test completes.
            // Background tasks (like IHostedService) might still be running and logging during teardown.
        }
    }
}