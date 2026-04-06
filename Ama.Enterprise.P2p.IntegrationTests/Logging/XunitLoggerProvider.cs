namespace Ama.Enterprise.P2p.IntegrationTests.Logging;

using System;
using Microsoft.Extensions.Logging;

/// <summary>
/// Provider for creating XunitLogger instances.
/// </summary>
public sealed class XunitLoggerProvider : ILoggerProvider
{
    private readonly ITestOutputHelper testOutputHelper;

    public XunitLoggerProvider(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));
    }

    public ILogger CreateLogger(string categoryName)
    {
        if (string.IsNullOrEmpty(categoryName))
        {
            throw new ArgumentException("Category name cannot be null or empty.", nameof(categoryName));
        }

        return new XunitLogger(testOutputHelper, categoryName);
    }

    public void Dispose()
    {
        // No unmanaged resources to release.
    }
}