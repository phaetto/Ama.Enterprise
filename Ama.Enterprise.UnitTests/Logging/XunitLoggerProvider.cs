namespace Ama.Enterprise.UnitTests.Logging;

using Microsoft.Extensions.Logging;
using System;

/// <summary>
/// Provider for creating XunitLogger instances.
/// </summary>
public sealed class XunitLoggerProvider(ITestOutputHelper testOutputHelper) : ILoggerProvider
{
    private readonly ITestOutputHelper testOutputHelper = testOutputHelper ?? throw new ArgumentNullException(nameof(testOutputHelper));

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