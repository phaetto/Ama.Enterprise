namespace Ama.Enterprise.P2p.IntegrationTests.Extensions;

using System;
using Ama.Enterprise.P2p.IntegrationTests.Logging;
using Microsoft.Extensions.Logging;

/// <summary>
/// Extension methods for registering xUnit logger with the logging builder.
/// </summary>
public static class XunitLoggingBuilderExtensions
{
    /// <summary>
    /// Adds xUnit logger to the logging builder.
    /// </summary>
    /// <param name="builder">The logging builder.</param>
    /// <param name="testOutputHelper">The xUnit test output helper.</param>
    /// <returns>The updated logging builder.</returns>
    public static ILoggingBuilder AddXunit(this ILoggingBuilder builder, ITestOutputHelper testOutputHelper)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (testOutputHelper is null)
        {
            throw new ArgumentNullException(nameof(testOutputHelper));
        }

        builder.AddProvider(new XunitLoggerProvider(testOutputHelper));
        return builder;
    }
}