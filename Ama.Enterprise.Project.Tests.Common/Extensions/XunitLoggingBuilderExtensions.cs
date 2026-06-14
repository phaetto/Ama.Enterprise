namespace Ama.Enterprise.Project.Tests.Common.Extensions;

using System;
using Ama.Enterprise.Project.Tests.Common.Logging;
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
        ArgumentNullException.ThrowIfNull(builder);

        ArgumentNullException.ThrowIfNull(testOutputHelper);

        builder.AddProvider(new XunitLoggerProvider(testOutputHelper));
        return builder;
    }
}