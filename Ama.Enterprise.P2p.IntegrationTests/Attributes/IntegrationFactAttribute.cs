namespace Ama.Enterprise.P2p.IntegrationTests.Attributes;

using Xunit;

/// <summary>
/// Custom xUnit Fact attribute to control the execution of all integration tests from a single place.
/// By default, skips tests that make actual HTTP requests or bind to local network ports.
/// </summary>
public sealed class IntegrationFactAttribute : FactAttribute
{
    /// <summary>
    /// Centralized toggle for integration tests. 
    /// Change to true to enable all integration tests decorated with this attribute.
    /// </summary>
    private const bool EnableIntegrationTests = true;

    /// <summary>
    /// Initializes a new instance of the <see cref="IntegrationFactAttribute"/> class.
    /// </summary>
    public IntegrationFactAttribute()
    {
        if (!EnableIntegrationTests)
        {
            this.Skip = "Integration tests are disabled by default. Change 'EnableIntegrationTests' in IntegrationFactAttribute to true to execute.";
        }
    }
}