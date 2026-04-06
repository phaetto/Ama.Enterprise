namespace Ama.Enterprise.P2p.IntegrationTests.Attributes;

using System;

/// <summary>
/// Indicates that a specific test method explicitly validates the given protocol version.
/// Used by architectural tests to ensure deployment versions always have explicit test coverage.
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = true)]
public sealed class TestedProtocolVersionAttribute : Attribute
{
    /// <summary>
    /// Gets the major protocol version being tested.
    /// </summary>
    public int Major { get; }

    /// <summary>
    /// Gets the minor protocol version being tested.
    /// </summary>
    public int Minor { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="TestedProtocolVersionAttribute"/> class.
    /// </summary>
    /// <param name="major">The major version number.</param>
    /// <param name="minor">The minor version number.</param>
    public TestedProtocolVersionAttribute(int major, int minor)
    {
        this.Major = major;
        this.Minor = minor;
    }
}