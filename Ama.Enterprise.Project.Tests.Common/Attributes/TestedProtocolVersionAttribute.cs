namespace Ama.Enterprise.Project.Tests.Common.Attributes;

using System;

/// <summary>
/// Indicates that a specific test method explicitly validates the given protocol version.
/// Used by architectural tests to ensure deployment versions always have explicit test coverage.
/// </summary>
/// <remarks>
/// Initializes a new instance of the <see cref="TestedProtocolVersionAttribute"/> class.
/// </remarks>
/// <param name="major">The major version number.</param>
/// <param name="minor">The minor version number.</param>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class, AllowMultiple = true)]
public sealed class TestedProtocolVersionAttribute(int major, int minor) : Attribute
{
    /// <summary>
    /// Gets the major protocol version being tested.
    /// </summary>
    public int Major { get; } = major;

    /// <summary>
    /// Gets the minor protocol version being tested.
    /// </summary>
    public int Minor { get; } = minor;
}