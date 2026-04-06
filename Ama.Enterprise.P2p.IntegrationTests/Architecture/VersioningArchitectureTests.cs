namespace Ama.Enterprise.P2p.IntegrationTests.Architecture;

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Ama.Enterprise.P2p.IntegrationTests.Attributes;
using Shouldly;

/// <summary>
/// Architectural tests ensuring versioning consistency between deployment scripts and test coverage.
/// </summary>
public sealed class VersioningArchitectureTests
{
    [Fact]
    public void DeployedYamlVersion_MustHaveExplicitTestCoverage()
    {
        var yamlPath = FindPublishNugetYaml();
        yamlPath.ShouldNotBeNull("Could not locate .github/workflows/publish-nuget.yml. Ensure the test is running within the repository structure.");

        var yamlContent = File.ReadAllText(yamlPath);
        var majorMatch = Regex.Match(yamlContent, @"MAJOR_VERSION:\s*(\d+)");
        var minorMatch = Regex.Match(yamlContent, @"MINOR_VERSION:\s*(\d+)");

        majorMatch.Success.ShouldBeTrue("MAJOR_VERSION not found in publish-nuget.yml");
        minorMatch.Success.ShouldBeTrue("MINOR_VERSION not found in publish-nuget.yml");

        var major = int.Parse(majorMatch.Groups[1].Value);
        var minor = int.Parse(minorMatch.Groups[1].Value);

        var assembly = Assembly.GetExecutingAssembly();
        var hasTestForVersion = assembly.GetTypes()
            .SelectMany(t => t.GetMethods())
            .SelectMany(m => m.GetCustomAttributes<TestedProtocolVersionAttribute>())
            .Any(attr => attr.Major == major && attr.Minor == minor);

        hasTestForVersion.ShouldBeTrue(
            $"Architectural constraint violated: Version {major}.{minor} is targeted for deployment in publish-nuget.yml, " +
            $"but no test method is marked with [TestedProtocolVersion({major}, {minor})]. " +
            $"You must write an explicit test for version {major}.{minor} to prevent deploying untested protocol versions.");
    }

    private string? FindPublishNugetYaml()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        
        while (directory != null)
        {
            var path = Path.Combine(directory.FullName, ".github", "workflows", "publish-nuget.yml");
            if (File.Exists(path))
            {
                return path;
            }
            
            directory = directory.Parent;
        }
        
        return null;
    }
}