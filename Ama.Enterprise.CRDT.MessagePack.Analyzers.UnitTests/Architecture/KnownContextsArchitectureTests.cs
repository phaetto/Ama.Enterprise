namespace Ama.Enterprise.CRDT.MessagePack.Analyzers.UnitTests.Architecture;

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Ama.Enterprise.CRDT.MessagePack.Analyzers.Generators;
using Shouldly;
using Xunit;

public sealed class KnownContextsArchitectureTests
{
    [Fact]
    public void MessagePackFormatterGenerator_ShouldContainAllFrameworkJsonContexts()
    {
        // 1. Get the registered contexts from the generator via reflection
        var generatorType = typeof(MessagePackFormatterGenerator);
        var knownContextsField = generatorType.GetField("KnownFrameworkContexts", BindingFlags.NonPublic | BindingFlags.Static);
        
        knownContextsField.ShouldNotBeNull("KnownFrameworkContexts field was not found on MessagePackFormatterGenerator.");
        
        var registeredContexts = (string[])knownContextsField.GetValue(null);
        registeredContexts.ShouldNotBeNull();
        var registeredSet = new HashSet<string>(registeredContexts, StringComparer.OrdinalIgnoreCase);

        // 2. Scan the solution for all STJ Contexts dynamically
        var solutionRoot = GetSolutionRoot();
        var allCsFiles = Directory.GetFiles(solutionRoot, "*.cs", SearchOption.AllDirectories);

        var expectedContexts = new List<string>();

        foreach (var file in allCsFiles)
        {
            // Skip non-framework code (tests, showcases, toolings, and build artifacts)
            if (file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}") ||
                file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}") ||
                file.Contains(".UnitTests") ||
                file.Contains(".IntegrationTests") ||
                file.Contains(".Testing") ||
                file.Contains(".ShowCase") ||
                file.Contains(".Cli") ||
                file.Contains("TestNamespace") ||
                file.Contains("LicensingJsonContext") // Special case for licensing
            )
            {
                continue;
            }

            var content = File.ReadAllText(file);
            if (content.Contains("JsonSerializerContext") && content.Contains("partial class"))
            {
                var nsMatch = Regex.Match(content, @"namespace\s+([\w\.]+)");
                var classMatch = Regex.Match(content, @"partial\s+class\s+(\w+)\s*:\s*JsonSerializerContext");

                if (nsMatch.Success && classMatch.Success)
                {
                    var fullyQualifiedName = $"{nsMatch.Groups[1].Value}.{classMatch.Groups[1].Value}";
                    expectedContexts.Add(fullyQualifiedName);
                }
            }
        }

        // 3. Assert all discovered framework contexts are registered
        expectedContexts.ShouldNotBeEmpty("No JSON contexts were discovered in the solution. Check the solution root scanning logic.");

        foreach (var expected in expectedContexts)
        {
            registeredSet.ShouldContain(expected, 
                $"The framework STJ Context '{expected}' is missing from the KnownFrameworkContexts array in MessagePackFormatterGenerator.cs. " +
                $"This must be added so the AOT generator natively extracts its types in consuming applications!");
        }
        
        // Note: Ama.CRDT.Models.Serialization.CrdtJsonContext is in an external upstream repository, 
        // so it won't be found by the local filesystem scan. We explicitly ensure it remains registered.
        registeredSet.ShouldContain("Ama.CRDT.Models.Serialization.CrdtJsonContext");
    }

    private static string GetSolutionRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        
        // Climb the directory tree until we find the root solution file
        while (dir != null && !dir.GetFiles("Ama.Enterprise.slnx").Any() && !dir.GetFiles("*.sln").Any())
        {
            dir = dir.Parent;
        }
        
        return dir?.FullName ?? throw new InvalidOperationException("Could not find solution root directory.");
    }
}