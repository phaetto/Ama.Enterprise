namespace Ama.Enterprise.Project.Analyzers.UnitTests;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using System.Threading.Tasks;
using Xunit;

public sealed class DirectSerializationUsageAnalyzerTests
{
    [Fact]
    public async Task WhenSystemTextJsonSerializerIsUsed_ShouldReportDiagnostic()
    {
        var source = @"
using System.Text.Json;

public class TestClass
{
    public void DoWork()
    {
        var value = JsonSerializer.Serialize(new { });
    }
}
";
        var expected = new DiagnosticResult("CRDTPROJ0003", DiagnosticSeverity.Error)
            .WithLocation(8, 21)
            .WithArguments("JsonSerializer", "Serialize");

        var test = CreateTest();
        test.TestCode = source;
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenSystemTextJsonDeserializerIsUsed_ShouldReportDiagnostic()
    {
        var source = @"
using System.Text.Json;

public class TestClass
{
    public void DoWork()
    {
        var value = JsonSerializer.Deserialize<object>(""{}"");
    }
}
";
        var expected = new DiagnosticResult("CRDTPROJ0003", DiagnosticSeverity.Error)
            .WithLocation(8, 21)
            .WithArguments("JsonSerializer", "Deserialize");

        var test = CreateTest();
        test.TestCode = source;
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenICrdtSerializerIsUsed_ShouldNotReportDiagnostic()
    {
        var source = @"
public interface ICrdtSerializer 
{
    string SerializeToString<T>(T value);
}

public class TestClass
{
    private readonly ICrdtSerializer _serializer;

    public TestClass(ICrdtSerializer serializer)
    {
        _serializer = serializer;
    }

    public void DoWork()
    {
        var value = _serializer.SerializeToString(new { });
    }
}
";
        var test = CreateTest();
        test.TestCode = source;
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenCustomJsonSerializerIsUsed_ShouldNotReportDiagnostic()
    {
        var source = @"
public static class JsonSerializer 
{
    public static string Serialize(object value) => string.Empty;
}

public class TestClass
{
    public void DoWork()
    {
        var value = JsonSerializer.Serialize(new { });
    }
}
";
        var test = CreateTest();
        test.TestCode = source;
        await test.RunAsync();
    }

    private static CSharpAnalyzerTest<DirectSerializationUsageAnalyzer, DefaultVerifier> CreateTest()
    {
        var test = new CSharpAnalyzerTest<DirectSerializationUsageAnalyzer, DefaultVerifier>
        {
            ReferenceAssemblies = ReferenceAssemblies.Net.Net80
        };

        return test;
    }
}