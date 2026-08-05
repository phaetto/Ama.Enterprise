namespace Ama.Enterprise.Project.Analyzers.UnitTests;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using System.Threading.Tasks;
using Xunit;

public sealed class ThreadSleepUsageAnalyzerTests
{
    [Fact]
    public async Task WhenThreadSleepWithIntIsUsed_ShouldReportDiagnostic()
    {
        var source = @"
using System.Threading;

public class TestClass
{
    public void DoWork()
    {
        Thread.Sleep(1000);
    }
}
";
        var expected = new DiagnosticResult("CRDTPROJ0006", DiagnosticSeverity.Error)
            .WithLocation(8, 9);

        var test = CreateTest();
        test.TestCode = source;
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenThreadSleepWithTimeSpanIsUsed_ShouldReportDiagnostic()
    {
        var source = @"
using System;
using System.Threading;

public class TestClass
{
    public void DoWork()
    {
        Thread.Sleep(TimeSpan.FromSeconds(1));
    }
}
";
        var expected = new DiagnosticResult("CRDTPROJ0006", DiagnosticSeverity.Error)
            .WithLocation(9, 9);

        var test = CreateTest();
        test.TestCode = source;
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenTaskDelayIsUsed_ShouldNotReportDiagnostic()
    {
        var source = @"
using System.Threading.Tasks;

public class TestClass
{
    public async Task DoWorkAsync()
    {
        await Task.Delay(1000);
    }
}
";
        var test = CreateTest();
        test.TestCode = source;
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenCustomThreadSleepIsUsed_ShouldNotReportDiagnostic()
    {
        var source = @"
public static class Thread 
{
    public static void Sleep(int ms) { }
}

public class TestClass
{
    public void DoWork()
    {
        Thread.Sleep(1000);
    }
}
";
        var test = CreateTest();
        test.TestCode = source;
        await test.RunAsync();
    }

    private static CSharpAnalyzerTest<ThreadSleepUsageAnalyzer, DefaultVerifier> CreateTest()
    {
        var test = new CSharpAnalyzerTest<ThreadSleepUsageAnalyzer, DefaultVerifier>
        {
            ReferenceAssemblies = ReferenceAssemblies.Net.Net90
        };

        return test;
    }
}