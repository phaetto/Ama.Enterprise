namespace Ama.Enterprise.Project.Analyzers.UnitTests;

using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using Xunit;

public sealed class TaskDelayWithoutOptionsAnalyzerTests
{
    [Fact]
    public async Task WhenTaskDelayHasHardcodedInt_ShouldReportDiagnostic()
    {
        var source = @"
using System.Threading.Tasks;

public class TestClass
{
    public async Task DoWork()
    {
        await Task.Delay(1000);
    }
}
";
        var expected = new DiagnosticResult("CRDTPROJ0004", DiagnosticSeverity.Error)
            .WithLocation(8, 15);

        var test = CreateTest(source);
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenTaskDelayHasHardcodedTimeSpan_ShouldReportDiagnostic()
    {
        var source = @"
using System;
using System.Threading.Tasks;

public class TestClass
{
    public async Task DoWork()
    {
        await Task.Delay(TimeSpan.FromSeconds(5));
    }
}
";
        var expected = new DiagnosticResult("CRDTPROJ0004", DiagnosticSeverity.Error)
            .WithLocation(9, 15);

        var test = CreateTest(source);
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenTaskDelayHasHardcodedLocalVariable_ShouldReportDiagnostic()
    {
        var source = @"
using System;
using System.Threading;
using System.Threading.Tasks;

public class TestClass
{
    public async Task DoWork(CancellationToken cancellationToken)
    {
        var checkInterval = TimeSpan.FromSeconds(5);
        await Task.Delay(checkInterval, cancellationToken);
    }
}
";
        var expected = new DiagnosticResult("CRDTPROJ0004", DiagnosticSeverity.Error)
            .WithLocation(11, 15);

        var test = CreateTest(source);
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenTaskDelayUsesOptionsProperty_ShouldNotReportDiagnostic()
    {
        var source = @"
using System;
using System.Threading.Tasks;

public class MyOptions
{
    public TimeSpan DelayInterval { get; set; }
}

public class TestClass
{
    private readonly MyOptions options;

    public TestClass(MyOptions options)
    {
        this.options = options;
    }

    public async Task DoWork()
    {
        await Task.Delay(options.DelayInterval);
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenTaskDelayIsInsideOptionsClass_ShouldNotReportDiagnostic()
    {
        var source = @"
using System.Threading.Tasks;

public class ConfigOptions
{
    public async Task WaitAsync()
    {
        await Task.Delay(1000);
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenTaskDelayUsesOptionsPropertyWithTimeSpanFromSeconds_ShouldNotReportDiagnostic()
    {
        var source = @"
using System;
using System.Threading.Tasks;

public class MyOptions
{
    public int DelayInterval { get; set; }
}

public class TestClass
{
    private readonly MyOptions options;

    public TestClass(MyOptions options)
    {
        this.options = options;
    }

    public async Task DoWork()
    {
        await Task.Delay(TimeSpan.FromSeconds(options.DelayInterval));
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenTaskDelayUsesMathMaxWithOptions_ShouldNotReportDiagnostic()
    {
        var source = @"
using System;
using System.Threading;
using System.Threading.Tasks;

public class MyOptions
{
    public int AntiEntropyInitialDelaySeconds { get; set; }
}

public class TestClass
{
    private readonly MyOptions options;

    public TestClass(MyOptions options)
    {
        this.options = options;
    }

    public async Task DoWork(CancellationToken stoppingToken)
    {
        var initialDelay = TimeSpan.FromSeconds(Math.Max(0, options.AntiEntropyInitialDelaySeconds));
        await Task.Delay(initialDelay, stoppingToken).ConfigureAwait(false);
    }
}
";
        var test = CreateTest(source);
        await test.RunAsync();
    }

    private static CSharpAnalyzerTest<TaskDelayWithoutOptionsAnalyzer, DefaultVerifier> CreateTest(string source)
    {
        var test = new CSharpAnalyzerTest<TaskDelayWithoutOptionsAnalyzer, DefaultVerifier>
        {
            TestCode = source,
            ReferenceAssemblies = ReferenceAssemblies.Net.Net100
        };

        return test;
    }
}