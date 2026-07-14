namespace Ama.Enterprise.Project.Analyzers.UnitTests;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using System.Threading.Tasks;
using Xunit;

public sealed class JsonSerializableExtensiblePayloadAnalyzerTests
{
    [Fact]
    public async Task WhenModelImplementsInterface_ShouldNotReportDiagnostic()
    {
        var source = @"using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Models.Core
{
    public interface IExtensibleDistributedPayload { }
}

public class ValidModel : IExtensibleDistributedPayload { }

[JsonSerializable(typeof(ValidModel))]
public partial class MyContext : JsonSerializerContext { }
";
        var test = CreateTest();
        test.TestCode = source;
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenModelDoesNotImplementInterface_ShouldReportDiagnostic()
    {
        var source = @"using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Models.Core
{
    public interface IExtensibleDistributedPayload { }
}

public class InvalidModel { }

[JsonSerializable(typeof(InvalidModel))]
public partial class MyContext : JsonSerializerContext { }
";
        var expected = new DiagnosticResult("CRDTPROJ0006", DiagnosticSeverity.Error)
            .WithLocation(11, 2)
            .WithArguments("InvalidModel");

        var test = CreateTest();
        test.TestCode = source;
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenSystemTypeIsRegistered_ShouldNotReportDiagnostic()
    {
        var source = @"using System.Text.Json.Serialization;
using System.Collections.Generic;
using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Models.Core
{
    public interface IExtensibleDistributedPayload { }
}

[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(Dictionary<string, int>))]
public partial class MyContext : JsonSerializerContext { }
";
        var test = CreateTest();
        test.TestCode = source;
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenEnumIsRegistered_ShouldNotReportDiagnostic()
    {
        var source = @"using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Models.Core
{
    public interface IExtensibleDistributedPayload { }
}

public enum MyEnum { ValueA, ValueB }

[JsonSerializable(typeof(MyEnum))]
public partial class MyContext : JsonSerializerContext { }
";
        var test = CreateTest();
        test.TestCode = source;
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenGenericCollectionOfInvalidModelIsRegistered_ShouldReportDiagnostic()
    {
        var source = @"using System.Text.Json.Serialization;
using System.Collections.Generic;
using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Models.Core
{
    public interface IExtensibleDistributedPayload { }
}

public class InvalidModel { }

[JsonSerializable(typeof(List<InvalidModel>))]
public partial class MyContext : JsonSerializerContext { }
";
        var expected = new DiagnosticResult("CRDTPROJ0006", DiagnosticSeverity.Error)
            .WithLocation(12, 2)
            .WithArguments("InvalidModel");

        var test = CreateTest();
        test.TestCode = source;
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    [Fact]
    public async Task WhenOptionsTypeRegistered_ShouldReportDiagnostic()
    {
        var source = @"using System.Text.Json.Serialization;
using Ama.Enterprise.P2p.Models.Core;

namespace Ama.Enterprise.P2p.Models.Core
{
    public interface IExtensibleDistributedPayload { }
}

namespace Ama.Enterprise.P2p.Models.Transports
{
    public class TcpTransportOptions { }
}

[JsonSerializable(typeof(Ama.Enterprise.P2p.Models.Transports.TcpTransportOptions))]
public partial class MyContext : JsonSerializerContext { }
";
        var expected = new DiagnosticResult("CRDTPROJ0006", DiagnosticSeverity.Error)
            .WithLocation(14, 2)
            .WithArguments("TcpTransportOptions");

        var test = CreateTest();
        test.TestCode = source;
        test.ExpectedDiagnostics.Add(expected);
        await test.RunAsync();
    }

    private static CSharpAnalyzerTest<JsonSerializableExtensiblePayloadAnalyzer, DefaultVerifier> CreateTest()
    {
        var test = new CSharpAnalyzerTest<JsonSerializableExtensiblePayloadAnalyzer, DefaultVerifier>
        {
            ReferenceAssemblies = ReferenceAssemblies.Net.Net100,
            CompilerDiagnostics = CompilerDiagnostics.None 
        };

        return test;
    }
}