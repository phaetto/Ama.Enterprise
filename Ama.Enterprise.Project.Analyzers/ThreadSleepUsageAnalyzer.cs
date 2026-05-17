namespace Ama.Enterprise.Project.Analyzers;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;
using System.Collections.Immutable;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class ThreadSleepUsageAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "CRDTPROJ0006";

    private static readonly LocalizableString Title = "Avoid Thread.Sleep usage";
    private static readonly LocalizableString MessageFormat = "Do not use Thread.Sleep. Use asynchronous Task.Delay instead.";
    private static readonly LocalizableString Description = "Synchronously blocking threads using Thread.Sleep leads to thread pool starvation and poor performance in scalable applications. Use Task.Delay for asynchronous yielding.";
    private const string Category = "Performance";

    private static readonly DiagnosticDescriptor Rule = new(
        DiagnosticId, 
        Title, 
        MessageFormat, 
        Category, 
        DiagnosticSeverity.Error, 
        isEnabledByDefault: true, 
        description: Description);

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => [Rule];

    public override void Initialize(AnalysisContext context)
    {
        if (context is null)
        {
            return;
        }

        context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
        context.EnableConcurrentExecution();
        context.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        if (context.Operation is not IInvocationOperation invocation)
        {
            return;
        }

        var targetMethod = invocation.TargetMethod;
        var containingType = targetMethod.ContainingType?.ToDisplayString();

        if (string.IsNullOrEmpty(containingType))
        {
            return;
        }

        if (containingType == "System.Threading.Thread" && targetMethod.Name == "Sleep")
        {
            var diagnostic = Diagnostic.Create(
                Rule,
                invocation.Syntax.GetLocation());
            
            context.ReportDiagnostic(diagnostic);
        }
    }
}