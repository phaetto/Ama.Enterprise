namespace Ama.Enterprise.Project.Analyzers;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;
using System.Collections.Immutable;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class DirectSerializationUsageAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "CRDTPROJ0003";

    private static readonly LocalizableString Title = "Avoid direct use of JSON serialization";
    private static readonly LocalizableString MessageFormat = "Do not use {0}.{1} directly. Inject and use ICrdtSerializer instead.";
    private static readonly LocalizableString Description = "Avoid using serialization libraries directly. Use ICrdtSerializer to ensure format-agnostic and Native AOT compatible serialization throughout the codebase.";
    private const string Category = "Design";

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

        // Catch direct usages of common serialization classes
        if (containingType == "System.Text.Json.JsonSerializer" || 
            containingType == "Newtonsoft.Json.JsonConvert")
        {
            var typeName = targetMethod.ContainingType?.Name ?? "JsonSerializer";
            
            var diagnostic = Diagnostic.Create(
                Rule,
                invocation.Syntax.GetLocation(),
                typeName,
                targetMethod.Name);
            
            context.ReportDiagnostic(diagnostic);
        }
    }
}