namespace Ama.Enterprise.Project.Analyzers;

using System;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class TaskDelayWithoutOptionsAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "CRDTPROJ0004";

    private static readonly LocalizableString Title = "Task.Delay should use configurable options";
    private static readonly LocalizableString MessageFormat = "Task.Delay is using a hardcoded interval. Use a configurable option instead.";
    private static readonly LocalizableString Description = "Avoid hardcoding delay intervals in Task.Delay. Extract the interval into an options class to allow configurable overrides and to avoid magic numbers.";
    private const string Category = "Maintainability";

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

        context.RegisterCompilationStartAction(compilationContext =>
        {
            var assemblyName = compilationContext.Compilation.AssemblyName ?? string.Empty;

            // Skip execution if running inside a test project
            if (assemblyName.Contains(".Test", StringComparison.OrdinalIgnoreCase) ||
                assemblyName.Contains(".UnitTests", StringComparison.OrdinalIgnoreCase) ||
                assemblyName.Contains(".IntegrationTests", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            compilationContext.RegisterOperationAction(AnalyzeInvocation, OperationKind.Invocation);
        });
    }

    private static void AnalyzeInvocation(OperationAnalysisContext context)
    {
        if (context.Operation is not IInvocationOperation invocation)
        {
            return;
        }

        var targetMethod = invocation.TargetMethod;
        if (targetMethod.Name != "Delay" || targetMethod.ContainingType?.ToDisplayString() != "System.Threading.Tasks.Task")
        {
            return;
        }

        var containingType = context.ContainingSymbol?.ContainingType;
        if (containingType != null && containingType.Name.EndsWith("Options", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var delayArg = invocation.Arguments.FirstOrDefault(a =>
            a.Parameter?.Name == "delay" ||
            a.Parameter?.Name == "millisecondsDelay");

        if (delayArg != null && IsHardcoded(delayArg.Value))
        {
            var diagnostic = Diagnostic.Create(Rule, invocation.Syntax.GetLocation());
            context.ReportDiagnostic(diagnostic);
        }
    }

    private static bool IsHardcoded(IOperation operation)
    {
        if (operation is null)
        {
            return false;
        }

        // Unwrap conversions
        while (operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
        }

        // Catch direct literals and constants
        if (operation.ConstantValue.HasValue)
        {
            return true;
        }

        // Catch TimeSpan static factories e.g., TimeSpan.FromSeconds(5)
        if (operation is IInvocationOperation invocation)
        {
            var typeStr = invocation.TargetMethod.ContainingType?.ToDisplayString();
            if (typeStr == "System.TimeSpan")
            {
                foreach (var arg in invocation.Arguments)
                {
                    if (IsHardcoded(arg.Value))
                    {
                        return true;
                    }
                }

                return false;
            }
        }

        // Trace back a local variable assignment to see if it was initialized with a hardcoded value
        if (operation is ILocalReferenceOperation localReference)
        {
            var local = localReference.Local;
            var declaringRefs = local.DeclaringSyntaxReferences;
            if (declaringRefs.Length == 1)
            {
                var syntaxNode = declaringRefs[0].GetSyntax(default);
                if (syntaxNode is VariableDeclaratorSyntax declarator && declarator.Initializer != null)
                {
                    var semanticModel = operation.SemanticModel;
                    if (semanticModel != null)
                    {
                        var initOperation = semanticModel.GetOperation(declarator.Initializer.Value, default);
                        if (initOperation != null && IsHardcoded(initOperation))
                        {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }
}