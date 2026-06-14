namespace Ama.Enterprise.Project.Analyzers;

using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class JsonSerializableExtensiblePayloadAnalyzer : DiagnosticAnalyzer
{
    public const string DiagnosticId = "CRDTPROJ0006";

    private static readonly LocalizableString Title = "Models in JsonSerializerContext must implement IExtensibleDistributedPayload";
    private static readonly LocalizableString MessageFormat = "The custom type '{0}' is registered in a JsonSerializerContext but does not implement IExtensibleDistributedPayload. Local configurations or non-distributed payloads should not be serialized via P2P contexts.";
    private static readonly LocalizableString Description = "Ensure all distributed P2P models natively support the Tolerant Reader pattern protecting network compatibility boundaries.";
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
        context.RegisterSymbolAction(AnalyzeSymbol, SymbolKind.NamedType);
    }

    private static void AnalyzeSymbol(SymbolAnalysisContext context)
    {
        if (context.Symbol is not INamedTypeSymbol namedTypeSymbol)
        {
            return;
        }

        if (!InheritsFrom(namedTypeSymbol, "JsonSerializerContext", "System.Text.Json.Serialization"))
        {
            return;
        }

        foreach (var attribute in namedTypeSymbol.GetAttributes())
        {
            if (attribute.AttributeClass == null)
            {
                continue;
            }

            var attrName = attribute.AttributeClass.Name;
            if (attrName != "JsonSerializableAttribute" && attrName != "JsonSerializable")
            {
                continue;
            }

            if (attribute.AttributeClass.ContainingNamespace?.ToDisplayString() != "System.Text.Json.Serialization")
            {
                continue;
            }

            if (attribute.ConstructorArguments.Length == 0)
            {
                continue;
            }

            if (attribute.ConstructorArguments[0].Value is not ITypeSymbol typeArg)
            {
                continue;
            }

            foreach (var typeToCheck in GetTypesToCheck(typeArg))
            {
                var isInterface = typeToCheck.Name == "IExtensibleDistributedPayload" && 
                                  typeToCheck.ContainingNamespace?.ToDisplayString() == "Ama.Enterprise.P2p.Models.Core";

                var implementsInterface = typeToCheck.AllInterfaces.Any(i => 
                    i.Name == "IExtensibleDistributedPayload" && 
                    i.ContainingNamespace?.ToDisplayString() == "Ama.Enterprise.P2p.Models.Core");

                if (!implementsInterface && !isInterface)
                {
                    var syntax = attribute.ApplicationSyntaxReference?.GetSyntax(context.CancellationToken);
                    var location = syntax?.GetLocation() ?? namedTypeSymbol.Locations.FirstOrDefault();
                    
                    if (location != null)
                    {
                        var diagnostic = Diagnostic.Create(Rule, location, typeToCheck.Name);
                        context.ReportDiagnostic(diagnostic);
                    }
                }
            }
        }
    }

    private static IEnumerable<INamedTypeSymbol> GetTypesToCheck(ITypeSymbol symbol)
    {
        if (symbol is IArrayTypeSymbol arrayType)
        {
            foreach (var type in GetTypesToCheck(arrayType.ElementType))
            {
                yield return type;
            }
        }
        else if (symbol is INamedTypeSymbol namedType)
        {
            if (!IsIgnoredType(namedType) && namedType.TypeKind != TypeKind.Enum)
            {
                yield return namedType;
            }

            if (namedType.IsGenericType)
            {
                foreach (var arg in namedType.TypeArguments)
                {
                    foreach (var type in GetTypesToCheck(arg))
                    {
                        yield return type;
                    }
                }
            }
        }
    }

    private static bool InheritsFrom(INamedTypeSymbol symbol, string expectedName, string expectedNamespace)
    {
        var currentBase = symbol.BaseType;
        while (currentBase != null)
        {
            if (currentBase.Name == expectedName && 
                currentBase.ContainingNamespace?.ToDisplayString() == expectedNamespace)
            {
                return true;
            }
            currentBase = currentBase.BaseType;
        }
        
        return false;
    }

    private static bool IsIgnoredType(INamedTypeSymbol symbol)
    {
        var namespaceName = symbol.ContainingNamespace?.ToDisplayString();
        if (string.IsNullOrEmpty(namespaceName))
        {
            return false;
        }

        return namespaceName == "System" || 
               namespaceName.StartsWith("System.") || 
               namespaceName == "Microsoft" || 
               namespaceName.StartsWith("Microsoft.") ||
               namespaceName == "Ama.CRDT" ||
               namespaceName.StartsWith("Ama.CRDT.");
    }
}