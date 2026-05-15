namespace Ama.Enterprise.CRDT.MessagePack.Analyzers.Generators;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

[Generator]
public sealed class MessagePackFormatterGenerator : IIncrementalGenerator
{
    private const string JsonSerializableAttributeFullName = "System.Text.Json.Serialization.JsonSerializableAttribute";

    // Add all known decoupled contexts here so the generator natively reaches into referenced NuGet assemblies 
    // and synthesizes their formatters automatically into the consumer's App resolver.
    private static readonly string[] KnownFrameworkContexts = new[]
    {
        "Ama.CRDT.Models.Serialization.CrdtJsonContext",
        "Ama.Enterprise.P2p.Models.Gossip.P2pJsonSerializerContext",
        "Ama.Enterprise.CRDT.Distributed.Models.DistributedCrdtSystemJsonContext",
        "Ama.Enterprise.FeatureFlags.Models.FeatureFlagsJsonContext",
        "Ama.Enterprise.P2p.AspNetCore.Models.AspNetCoreJsonContext",
        "Ama.Enterprise.P2p.Mqtt.Models.MqttJsonContext",
        "Ama.Enterprise.P2p.Mqtt.Models.MqttDiscoveryJsonContext",
        "Ama.Enterprise.P2p.Telemetry.Models.TelemetryJsonContext",
        "Ama.Enterprise.P2p.WebRTC.Models.WebRtcJsonContext",
        "Ama.Enterprise.CRDT.Distributed.Models.DistributedCrdtP2pJsonContext",
        "Ama.Enterprise.P2p.Services.Discovery.UdpDiscoveryJsonContext",
    };

    private static readonly SymbolDisplayFormat DefinitionFormat = new SymbolDisplayFormat(
        typeQualificationStyle: SymbolDisplayTypeQualificationStyle.NameAndContainingTypesAndNamespaces,
        genericsOptions: SymbolDisplayGenericsOptions.IncludeTypeParameters);

    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var stjAttributes = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                JsonSerializableAttributeFullName,
                predicate: static (node, _) => node is ClassDeclarationSyntax,
                transform: static (ctx, _) => ctx.TargetSymbol as INamedTypeSymbol)
            .Where(static symbol => symbol is not null);

        var compilationProvider = context.CompilationProvider;

        var combined = stjAttributes.Collect().Combine(compilationProvider);

        context.RegisterSourceOutput(combined, Execute);
    }

    private static void Execute(SourceProductionContext context, (ImmutableArray<INamedTypeSymbol?> SyntaxContexts, Compilation Compilation) source)
    {
        var targetTypes = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
        
        // 1. Collect manually specified JsonSerializable attributes in current application's compilation
        foreach (var symbol in source.SyntaxContexts)
        {
            if (symbol == null) continue;
            ExtractTargetTypes(symbol, targetTypes);
        }

        // 2. Automatically map against standard CRDT/P2P STJ contexts referenced via NuGet or Project References
        foreach (var contextName in KnownFrameworkContexts)
        {
            var frameworkContext = source.Compilation.GetTypeByMetadataName(contextName);
            if (frameworkContext != null)
            {
                ExtractTargetTypes(frameworkContext, targetTypes);
            }
        }

        if (targetTypes.Count == 0) return;

        var discoveredCustomTypes = new HashSet<INamedTypeSymbol>(SymbolEqualityComparer.Default);
        var discoveredCollectionTypes = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
        
        TraverseTypes(targetTypes, discoveredCustomTypes, discoveredCollectionTypes);

        var assemblyName = source.Compilation.AssemblyName?.Replace(".", "_").Replace("-", "_") ?? "App";
        var resolverName = $"{assemblyName}_MessagePackResolver";

        GenerateResolver(context, discoveredCustomTypes, discoveredCollectionTypes, resolverName);
    }

    private static void ExtractTargetTypes(INamedTypeSymbol contextSymbol, HashSet<ITypeSymbol> targetTypes)
    {
        var attributes = contextSymbol.GetAttributes()
            .Where(a => a.AttributeClass?.Name == "JsonSerializableAttribute" || 
                        a.AttributeClass?.ToDisplayString() == JsonSerializableAttributeFullName);

        foreach (var attr in attributes)
        {
            if (attr.ConstructorArguments.Length > 0 && attr.ConstructorArguments[0].Value is ITypeSymbol targetType)
            {
                targetTypes.Add(targetType);
            }
        }
    }

    private static void TraverseTypes(HashSet<ITypeSymbol> rootTypes, HashSet<INamedTypeSymbol> customTypes, HashSet<ITypeSymbol> collectionTypes)
    {
        var visited = new HashSet<ITypeSymbol>(SymbolEqualityComparer.Default);
        var queue = new Queue<ITypeSymbol>(rootTypes);

        while (queue.Count > 0)
        {
            var type = queue.Dequeue();
            if (type == null) continue;
            if (!visited.Add(type)) continue;

            if (type is ITypeParameterSymbol) continue; // Skip unbound generics like T

            if (type.TypeKind == TypeKind.Array && type is IArrayTypeSymbol arrayType)
            {
                collectionTypes.Add(arrayType);
                queue.Enqueue(arrayType.ElementType);
                continue;
            }

            if (type.SpecialType != SpecialType.None) continue; // Primitive/String/Object
            if (type.TypeKind == TypeKind.Enum) continue;

            if (type is INamedTypeSymbol namedType)
            {
                var originalDef = namedType.OriginalDefinition.ToDisplayString(DefinitionFormat);
                
                // Exclude system basics
                if (originalDef == "System.Object" || originalDef == "System.ValueType") continue;

                if (namedType.IsGenericType)
                {
                    if (originalDef.StartsWith("System.Collections.") || originalDef.StartsWith("System.Linq."))
                    {
                        collectionTypes.Add(namedType);
                        foreach (var arg in namedType.TypeArguments) queue.Enqueue(arg);
                        continue;
                    }
                    if (originalDef == "System.Nullable<T>")
                    {
                        collectionTypes.Add(namedType);
                        queue.Enqueue(namedType.TypeArguments[0]);
                        continue;
                    }
                    if (originalDef == "System.Collections.Generic.KeyValuePair<TKey, TValue>")
                    {
                        collectionTypes.Add(namedType);
                        foreach (var arg in namedType.TypeArguments) queue.Enqueue(arg);
                        continue;
                    }
                }

                if (namedType.TypeKind == TypeKind.Class || namedType.TypeKind == TypeKind.Struct)
                {
                    if (namedType.ContainingNamespace.ToDisplayString().StartsWith("System")) continue;

                    customTypes.Add(namedType);

                    // Scan properties
                    var properties = namedType.GetMembers().OfType<IPropertySymbol>()
                        .Where(p => p.DeclaredAccessibility == Accessibility.Public && !p.IsStatic);
                    
                    foreach (var prop in properties) queue.Enqueue(prop.Type);

                    // Scan constructors
                    var ctor = namedType.Constructors.OrderByDescending(c => c.Parameters.Length).FirstOrDefault();
                    if (ctor != null)
                    {
                        foreach (var param in ctor.Parameters) queue.Enqueue(param.Type);
                    }
                }
            }
        }
    }

    private static void GenerateResolver(SourceProductionContext context, HashSet<INamedTypeSymbol> customTypes, HashSet<ITypeSymbol> collectionTypes, string resolverName)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("using System;");
        sb.AppendLine("using MessagePack;");
        sb.AppendLine("using MessagePack.Formatters;");
        sb.AppendLine();
        sb.AppendLine("namespace Ama.Enterprise.CRDT.MessagePack.Formatters");
        sb.AppendLine("{");
        
        sb.AppendLine($"    public sealed class {resolverName} : global::MessagePack.IFormatterResolver");
        sb.AppendLine("    {");
        sb.AppendLine($"        public static readonly global::MessagePack.IFormatterResolver Instance = new {resolverName}();");
        sb.AppendLine();
        sb.AppendLine($"        private {resolverName}() {{}}");
        sb.AppendLine();
        sb.AppendLine("        public global::MessagePack.Formatters.IMessagePackFormatter<T> GetFormatter<T>()");
        sb.AppendLine("        {");
        sb.AppendLine("            return FormatterCache<T>.Formatter;");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine("        private static class FormatterCache<T>");
        sb.AppendLine("        {");
        sb.AppendLine("            internal static readonly global::MessagePack.Formatters.IMessagePackFormatter<T> Formatter;");
        sb.AppendLine("            static FormatterCache()");
        sb.AppendLine("            {");
        sb.AppendLine("                object formatter = null;");
        sb.AppendLine("                var type = typeof(T);");

        foreach (var type in collectionTypes)
        {
            var typeFullName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var instantiation = GetFormatterInstantiation(type);
            if (instantiation != null)
            {
                sb.AppendLine($"                if (type == typeof({typeFullName})) formatter = {instantiation};");
            }
        }

        foreach (var type in customTypes)
        {
            var typeFullName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            var formatterName = GetFormatterClassName(type);
            sb.AppendLine($"                if (type == typeof({typeFullName})) formatter = new {formatterName}();");
        }

        sb.AppendLine("                Formatter = (global::MessagePack.Formatters.IMessagePackFormatter<T>)formatter;");
        sb.AppendLine("            }");
        sb.AppendLine("        }");
        sb.AppendLine("    }");

        foreach (var type in customTypes)
        {
            GenerateFormatter(sb, type);
        }

        sb.AppendLine("}");

        context.AddSource($"{resolverName}.g.cs", SourceText.From(sb.ToString(), Encoding.UTF8));
    }

    private static string GetFormatterClassName(INamedTypeSymbol typeSymbol)
    {
        return typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat)
            .Replace("global::", "")
            .Replace(".", "_")
            .Replace("<", "_")
            .Replace(">", "_")
            .Replace(", ", "_") + "Formatter";
    }

    private static string GetFormatterInstantiation(ITypeSymbol type)
    {
        if (type.TypeKind == TypeKind.Array && type is IArrayTypeSymbol arrType)
        {
            var elemName = arrType.ElementType.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            return $"new global::MessagePack.Formatters.ArrayFormatter<{elemName}>()";
        }
        
        if (type is INamedTypeSymbol named && named.IsGenericType)
        {
            var originalDef = named.OriginalDefinition.ToDisplayString(DefinitionFormat);
            var arg0 = named.TypeArguments.Length > 0 ? named.TypeArguments[0].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) : "";
            var arg1 = named.TypeArguments.Length > 1 ? named.TypeArguments[1].ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat) : "";
            
            switch (originalDef)
            {
                case "System.Collections.Generic.List<T>":
                    return $"new global::MessagePack.Formatters.ListFormatter<{arg0}>()";
                case "System.Collections.Generic.IList<T>":
                case "System.Collections.Generic.IReadOnlyList<T>":
                case "System.Collections.Generic.IEnumerable<T>":
                    return $"new global::MessagePack.Formatters.InterfaceListFormatter<{arg0}>()";
                case "System.Collections.Generic.Dictionary<TKey, TValue>":
                    return $"new global::MessagePack.Formatters.DictionaryFormatter<{arg0}, {arg1}>()";
                case "System.Collections.Generic.IDictionary<TKey, TValue>":
                case "System.Collections.Generic.IReadOnlyDictionary<TKey, TValue>":
                    return $"new global::MessagePack.Formatters.InterfaceDictionaryFormatter<{arg0}, {arg1}>()";
                case "System.Collections.Generic.HashSet<T>":
                    return $"new global::MessagePack.Formatters.HashSetFormatter<{arg0}>()";
                case "System.Collections.Generic.ISet<T>":
                case "System.Collections.Generic.IReadOnlySet<T>":
                    return $"new global::MessagePack.Formatters.InterfaceSetFormatter<{arg0}>()";
                case "System.Collections.Generic.KeyValuePair<TKey, TValue>":
                    return $"new global::MessagePack.Formatters.KeyValuePairFormatter<{arg0}, {arg1}>()";
                case "System.Nullable<T>":
                    return $"new global::MessagePack.Formatters.NullableFormatter<{arg0}>()";
            }
        }
        return null;
    }

    private static void GenerateFormatter(StringBuilder sb, INamedTypeSymbol typeSymbol)
    {
        var typeFullName = typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        var formatterName = GetFormatterClassName(typeSymbol);

        sb.AppendLine();
        sb.AppendLine($"    public sealed class {formatterName} : global::MessagePack.Formatters.IMessagePackFormatter<{typeFullName}>");
        sb.AppendLine("    {");

        var ctor = typeSymbol.Constructors
            .Where(c => c.DeclaredAccessibility == Accessibility.Public)
            .OrderByDescending(c => c.Parameters.Length)
            .FirstOrDefault();

        if (ctor == null && typeSymbol.TypeKind != TypeKind.Struct)
        {
            sb.AppendLine($"        // No public constructor found for {typeSymbol.Name}");
            sb.AppendLine("    }");
            return;
        }

        var members = new List<SerializedMember>();
        var mappedProps = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        if (ctor != null)
        {
            for (int i = 0; i < ctor.Parameters.Length; i++)
            {
                var param = ctor.Parameters[i];
                var propName = GetMatchingPropertyName(typeSymbol, param.Name);
                mappedProps.Add(propName);

                var defValue = "default";
                if (param.HasExplicitDefaultValue)
                {
                    defValue = param.ExplicitDefaultValue == null ? "default" : param.ExplicitDefaultValue.ToString();
                    if (param.ExplicitDefaultValue is string) defValue = $"\"{defValue}\"";
                    else if (param.ExplicitDefaultValue is bool b) defValue = b ? "true" : "false";
                }

                members.Add(new SerializedMember
                {
                    Name = param.Name,
                    PropName = propName,
                    Type = param.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    IsCtor = true,
                    CtorIndex = i,
                    HasDefault = param.HasExplicitDefaultValue,
                    DefaultValue = defValue,
                    Index = members.Count
                });
            }
        }

        // Add public settable/initable properties missing from constructors dynamically handling `get; init;` records
        var properties = typeSymbol.GetMembers().OfType<IPropertySymbol>()
            .Where(p => p.DeclaredAccessibility == Accessibility.Public && !p.IsStatic && p.GetMethod != null && p.SetMethod != null)
            .OrderBy(p => p.Name);

        foreach (var prop in properties)
        {
            if (!mappedProps.Contains(prop.Name))
            {
                members.Add(new SerializedMember
                {
                    Name = prop.Name,
                    PropName = prop.Name,
                    Type = prop.Type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat),
                    IsCtor = false,
                    CtorIndex = -1,
                    HasDefault = false,
                    DefaultValue = "default",
                    Index = members.Count
                });
            }
        }

        // Serialize
        sb.AppendLine($"        public void Serialize(ref global::MessagePack.MessagePackWriter writer, {typeFullName} value, global::MessagePack.MessagePackSerializerOptions options)");
        sb.AppendLine("        {");
        if (!typeSymbol.IsValueType)
        {
            sb.AppendLine("            if (value == null) { writer.WriteNil(); return; }");
        }
        sb.AppendLine($"            writer.WriteArrayHeader({members.Count});");

        foreach (var member in members)
        {
            sb.AppendLine($"            options.Resolver.GetFormatterWithVerify<{member.Type}>().Serialize(ref writer, value.{member.PropName}, options);");
        }
        sb.AppendLine("        }");

        // Deserialize
        sb.AppendLine();
        sb.AppendLine($"        public {typeFullName} Deserialize(ref global::MessagePack.MessagePackReader reader, global::MessagePack.MessagePackSerializerOptions options)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (reader.TryReadNil()) return default;");
        sb.AppendLine("            var count = reader.ReadArrayHeader();");

        for (int i = 0; i < members.Count; i++)
        {
            var member = members[i];
            var varName = $"p{i}";

            sb.AppendLine($"            {member.Type} {varName} = default;");

            if (member.HasDefault)
            {
                sb.AppendLine($"            if (count <= {i}) {varName} = {member.DefaultValue};");
            }

            sb.AppendLine($"            if (count > {i}) {varName} = options.Resolver.GetFormatterWithVerify<{member.Type}>().Deserialize(ref reader, options);");
        }

        sb.AppendLine($"            for (int i = {members.Count}; i < count; i++) reader.Skip();");

        var ctorArgs = string.Join(", ", members.Where(m => m.IsCtor).OrderBy(m => m.CtorIndex).Select(m => $"p{m.Index}"));

        var propSetters = members.Where(m => !m.IsCtor).Select(m => $"{m.PropName} = p{m.Index}").ToList();
        var objectInitializer = propSetters.Count > 0 ? $" {{ {string.Join(", ", propSetters)} }}" : "";

        sb.AppendLine($"            return new {typeFullName}({ctorArgs}){objectInitializer};");
        sb.AppendLine("        }");

        sb.AppendLine("    }");
    }

    private static string GetMatchingPropertyName(INamedTypeSymbol typeSymbol, string paramName)
    {
        var properties = typeSymbol.GetMembers().OfType<IPropertySymbol>();
        var match = properties.FirstOrDefault(p => p.Name.Equals(paramName, StringComparison.OrdinalIgnoreCase));
        return match != null ? match.Name : paramName;
    }

    private sealed class SerializedMember
    {
        public string Name { get; set; }
        public string PropName { get; set; }
        public string Type { get; set; }
        public bool IsCtor { get; set; }
        public int CtorIndex { get; set; }
        public bool HasDefault { get; set; }
        public string DefaultValue { get; set; }
        public int Index { get; set; }
    }
}