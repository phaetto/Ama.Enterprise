namespace Ama.Enterprise.CRDT.Distributed.Analyzers;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Operations;

/// <summary>
/// Roslyn analyzer that validates Distributed CRDT replica registrations and P2P mesh bindings at compile time.
/// </summary>
[DiagnosticAnalyzer(LanguageNames.CSharp)]
public sealed class CrdtReplicaAndMeshConfigurationAnalyzer : DiagnosticAnalyzer
{
    public const string MissingReplicaDiagnosticId = "DCRDT0001";
    public const string UnboundReplicaDiagnosticId = "DCRDT0002";
    public const string UnboundMeshDiagnosticId = "DCRDT0003";
    public const string UnconfiguredMeshDiagnosticId = "DCRDT0004";

    private const string Category = "Configuration";

    private static readonly DiagnosticDescriptor MissingReplicaRule = new(
        MissingReplicaDiagnosticId,
        "Distributed CRDT core requires at least one replica registration",
        "'AddDistributedCrdtCore' is called without registering any CRDT replica via 'AddDistributedCrdtP2p', 'AddDistributedCrdtReplica', or 'BindCrdtReplica'",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "When configuring Distributed CRDT core services, at least one local CRDT replica must be registered so documents can be initialized and synchronized.");

    private static readonly DiagnosticDescriptor UnboundReplicaRule = new(
        UnboundReplicaDiagnosticId,
        "CRDT replica is not bound to any P2P mesh",
        "CRDT replica '{0}' is registered without being bound to any P2P mesh via 'AddDistributedCrdtP2p', 'BindCrdtReplica', or mesh ID parameters",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A CRDT replica registered via 'AddDistributedCrdtReplica' without mesh bindings operates only locally. Suppress this warning if the replica is intentionally offline or local-only.");

    private static readonly DiagnosticDescriptor UnboundMeshRule = new(
        UnboundMeshDiagnosticId,
        "P2P mesh is not bound to any CRDT replica",
        "P2P mesh '{0}' is configured via 'AddP2pMesh' but is not bound to any CRDT replica",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "When Distributed CRDTs are active, P2P meshes are typically bound to a CRDT replica. Suppress this warning if the mesh is intentionally dedicated to non-CRDT traffic such as telemetry or monitoring.");

    private static readonly DiagnosticDescriptor UnconfiguredMeshRule = new(
        UnconfiguredMeshDiagnosticId,
        "CRDT P2P binding references an unconfigured P2P mesh",
        "CRDT replica '{0}' is bound to P2P mesh '{1}', but no matching 'AddP2pMesh' configuration was found for '{1}'",
        Category,
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "Binding a CRDT replica to a P2P mesh ID requires a corresponding 'AddP2pMesh' registration with the same mesh ID.");

    private readonly record struct ResolvedIdentifier(string Key, string DisplayName);

    private readonly record struct ReplicaInvocationInfo(Location Location, ResolvedIdentifier ReplicaId, bool HasInlineMeshes);

    private readonly record struct P2pBindingInvocationInfo(Location Location, ResolvedIdentifier MeshId, ResolvedIdentifier ReplicaId, Location? OriginatingAddP2pMeshLocation);

    private readonly record struct MeshInvocationInfo(Location Location, ResolvedIdentifier MeshId);

    private sealed class ConfigurationState
    {
        public List<Location> CoreLocations { get; } = [];
        public List<ReplicaInvocationInfo> ReplicaRegistrations { get; } = [];
        public List<P2pBindingInvocationInfo> P2pBindings { get; } = [];
        public List<MeshInvocationInfo> MeshRegistrations { get; } = [];
    }

    public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics =>
    [
        MissingReplicaRule,
        UnboundReplicaRule,
        UnboundMeshRule,
        UnconfiguredMeshRule
    ];

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

            if (string.Equals(assemblyName, "Ama.Enterprise.CRDT.Distributed", StringComparison.OrdinalIgnoreCase) ||
                assemblyName.IndexOf(".Test", StringComparison.OrdinalIgnoreCase) >= 0 ||
                assemblyName.IndexOf(".UnitTests", StringComparison.OrdinalIgnoreCase) >= 0 ||
                assemblyName.IndexOf(".IntegrationTests", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return;
            }

            compilationContext.RegisterOperationBlockStartAction(blockStartContext =>
            {
                if (IsExtensionMethodDefinition(blockStartContext.OwningSymbol))
                {
                    return;
                }

                var state = new ConfigurationState();

                blockStartContext.RegisterOperationAction(
                    opContext => AnalyzeInvocation(opContext, state),
                    OperationKind.Invocation);

                blockStartContext.RegisterOperationBlockEndAction(
                    endContext => ReportDiagnostics(endContext, state));
            });
        });
    }

    private static bool IsExtensionMethodDefinition(ISymbol owningSymbol)
    {
        if (owningSymbol is not IMethodSymbol methodSymbol)
        {
            return false;
        }

        if (methodSymbol.IsExtensionMethod &&
            string.Equals(methodSymbol.ContainingType?.Name, "ServiceCollectionExtensions", StringComparison.Ordinal))
        {
            return true;
        }

        return false;
    }

    private static void AnalyzeInvocation(
        OperationAnalysisContext context,
        ConfigurationState state)
    {
        if (context.Operation is not IInvocationOperation invocation)
        {
            return;
        }

        var methodName = invocation.TargetMethod.Name;

        if (string.Equals(methodName, "AddDistributedCrdtCore", StringComparison.Ordinal))
        {
            state.CoreLocations.Add(GetInvocationIdentifierLocation(invocation));
            return;
        }

        if (string.Equals(methodName, "AddDistributedCrdtReplica", StringComparison.Ordinal))
        {
            var replicaArg = GetArgument(invocation, "replicaId", 0);
            if (replicaArg is null)
            {
                return;
            }

            var replicaId = ExtractIdentifier(replicaArg.Value);
            var meshIdsArg = GetArgument(invocation, "meshIds", 1);
            var meshIds = ExtractMeshIdsFromParamsArgument(meshIdsArg);

            var location = GetInvocationIdentifierLocation(invocation);
            state.ReplicaRegistrations.Add(new ReplicaInvocationInfo(location, replicaId, meshIds.Count > 0));

            for (int i = 0; i < meshIds.Count; i++)
            {
                state.P2pBindings.Add(new P2pBindingInvocationInfo(location, meshIds[i], replicaId, null));
            }

            return;
        }

        if (string.Equals(methodName, "AddDistributedCrdtP2p", StringComparison.Ordinal))
        {
            var meshArg = GetArgument(invocation, "meshId", 0);
            var replicaArg = GetArgument(invocation, "replicaId", 1);
            if (meshArg is null || replicaArg is null)
            {
                return;
            }

            var meshId = ExtractIdentifier(meshArg.Value);
            var replicaId = ExtractIdentifier(replicaArg.Value);
            var location = GetInvocationIdentifierLocation(invocation);

            state.P2pBindings.Add(new P2pBindingInvocationInfo(location, meshId, replicaId, null));
            return;
        }

        if (string.Equals(methodName, "BindCrdtReplica", StringComparison.Ordinal))
        {
            var addP2pMeshInvocation = FindOriginatingAddP2pMeshInvocation(invocation);
            ResolvedIdentifier meshId;
            Location? originatingMeshLocation = null;

            if (addP2pMeshInvocation is not null)
            {
                originatingMeshLocation = GetInvocationIdentifierLocation(addP2pMeshInvocation);
                var meshArg = GetArgument(addP2pMeshInvocation, "meshId", 0);
                meshId = meshArg is not null
                    ? ExtractIdentifier(meshArg.Value)
                    : new ResolvedIdentifier("unknown-mesh", "unknown");
            }
            else
            {
                meshId = new ResolvedIdentifier("unknown-mesh", "unknown");
            }

            var replicaArg = GetArgument(invocation, "replicaId", 0);
            ResolvedIdentifier replicaId;
            if (replicaArg is null || IsNullOrDefaultArgument(replicaArg))
            {
                replicaId = meshId;
            }
            else
            {
                replicaId = ExtractIdentifier(replicaArg.Value);
            }

            var location = GetInvocationIdentifierLocation(invocation);
            state.P2pBindings.Add(new P2pBindingInvocationInfo(location, meshId, replicaId, originatingMeshLocation));
            return;
        }

        if (string.Equals(methodName, "AddP2pMesh", StringComparison.Ordinal))
        {
            var meshArg = GetArgument(invocation, "meshId", 0);
            if (meshArg is null)
            {
                return;
            }

            var meshId = ExtractIdentifier(meshArg.Value);
            var location = GetInvocationIdentifierLocation(invocation);
            state.MeshRegistrations.Add(new MeshInvocationInfo(location, meshId));
        }
    }

    private static void ReportDiagnostics(
        OperationBlockAnalysisContext context,
        ConfigurationState state)
    {
        var hasCore = state.CoreLocations.Count > 0;
        var hasReplicas = state.ReplicaRegistrations.Count > 0;
        var hasBindings = state.P2pBindings.Count > 0;
        var isCrdtUsed = hasCore || hasReplicas || hasBindings;

        if (!isCrdtUsed)
        {
            return;
        }

        // DCRDT0001: AddDistributedCrdtCore called without any replica or P2P binding
        if (hasCore && !hasReplicas && !hasBindings)
        {
            foreach (var location in state.CoreLocations.OrderBy(l => l.SourceSpan.Start))
            {
                context.ReportDiagnostic(Diagnostic.Create(MissingReplicaRule, location));
            }
        }

        var boundReplicaKeys = new HashSet<string>(
            state.P2pBindings.Select(b => b.ReplicaId.Key),
            StringComparer.Ordinal);

        // DCRDT0002: AddDistributedCrdtReplica called without any P2P mesh binding
        foreach (var replicaReg in state.ReplicaRegistrations.OrderBy(r => r.Location.SourceSpan.Start))
        {
            if (!replicaReg.HasInlineMeshes && !boundReplicaKeys.Contains(replicaReg.ReplicaId.Key))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    UnboundReplicaRule,
                    replicaReg.Location,
                    replicaReg.ReplicaId.DisplayName));
            }
        }

        var boundMeshKeys = new HashSet<string>(
            state.P2pBindings.Select(b => b.MeshId.Key),
            StringComparer.Ordinal);

        var directlyBoundMeshLocations = new HashSet<Location>(
            state.P2pBindings.Where(b => b.OriginatingAddP2pMeshLocation is not null).Select(b => b.OriginatingAddP2pMeshLocation!));

        // DCRDT0003: AddP2pMesh called without any CRDT replica bound to it
        foreach (var meshReg in state.MeshRegistrations.OrderBy(m => m.Location.SourceSpan.Start))
        {
            if (!boundMeshKeys.Contains(meshReg.MeshId.Key) && !directlyBoundMeshLocations.Contains(meshReg.Location))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    UnboundMeshRule,
                    meshReg.Location,
                    meshReg.MeshId.DisplayName));
            }
        }

        var configuredMeshKeys = new HashSet<string>(
            state.MeshRegistrations.Select(m => m.MeshId.Key),
            StringComparer.Ordinal);

        // DCRDT0004: AddDistributedCrdtP2p or AddDistributedCrdtReplica(replicaId, meshIds) references a meshId not configured via AddP2pMesh
        foreach (var binding in state.P2pBindings.OrderBy(b => b.Location.SourceSpan.Start))
        {
            if (binding.OriginatingAddP2pMeshLocation is null &&
                !string.Equals(binding.MeshId.Key, "unknown-mesh", StringComparison.Ordinal) &&
                !configuredMeshKeys.Contains(binding.MeshId.Key))
            {
                context.ReportDiagnostic(Diagnostic.Create(
                    UnconfiguredMeshRule,
                    binding.Location,
                    binding.ReplicaId.DisplayName,
                    binding.MeshId.DisplayName));
            }
        }
    }

    private static IArgumentOperation? GetArgument(IInvocationOperation invocation, string parameterName, int LogicalIndex)
    {
        foreach (var arg in invocation.Arguments)
        {
            if (string.Equals(arg.Parameter?.Name, parameterName, StringComparison.Ordinal))
            {
                return arg;
            }
        }

        var isExtensionOnReceiver = invocation.TargetMethod.IsExtensionMethod && invocation.Instance is null;
        var adjustedIndex = isExtensionOnReceiver ? LogicalIndex + 1 : LogicalIndex;

        if (adjustedIndex >= 0 && adjustedIndex < invocation.Arguments.Length)
        {
            return invocation.Arguments[adjustedIndex];
        }

        return null;
    }

    private static List<ResolvedIdentifier> ExtractMeshIdsFromParamsArgument(IArgumentOperation? meshIdsArg)
    {
        var result = new List<ResolvedIdentifier>();
        if (meshIdsArg is null || meshIdsArg.ArgumentKind == ArgumentKind.DefaultValue)
        {
            return result;
        }

        var value = UnwrapConversions(meshIdsArg.Value);
        if (value is IArrayCreationOperation arrayCreation)
        {
            if (arrayCreation.Initializer is not null)
            {
                foreach (var element in arrayCreation.Initializer.ElementValues)
                {
                    result.Add(ExtractIdentifier(element));
                }
            }

            return result;
        }

        if (value.Syntax is CollectionExpressionSyntax collectionSyntax && value.SemanticModel is not null)
        {
            foreach (var elementSyntax in collectionSyntax.Elements)
            {
                if (elementSyntax is ExpressionElementSyntax exprElement)
                {
                    var elementOp = value.SemanticModel.GetOperation(exprElement.Expression);
                    if (elementOp is not null)
                    {
                        result.Add(ExtractIdentifier(elementOp));
                    }
                }
            }

            return result;
        }

        result.Add(ExtractIdentifier(value));
        return result;
    }

    private static bool IsNullOrDefaultArgument(IArgumentOperation argument)
    {
        if (argument.ArgumentKind == ArgumentKind.DefaultValue)
        {
            return true;
        }

        var value = UnwrapConversions(argument.Value);
        return value.ConstantValue.HasValue && value.ConstantValue.Value is null;
    }

    private static IInvocationOperation? FindOriginatingAddP2pMeshInvocation(IInvocationOperation invocation)
    {
        var currentReceiver = GetExtensionReceiver(invocation);

        while (currentReceiver is not null)
        {
            currentReceiver = UnwrapConversions(currentReceiver);

            if (currentReceiver is IInvocationOperation receiverInvocation)
            {
                if (string.Equals(receiverInvocation.TargetMethod.Name, "AddP2pMesh", StringComparison.Ordinal))
                {
                    return receiverInvocation;
                }

                currentReceiver = GetExtensionReceiver(receiverInvocation);
                continue;
            }

            if (currentReceiver is ILocalReferenceOperation localRef)
            {
                var declaratorRefs = localRef.Local.DeclaringSyntaxReferences;
                if (declaratorRefs.Length == 1 &&
                    declaratorRefs[0].GetSyntax() is VariableDeclaratorSyntax declarator &&
                    declarator.Initializer is not null &&
                    localRef.SemanticModel is not null)
                {
                    currentReceiver = localRef.SemanticModel.GetOperation(declarator.Initializer.Value);
                    continue;
                }
            }

            break;
        }

        return null;
    }

    private static IOperation? GetExtensionReceiver(IInvocationOperation invocation)
    {
        if (invocation.Instance is not null)
        {
            return invocation.Instance;
        }

        if (invocation.TargetMethod.IsExtensionMethod && invocation.Arguments.Length > 0)
        {
            return invocation.Arguments[0].Value;
        }

        return null;
    }

    private static ResolvedIdentifier ExtractIdentifier(IOperation operation)
    {
        operation = UnwrapConversions(operation);

        if (operation.ConstantValue.HasValue && operation.ConstantValue.Value is string constStr)
        {
            return new ResolvedIdentifier($"val:{constStr}", constStr);
        }

        if (operation is ILocalReferenceOperation localRef)
        {
            var declaringRefs = localRef.Local.DeclaringSyntaxReferences;
            if (declaringRefs.Length == 1 &&
                declaringRefs[0].GetSyntax() is VariableDeclaratorSyntax declarator &&
                declarator.Initializer is not null &&
                operation.SemanticModel is not null)
            {
                var initOp = operation.SemanticModel.GetOperation(declarator.Initializer.Value);
                if (initOp is not null)
                {
                    initOp = UnwrapConversions(initOp);
                    if (initOp.ConstantValue.HasValue && initOp.ConstantValue.Value is string initConstStr)
                    {
                        return new ResolvedIdentifier($"val:{initConstStr}", initConstStr);
                    }
                }
            }

            var locStart = localRef.Local.Locations.FirstOrDefault()?.SourceSpan.Start ?? 0;
            return new ResolvedIdentifier($"sym:{localRef.Local.Name}:{locStart}", localRef.Local.Name);
        }

        if (operation is IParameterReferenceOperation paramRef)
        {
            var locStart = paramRef.Parameter.Locations.FirstOrDefault()?.SourceSpan.Start ?? 0;
            return new ResolvedIdentifier($"sym:{paramRef.Parameter.Name}:{locStart}", paramRef.Parameter.Name);
        }

        if (operation is IFieldReferenceOperation fieldRef)
        {
            return new ResolvedIdentifier($"field:{fieldRef.Field.ContainingType?.ToDisplayString()}.{fieldRef.Field.Name}", fieldRef.Field.Name);
        }

        if (operation is IPropertyReferenceOperation propRef)
        {
            return new ResolvedIdentifier($"prop:{propRef.Property.ContainingType?.ToDisplayString()}.{propRef.Property.Name}", propRef.Property.Name);
        }

        var rawSyntax = operation.Syntax.ToString().Trim();
        return new ResolvedIdentifier($"expr:{rawSyntax}", rawSyntax);
    }

    private static IOperation UnwrapConversions(IOperation operation)
    {
        while (operation is IConversionOperation conversion)
        {
            operation = conversion.Operand;
        }

        return operation;
    }

    private static Location GetInvocationIdentifierLocation(IInvocationOperation invocation)
    {
        if (invocation.Syntax is InvocationExpressionSyntax invocationSyntax)
        {
            if (invocationSyntax.Expression is MemberAccessExpressionSyntax memberAccess)
            {
                return memberAccess.Name.GetLocation();
            }

            return invocationSyntax.Expression.GetLocation();
        }

        return invocation.Syntax.GetLocation();
    }
}