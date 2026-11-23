namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Services;

using System;

/// <summary>
/// Default implementation of <see cref="IRbacScopeAccessEvaluator"/> that enforces role separation and US/EU regional data rules.
/// </summary>
public sealed class RbacScopeAccessEvaluator : IRbacScopeAccessEvaluator
{
    /// <inheritdoc />
    public bool IsNodeAuthorizedForScope(string nodeRole, string nodeRegion, string? scopeId)
    {
        if (string.IsNullOrWhiteSpace(scopeId))
        {
            return true;
        }

        var normalizedScope = ExtractLogicalScopeId(scopeId);
        if (!TryParseScopeRegionAndRole(normalizedScope, out var scopeOriginRegion, out var scopeRole))
        {
            return true;
        }

        var isServer = string.IsNullOrEmpty(nodeRole);

        // Rule 1: Role separation ("admin" vs "user"). Servers (role == "") host all roles; shared scopes ("shared") are accessible to both.
        if (!isServer &&
            !string.IsNullOrEmpty(scopeRole) &&
            !string.Equals(scopeRole, "shared", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(nodeRole, scopeRole, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // Rule 2 & 3: Regional rules.
        // - US-origin scopes ("us-*") are allowed in US and EU (US -> EU flow allowed).
        // - EU-origin scopes ("eu-*") must stay in EU (EU -> US blocked).
        if (string.Equals(scopeOriginRegion, "EU", StringComparison.OrdinalIgnoreCase))
        {
            return string.Equals(nodeRegion, "EU", StringComparison.OrdinalIgnoreCase);
        }

        if (string.Equals(scopeOriginRegion, "US", StringComparison.OrdinalIgnoreCase))
        {
            return string.Equals(nodeRegion, "US", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(nodeRegion, "EU", StringComparison.OrdinalIgnoreCase);
        }

        return string.Equals(nodeRegion, scopeOriginRegion, StringComparison.OrdinalIgnoreCase);
    }

    /// <inheritdoc />
    public bool IsPhysicalNodeFlowAllowed(string sourceRole, string sourceRegion, string targetRole, string targetRegion)
    {
        var isSourceServer = string.IsNullOrEmpty(sourceRole);
        var isTargetServer = string.IsNullOrEmpty(targetRole);

        if (!isSourceServer && !isTargetServer && !string.Equals(sourceRole, targetRole, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        // Physical server-to-server transport remains open so US scopes can be replicated to EU servers.
        if (isSourceServer && isTargetServer)
        {
            return true;
        }

        if (string.Equals(targetRegion, "US", StringComparison.OrdinalIgnoreCase))
        {
            return string.Equals(sourceRegion, "US", StringComparison.OrdinalIgnoreCase);
        }

        if (string.Equals(targetRegion, "EU", StringComparison.OrdinalIgnoreCase))
        {
            return string.Equals(sourceRegion, "EU", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(sourceRegion, "US", StringComparison.OrdinalIgnoreCase);
        }

        return string.Equals(sourceRegion, targetRegion, StringComparison.OrdinalIgnoreCase);
    }

    private static string ExtractLogicalScopeId(string scopeOrReplicaId)
    {
        var atIdx = scopeOrReplicaId.IndexOf('@', StringComparison.Ordinal);
        return atIdx >= 0 && atIdx < scopeOrReplicaId.Length - 1
            ? scopeOrReplicaId[(atIdx + 1)..]
            : scopeOrReplicaId;
    }

    private static bool TryParseScopeRegionAndRole(string scopeId, out string region, out string role)
    {
        region = string.Empty;
        role = string.Empty;

        var dashIdx = scopeId.IndexOf('-', StringComparison.Ordinal);
        if (dashIdx <= 0 || dashIdx >= scopeId.Length - 1)
        {
            return false;
        }

        var candidateRegion = scopeId[..dashIdx];
        if (string.Equals(candidateRegion, "node", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        region = candidateRegion;
        var remainder = scopeId[(dashIdx + 1)..];
        var secondDash = remainder.IndexOf('-', StringComparison.Ordinal);
        role = secondDash > 0 ? remainder[..secondDash] : remainder;
        return true;
    }
}