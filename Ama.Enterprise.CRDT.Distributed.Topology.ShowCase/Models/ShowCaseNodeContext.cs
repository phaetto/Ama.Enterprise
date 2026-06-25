namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;

/// <summary>
/// Data structure encapsulating local session boundaries tracking active role and region.
/// </summary>
public record ShowCaseNodeContext(string Role, string Region);