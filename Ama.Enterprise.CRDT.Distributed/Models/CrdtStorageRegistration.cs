namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Configuration mapping defining a registered explicit storage backend resolving dynamically bounded CRDT aliases or specific identities natively.
/// </summary>
public sealed record CrdtStorageRegistration(string Key, string TargetValue, CrdtStorageRoutingType RoutingType);