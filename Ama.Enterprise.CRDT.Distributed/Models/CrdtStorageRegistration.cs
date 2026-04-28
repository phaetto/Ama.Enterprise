namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Configuration mapping defining a registered explicit storage backend resolving dynamically bounded CRDT aliases natively.
/// </summary>
public sealed record CrdtStorageRegistration(string Key);