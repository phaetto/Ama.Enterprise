namespace Ama.Enterprise.CRDT.Distributed.Models;

/// <summary>
/// Envelope wrapper for generic CRDT messages sent over the P2P gossip network.
/// </summary>
public readonly record struct CrdtMessageWrapper(string? DocumentId, string? MessageType, byte[]? Payload);