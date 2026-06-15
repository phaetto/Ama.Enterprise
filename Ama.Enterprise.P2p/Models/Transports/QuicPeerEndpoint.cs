namespace Ama.Enterprise.P2p.Models.Transports;

using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents an isolated QUIC network address where a peer can be directly reached.
/// </summary>
public sealed record QuicPeerEndpoint(string Host, int Port) : PeerEndpoint, IExtensibleDistributedPayload;