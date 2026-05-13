namespace Ama.Enterprise.P2p.Models.Transports;

using System;
using Ama.Enterprise.P2p.Models.Core;

/// <summary>
/// Represents an isolated UDP network address where a peer can be directly reached.
/// </summary>
public sealed record UdpPeerEndpoint(string Host, int Port) : PeerEndpoint;