namespace Ama.Enterprise.P2p.Models.Core;

using System;
using System.Text.Json.Serialization;

/// <summary>
/// Represents the abstract base network address where a peer can be reached.
/// Designed for polymorphism to support HTTP, WebRTC, and other future transport paradigms.
/// </summary>
[JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
[JsonDerivedType(typeof(HttpPeerEndpoint), "http")]
[JsonDerivedType(typeof(WebRtcPeerEndpoint), "webrtc")]
public abstract record PeerEndpoint : IEquatable<PeerEndpoint>;