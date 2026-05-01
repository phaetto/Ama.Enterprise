namespace Ama.Enterprise.P2p.Models.Discovery;

using System;

/// <summary>
/// Data structure representing a resolved target from a DNS SRV query.
/// </summary>
/// <param name="Hostname">The underlying target hostname.</param>
/// <param name="Port">The associated target port assigned to the resolved service.</param>
public readonly record struct SrvRecordTarget(string Hostname, int Port) : IEquatable<SrvRecordTarget>;