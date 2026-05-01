namespace Ama.Enterprise.P2p.Services.Discovery;

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.P2p.Models.Discovery;

/// <summary>
/// Interface defining the contract for resolving DNS SRV records.
/// Designed to abstract over third-party libraries since the underlying .NET framework lacks native SRV resolution implementations.
/// </summary>
public interface IDnsSrvResolver
{
    /// <summary>
    /// Resolves the specified hostname for SRV records, returning the target hostnames and respective ports.
    /// </summary>
    /// <param name="hostname">The DNS SRV hostname to resolve.</param>
    /// <param name="cancellationToken">The cancellation token to observe.</param>
    /// <returns>A collection of resolved SRV targets including target hosts and ports.</returns>
    Task<IEnumerable<SrvRecordTarget>> ResolveSrvAsync(string hostname, CancellationToken cancellationToken);
}