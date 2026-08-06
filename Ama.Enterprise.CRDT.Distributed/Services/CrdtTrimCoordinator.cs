namespace Ama.Enterprise.CRDT.Distributed.Services;

using System.Threading;

/// <summary>
/// Coordinates trimming operations across the entire node to ensure only one aggressive 
/// or maintenance trim runs at a time, preventing IO and memory saturation.
/// </summary>
internal static class CrdtTrimCoordinator
{
    /// <summary>
    /// A global semaphore ensuring single-threaded execution of CRDT journal trims across all replicas and scopes.
    /// </summary>
    public static readonly SemaphoreSlim GlobalTrimLock = new SemaphoreSlim(1, 1);
}