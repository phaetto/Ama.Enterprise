namespace Ama.Enterprise.FeatureFlags.Services;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Ama.Enterprise.CRDT.Distributed.Services.Observers;
using Ama.Enterprise.FeatureFlags.Models;
using Microsoft.Extensions.Logging;

/// <summary>
/// Observer tracking feature flag distributed state modifications securely decoupled from the core manager.
/// </summary>
public sealed class FeatureFlagStateObserver : IDocumentStateObserver<FeatureFlagState>
{
    private readonly IEnumerable<IFeatureFlagStateObserver> domainObservers;
    private readonly ILogger<FeatureFlagStateObserver> logger;

    public FeatureFlagStateObserver(
        IEnumerable<IFeatureFlagStateObserver> domainObservers,
        ILogger<FeatureFlagStateObserver> logger)
    {
        this.domainObservers = domainObservers ?? throw new ArgumentNullException(nameof(domainObservers));
        this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc />
    public Task OnStateChangedAsync(string documentId, CancellationToken cancellationToken = default)
    {
        if (documentId == Constants.GlobalDocumentId)
        {
            _ = Task.Run(async () => 
            {
                foreach(var observer in domainObservers)
                {
                    try 
                    { 
                        await observer.OnFeatureFlagsUpdatedAsync(cancellationToken).ConfigureAwait(false); 
                    }
                    catch(Exception ex) 
                    { 
                        logger.LogError(ex, "Error notifying domain feature flag observer decoupled from structural synchronization pipeline."); 
                    }
                }
            }, cancellationToken);
        }
        
        return Task.CompletedTask;
    }
}