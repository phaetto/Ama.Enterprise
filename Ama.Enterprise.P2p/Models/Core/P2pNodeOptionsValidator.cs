namespace Ama.Enterprise.P2p.Models.Core;

using System;
using Microsoft.Extensions.Options;

/// <summary>
/// Validates the P2P node configuration options ensuring logical boundaries and valid interval values.
/// </summary>
public sealed class P2pNodeOptionsValidator : IValidateOptions<P2pNodeOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, P2pNodeOptions options)
    {
        if (options is null)
        {
            return ValidateOptionsResult.Fail("Options instance cannot be null.");
        }

        if (options.MinActivePeers < 0)
        {
            return ValidateOptionsResult.Fail($"{nameof(options.MinActivePeers)} must be greater than or equal to 0.");
        }

        if (options.MaxActivePeers < options.MinActivePeers)
        {
            return ValidateOptionsResult.Fail($"{nameof(options.MaxActivePeers)} must be greater than or equal to {nameof(options.MinActivePeers)}.");
        }

        if (options.MaxDiscoveryInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(options.MaxDiscoveryInterval)} must be greater than zero.");
        }

        if (options.HealthCheckInterval <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(options.HealthCheckInterval)} must be greater than zero.");
        }

        if (options.InitialDiscoveryDelay < TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail($"{nameof(options.InitialDiscoveryDelay)} must be greater than or equal to zero.");
        }

        return ValidateOptionsResult.Success;
    }
}