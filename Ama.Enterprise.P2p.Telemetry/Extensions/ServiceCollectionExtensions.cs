namespace Ama.Enterprise.P2p.Telemetry.Extensions;

using System;
using Ama.Enterprise.P2p.Telemetry.Models;
using Ama.Enterprise.P2p.Telemetry.Services;
using Microsoft.Extensions.DependencyInjection;

/// <summary>
/// Dependency injection registrations ensuring explicit metric forwarding scopes initialize safely.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers strict internal generic telemetry trackers interpreting explicit P2P metrics mappings actively bounding generic network configurations safely.
    /// </summary>
    /// <param name="services">Target IServiceCollection container evaluating parameters implicitly.</param>
    /// <param name="configure">Optional specific action configuring underlying decoupled behaviors explicitly.</param>
    /// <returns>Transitive IServiceCollection ensuring cascaded registrations cleanly.</returns>
    public static IServiceCollection AddP2pTelemetryForwarder(this IServiceCollection services, Action<TelemetryOptions>? configure = null)
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddHostedService<TelemetryForwarderService>();

        return services;
    }
}