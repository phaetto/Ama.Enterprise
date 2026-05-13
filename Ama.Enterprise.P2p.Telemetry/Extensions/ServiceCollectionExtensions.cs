namespace Ama.Enterprise.P2p.Telemetry.Extensions;

using Ama.CRDT.Extensions;
using Ama.Enterprise.P2p.Services.Core;
using Ama.Enterprise.P2p.Telemetry.Models;
using Ama.Enterprise.P2p.Telemetry.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using System;

/// <summary>
/// Dependency injection registrations ensuring explicit metric forwarding scopes initialize.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers strict internal generic telemetry trackers interpreting explicit P2P metrics mappings actively bounding generic network configurations.
    /// </summary>
    /// <param name="services">Target IServiceCollection container evaluating parameters implicitly.</param>
    /// <param name="configure">Optional specific action configuring underlying decoupled behaviors explicitly.</param>
    /// <returns>Transitive IServiceCollection ensuring cascaded registrations.</returns>
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

        services.AddCrdtJsonTypeInfoResolver(TelemetryJsonContext.Default);
        services.AddSingleton<TelemetryPushAlgorithm>();
        services.AddHostedService<TelemetryForwarderService>();

        return services;
    }

    /// <summary>
    /// Registers the centralized telemetry aggregator and wires the specialized application payload handler to receive explicitly mapped generic metrics.
    /// </summary>
    /// <param name="services">Target IServiceCollection container evaluating parameters implicitly.</param>
    /// <param name="meshId">The explicit P2P network mesh identifier bounding inbound network telemetry scopes.</param>
    /// <returns>Transitive IServiceCollection ensuring cascaded registrations.</returns>
    public static IServiceCollection AddP2pTelemetryAggregator(this IServiceCollection services, string meshId)
    {
        if (services is null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (string.IsNullOrWhiteSpace(meshId))
        {
            throw new ArgumentException("Explicit mesh identifier cannot be null or whitespace mapping generic configurations.", nameof(meshId));
        }

        services.AddCrdtJsonTypeInfoResolver(TelemetryJsonContext.Default);
        services.TryAddSingleton<ITelemetryAggregator, TelemetryAggregator>();
        services.AddKeyedSingleton<IApplicationPayloadHandler, TelemetryPayloadHandler>(meshId);

        return services;
    }
}