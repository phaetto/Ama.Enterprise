namespace Ama.Enterprise.Licensing.Extensions;

using System;
using System.Linq;
using Ama.Enterprise.Licensing.Models;
using Ama.Enterprise.Licensing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Provides extension methods for registering generic licensing dependencies explicitly natively.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the generic honor-based licensing dependencies mapping distinct explicit structural bounds naturally.
    /// </summary>
    public static IServiceCollection AddAmaEnterpriseLicense(
        this IServiceCollection services,
        Action<LicenseOptions>? configureOptions = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        if (configureOptions is not null)
        {
            services.Configure(configureOptions);
        }
        else
        {
            services.Configure<LicenseOptions>(_ => { });
        }

        services.TryAddSingleton<ILicenseManager, HonorLicenseManager>();

        if (!services.Any(s => s.ImplementationType == typeof(LicenseStartupService)))
        {
            services.AddHostedService<LicenseStartupService>();
        }

        return services;
    }
}