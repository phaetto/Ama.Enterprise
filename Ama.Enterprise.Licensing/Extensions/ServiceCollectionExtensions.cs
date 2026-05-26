namespace Ama.Enterprise.Licensing.Extensions;

using System;
using System.Linq;
using Ama.Enterprise.Licensing.Models;
using Ama.Enterprise.Licensing.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Provides extension methods for registering licensing dependencies.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the honor-based licensing checks.
    /// </summary>
    /// <param name="services">The service collection to add the licensing dependencies to.</param>
    /// <returns>The updated service collection.</returns>
    /// <remarks>
    /// This method registers the internal licensing managers and background startup services. 
    /// You must also invoke <see cref="ConfigureAmaEnterpriseLicense"/> to set the required configuration.
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.AddAmaEnterpriseLicense();
    /// </code>
    /// </example>
    public static IServiceCollection AddAmaEnterpriseLicense(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<ICertificateLoader, CertificateLoader>();
        services.TryAddSingleton<ILicenseManager, HonorLicenseManager>();

        if (!services.Any(s => s.ImplementationType == typeof(LicenseStartupService)))
        {
            services.AddHostedService<LicenseStartupService>();
        }

        return services;
    }

    /// <summary>
    /// Configures the options for the honor-based licensing setup.
    /// </summary>
    /// <param name="services">The service collection to configure the options for.</param>
    /// <param name="configureOptions">An action to configure the <see cref="LicenseOptions"/>.</param>
    /// <returns>The updated service collection.</returns>
    /// <remarks>
    /// It is required to set the <see cref="LicenseOptions.DeclaredLicenseType"/> property. 
    /// If left unset or set to Unknown, the licensing manager will log an error.
    /// </remarks>
    /// <example>
    /// To declare an Open Source license:
    /// <code>
    /// builder.Services.ConfigureAmaEnterpriseLicense(options =>
    /// {
    ///     options.DeclaredLicenseType = DeclaredLicenseType.OpenSource;
    /// });
    /// </code>
    /// 
    /// To declare an Enterprise license:
    /// <code>
    /// builder.Services.ConfigureAmaEnterpriseLicense(options =>
    /// {
    ///     options.DeclaredLicenseType = DeclaredLicenseType.Enterprise;
    ///     options.LicenseKey = "your-license-key-here";
    /// });
    /// </code>
    /// </example>
    public static IServiceCollection ConfigureAmaEnterpriseLicense(
        this IServiceCollection services,
        Action<LicenseOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.Configure(configureOptions);

        return services;
    }
}