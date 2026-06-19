namespace Ama.Enterprise.Licensing.Extensions;

using System;
using System.Linq;
using System.Runtime.Versioning;
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
    /// You must also invoke either <see cref="ConfigureAmaCommunityLicense"/> or <see cref="ConfigureAmaEnterpriseLicense"/> to set the required configuration.
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
    /// Configures the application to operate under the Community license terms.
    /// </summary>
    /// <param name="services">The service collection to configure the options for.</param>
    /// <returns>The updated service collection.</returns>
    /// <remarks>
    /// This method explicitly declares the Community intent and is safe for client-side (Blazor WebAssembly) execution.
    /// </remarks>
    /// <example>
    /// <code>
    /// builder.Services.ConfigureAmaCommunityLicense();
    /// </code>
    /// </example>
    public static IServiceCollection ConfigureAmaCommunityLicense(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton<ICertificateLoader, CertificateLoader>();
        
        services.Configure<LicenseOptions>(options =>
        {
            options.DeclaredLicenseType = DeclaredLicenseType.Community;
        });

        return services;
    }

    /// <summary>
    /// Configures the options for the Enterprise honor-based licensing setup.
    /// </summary>
    /// <param name="services">The service collection to configure the options for.</param>
    /// <param name="configureOptions">An action to configure the <see cref="LicenseOptions"/>.</param>
    /// <returns>The updated service collection.</returns>
    /// <remarks>
    /// This method automatically enforces the <see cref="DeclaredLicenseType.Enterprise"/> mode.
    /// Due to security constraints, declaring an enterprise license natively on client-side environments (such as Blazor WebAssembly) is prohibited and will throw a compilation error.
    /// </remarks>
    /// <example>
    /// To declare an Enterprise license:
    /// <code>
    /// builder.Services.ConfigureAmaEnterpriseLicense(options =>
    /// {
    ///     options.LicenseKey = "your-license-key-here";
    /// });
    /// </code>
    /// </example>
    [UnsupportedOSPlatform("browser")]
    public static IServiceCollection ConfigureAmaEnterpriseLicense(
        this IServiceCollection services,
        Action<LicenseOptions> configureOptions)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configureOptions);

        services.TryAddSingleton<ICertificateLoader, CertificateLoader>();
        
        services.Configure<LicenseOptions>(options =>
        {
            configureOptions(options);
            options.DeclaredLicenseType = DeclaredLicenseType.Enterprise;
        });

        return services;
    }
}