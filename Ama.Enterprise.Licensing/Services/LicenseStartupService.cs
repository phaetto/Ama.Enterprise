namespace Ama.Enterprise.Licensing.Services;

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;

/// <summary>
/// Background startup service tracking the single explicit license validation step without overlapping boundaries.
/// </summary>
public sealed class LicenseStartupService : IHostedService
{
    private readonly ILicenseManager licenseManager;

    /// <summary>
    /// Initializes a new instance of the <see cref="LicenseStartupService"/> class.
    /// </summary>
    public LicenseStartupService(ILicenseManager licenseManager)
    {
        ArgumentNullException.ThrowIfNull(licenseManager);
        this.licenseManager = licenseManager;
    }

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        licenseManager.ValidateLicense();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}