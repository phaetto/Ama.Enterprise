namespace Ama.Enterprise.Licensing.Services;

using System;

/// <summary>
/// Contract isolating the validation logic for the honor-based licensing system.
/// </summary>
public interface ILicenseManager
{
    /// <summary>
    /// Gets the current validated license type name.
    /// </summary>
    string LicenseType { get; }

    /// <summary>
    /// Gets the explicit company name registered within the validated license payload, if available.
    /// </summary>
    string? CompanyName { get; }

    /// <summary>
    /// Gets the exact date the license was registered natively, if available.
    /// </summary>
    DateTimeOffset? RegistrationDate { get; }

    /// <summary>
    /// Validates the configured license options dynamically logging states without blocking execution natively.
    /// </summary>
    void ValidateLicense();
}