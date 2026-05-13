namespace Ama.Enterprise.P2p.Services.Core;

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
    /// Validates the configured license options dynamically logging states without blocking execution natively.
    /// </summary>
    void ValidateLicense();
}