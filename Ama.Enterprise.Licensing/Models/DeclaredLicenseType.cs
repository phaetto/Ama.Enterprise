namespace Ama.Enterprise.Licensing.Models;

/// <summary>
/// Defines the declared license type to demonstrate acceptance of terms.
/// </summary>
public enum DeclaredLicenseType
{
    /// <summary>
    /// No license type declared.
    /// </summary>
    Unknown = 0,

    /// <summary>
    /// Open Source license terms accepted.
    /// </summary>
    OpenSource = 1,

    /// <summary>
    /// Enterprise license terms accepted.
    /// </summary>
    Enterprise = 2
}