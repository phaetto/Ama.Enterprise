namespace Ama.Enterprise.Licensing.Models;

using System;
using System.Text.Json.Serialization;

/// <summary>
/// Data structure representing the explicit JSON payload contained within a cryptographic enterprise license.
/// </summary>
/// <param name="LicenseId">The unique identifier of the issued license.</param>
/// <param name="CompanyName">The name of the company or organization holding the license.</param>
/// <param name="ContactEmail">The primary contact email for the license holder.</param>
/// <param name="Tier">The enterprise licensing tier (e.g., Professional, Enterprise).</param>
/// <param name="RegistrationDate">The date and time when the license was generated and registered.</param>
/// <param name="ExpirationDate">The date and time when the license expires, or null if it is a perpetual license.</param>
public readonly record struct LicensePayload(
    [property: JsonPropertyName("licenseId")] string LicenseId,
    [property: JsonPropertyName("companyName")] string CompanyName,
    [property: JsonPropertyName("contactEmail")] string ContactEmail,
    [property: JsonPropertyName("tier")] string Tier,
    [property: JsonPropertyName("registrationDate")] DateTimeOffset RegistrationDate,
    [property: JsonPropertyName("expirationDate")] DateTimeOffset? ExpirationDate
);