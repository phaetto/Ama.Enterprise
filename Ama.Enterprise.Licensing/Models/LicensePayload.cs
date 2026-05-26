namespace Ama.Enterprise.Licensing.Models;

using System;
using System.Text.Json.Serialization;

/// <summary>
/// Data structure representing the explicit JSON payload contained within a cryptographic enterprise license.
/// </summary>
public readonly record struct LicensePayload(
    [property: JsonPropertyName("companyName")] string CompanyName,
    [property: JsonPropertyName("registrationDate")] DateTimeOffset RegistrationDate
);