namespace Ama.Enterprise.P2p.Services.Core;

using System;
using System.IO;
using System.Security.Cryptography;
using Ama.Enterprise.P2p.Models.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Implementation tracking generic honor-based checks evaluating provided bounds explicitly natively.
/// </summary>
public sealed class HonorLicenseManager : ILicenseManager
{
    private readonly LicenseOptions options;
    private readonly ILogger<HonorLicenseManager> logger;

    /// <inheritdoc />
    public string LicenseType { get; private set; } = "Open Source License";

    /// <summary>
    /// Initializes a new instance of the <see cref="HonorLicenseManager"/> class.
    /// </summary>
    public HonorLicenseManager(IOptions<LicenseOptions> options, ILogger<HonorLicenseManager> logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        this.options = options.Value;
        this.logger = logger;
    }

    /// <inheritdoc />
    public void ValidateLicense()
    {
        var key = options.LicenseKey;

        if (string.IsNullOrWhiteSpace(key) && !string.IsNullOrWhiteSpace(options.LicenseFilePath))
        {
            key = LoadLicenseFromFile(options.LicenseFilePath);
        }

        if (string.IsNullOrWhiteSpace(key))
        {
            LicenseType = "Open Source License";
            // TODO: Custom text for the Open Source default fallback can be updated here.
            logger.LogInformation("No honor license provided. Defaulting natively to: {LicenseType}. Custom text: 'Thank you for using the Open Source version.'", LicenseType);
            return;
        }

        if (VerifyLicense(key, options.PublicKeyPem))
        {
            LicenseType = "Enterprise License";
            logger.LogInformation("Valid honor license detected. Operating under explicitly defined bound: {LicenseType}.", LicenseType);
        }
        else
        {
            LicenseType = "Open Source License";
            // TODO: Custom text for the Open Source default fallback can be updated here.
            logger.LogWarning("Invalid honor license provided. Defaulting natively back to: {LicenseType}. Custom text: 'Thank you for using the Open Source version.'", LicenseType);
        }
    }

    private string? LoadLicenseFromFile(string filePath)
    {
        try
        {
            if (File.Exists(filePath))
            {
                return File.ReadAllText(filePath).Trim();
            }
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Failed to read the specific honor license file.");
        }

        return null;
    }

    private bool VerifyLicense(string licenseKey, string? publicKeyPem)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(publicKeyPem))
            {
                return false;
            }

            var parts = licenseKey.Split('.');
            if (parts.Length != 2)
            {
                return false;
            }

            var payloadBytes = Convert.FromBase64String(parts[0]);
            var signatureBytes = Convert.FromBase64String(parts[1]);

            using var rsa = RSA.Create();
            rsa.ImportFromPem(publicKeyPem.AsSpan());

            return rsa.VerifyData(payloadBytes, signatureBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cryptographic license validation failed.");
            return false;
        }
    }
}