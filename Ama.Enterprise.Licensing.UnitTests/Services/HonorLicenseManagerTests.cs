namespace Ama.Enterprise.Licensing.UnitTests.Services;

using System;
using System.Security.Cryptography;
using System.Text;
using Ama.Enterprise.Licensing.Models;
using Ama.Enterprise.Licensing.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Shouldly;
using Xunit;

public sealed class HonorLicenseManagerTests
{
    [Fact]
    public void ValidateLicense_WithValidCryptographicSignature_ShouldSetEnterpriseLicense()
    {
        // Arrange
        using var rsa = RSA.Create(2048);
        var publicKeyPem = rsa.ExportSubjectPublicKeyInfoPem();
        
        var payload = "EnterpriseVersion=1.0";
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var signatureBytes = rsa.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";
        
        var options = Options.Create(new LicenseOptions
        {
            LicenseKey = licenseKey,
            PublicKeyPem = publicKeyPem
        });
        
        var manager = new HonorLicenseManager(options, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Enterprise License");
    }

    [Fact]
    public void ValidateLicense_WithInvalidSignature_ShouldFallbackToOpenSourceLicense()
    {
        // Arrange
        using var rsa = RSA.Create(2048);
        using var rsaInvalid = RSA.Create(2048);
        var publicKeyPem = rsa.ExportSubjectPublicKeyInfoPem();
        
        var payload = "EnterpriseVersion=1.0";
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var signatureBytes = rsaInvalid.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";
        
        var options = Options.Create(new LicenseOptions
        {
            LicenseKey = licenseKey,
            PublicKeyPem = publicKeyPem
        });
        
        var manager = new HonorLicenseManager(options, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Open Source License");
    }

    [Fact]
    public void ValidateLicense_WithMalformedLicense_ShouldFallbackToOpenSourceLicense()
    {
        // Arrange
        using var rsa = RSA.Create(2048);
        var publicKeyPem = rsa.ExportSubjectPublicKeyInfoPem();
        
        var options = Options.Create(new LicenseOptions
        {
            LicenseKey = "malformed-license-key",
            PublicKeyPem = publicKeyPem
        });
        
        var manager = new HonorLicenseManager(options, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Open Source License");
    }

    [Fact]
    public void ValidateLicense_WithEmptyLicense_ShouldDefaultToOpenSourceLicense()
    {
        // Arrange
        var options = Options.Create(new LicenseOptions
        {
            LicenseKey = string.Empty
        });
        
        var manager = new HonorLicenseManager(options, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Open Source License");
    }
}