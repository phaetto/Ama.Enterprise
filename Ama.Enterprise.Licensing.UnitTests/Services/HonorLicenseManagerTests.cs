namespace Ama.Enterprise.Licensing.UnitTests.Services;

using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Ama.Enterprise.Licensing.Models;
using Ama.Enterprise.Licensing.Services;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Shouldly;
using Xunit;

public sealed class HonorLicenseManagerTests
{
    [Fact]
    public void ValidateLicense_WithValidCryptographicSignature_ShouldSetEnterpriseLicense()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var privateKey);
        using var keyRef = privateKey;
        
        var payload = "EnterpriseVersion=1.0";
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var signatureBytes = privateKey.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";
        
        var options = Options.Create(new LicenseOptions
        {
            LicenseKey = licenseKey,
            CertificatePem = "mocked-pem"
        });

        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns(publicCert);
        
        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Enterprise License");
    }

    [Fact]
    public void ValidateLicense_WithInvalidSignature_ShouldFallbackToOpenSourceLicense()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var _);
        using var rsaInvalid = RSA.Create(2048);
        
        var payload = "EnterpriseVersion=1.0";
        var payloadBytes = Encoding.UTF8.GetBytes(payload);
        var signatureBytes = rsaInvalid.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";
        
        var options = Options.Create(new LicenseOptions
        {
            LicenseKey = licenseKey,
            CertificatePem = "mocked-pem"
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns(publicCert);

        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Open Source License");
    }

    [Fact]
    public void ValidateLicense_WithMalformedLicense_ShouldFallbackToOpenSourceLicense()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var privateKey);
        using var keyRef = privateKey;
        
        var options = Options.Create(new LicenseOptions
        {
            LicenseKey = "malformed-license-key",
            CertificatePem = "mocked-pem"
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns(publicCert);

        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

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
        
        var loaderMock = new Mock<ICertificateLoader>();
        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Open Source License");
    }

    private static X509Certificate2 GenerateTestKeypair(out RSA privateKey)
    {
        privateKey = RSA.Create(2048);
        var request = new CertificateRequest("cn=test", privateKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
        return new X509Certificate2(cert.Export(X509ContentType.Cert));
    }
}