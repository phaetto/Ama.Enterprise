namespace Ama.Enterprise.Licensing.UnitTests.Services;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
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
    public void ValidateLicense_WithValidCryptographicSignature_ShouldSetEnterpriseLicenseAndExtractJsonPayload()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var privateKey);
        using var keyRef = privateKey;
        
        var payloadObj = new LicensePayload("Acme Corp", new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(payloadObj, LicensingJsonContext.Default.LicensePayload);
        var signatureBytes = privateKey.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";
        
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.Enterprise,
            LicenseKey = licenseKey,
            CertificatePem = "mocked-pem"
        });

        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns(publicCert);
        
        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Enterprise");
        manager.CompanyName.ShouldBe("Acme Corp");
        manager.RegistrationDate.ShouldBe(new DateTimeOffset(2023, 1, 1, 0, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public void ValidateLicense_WithInvalidSignature_ShouldFallbackToUnknown()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var _);
        using var rsaInvalid = RSA.Create(2048);
        
        var payloadObj = new LicensePayload("Evil Corp", DateTimeOffset.UtcNow);
        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(payloadObj, LicensingJsonContext.Default.LicensePayload);
        var signatureBytes = rsaInvalid.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";
        
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.Enterprise,
            LicenseKey = licenseKey,
            CertificatePem = "mocked-pem"
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns(publicCert);

        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Unknown");
        manager.CompanyName.ShouldBeNull();
        manager.RegistrationDate.ShouldBeNull();
    }

    [Fact]
    public void ValidateLicense_WithMalformedLicense_ShouldFallbackToUnknown()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var privateKey);
        using var keyRef = privateKey;
        
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.Enterprise,
            LicenseKey = "malformed-license-key",
            CertificatePem = "mocked-pem"
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns(publicCert);

        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Unknown");
    }

    [Fact]
    public void ValidateLicense_WithEmptyLicense_ShouldDefaultToUnknown()
    {
        // Arrange
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.Enterprise,
            LicenseKey = string.Empty
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Unknown");
    }

    [Fact]
    public void ValidateLicense_WhenDeclaredOpenSource_ShouldSetOpenSource()
    {
        // Arrange
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.OpenSource
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Open Source");
        manager.CompanyName.ShouldBeNull();
        manager.RegistrationDate.ShouldBeNull();
    }

    [Fact]
    public void ValidateLicense_WhenDeclaredUnknown_ShouldSetUnknown()
    {
        // Arrange
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.Unknown
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Unknown");
    }

    [Fact]
    public void ValidateLicense_WithMissingCertificate_ShouldSetUnknown()
    {
        // Arrange
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.Enterprise,
            LicenseKey = "validBase64.validBase64",
            CertificatePem = "mocked-pem"
        });
        
        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns((X509Certificate2?)null);

        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Unknown");
    }

    [Fact]
    public void ValidateLicense_WithLicenseFilePath_ShouldReadAndValidate()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var privateKey);
        using var keyRef = privateKey;
        
        var payloadObj = new LicensePayload("File Corp", new DateTimeOffset(2024, 5, 10, 0, 0, 0, TimeSpan.Zero));
        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(payloadObj, LicensingJsonContext.Default.LicensePayload);
        var signatureBytes = privateKey.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";

        var tempFilePath = Path.GetTempFileName();
        try
        {
            File.WriteAllText(tempFilePath, licenseKey);

            var options = Options.Create(new LicenseOptions
            {
                DeclaredLicenseType = DeclaredLicenseType.Enterprise,
                LicenseFilePath = tempFilePath,
                CertificatePem = "mocked-pem"
            });

            var loaderMock = new Mock<ICertificateLoader>();
            loaderMock.Setup(x => x.LoadFromPem("mocked-pem")).Returns(publicCert);
            
            var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

            // Act
            manager.ValidateLicense();

            // Assert
            manager.LicenseType.ShouldBe("Enterprise");
            manager.CompanyName.ShouldBe("File Corp");
        }
        finally
        {
            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }
        }
    }
    
    [Fact]
    public void ValidateLicense_WithCertificateBase64_ShouldLoadCert()
    {
        // Arrange
        using var publicCert = GenerateTestKeypair(out var privateKey);
        using var keyRef = privateKey;
        
        var payloadObj = new LicensePayload("Base64 Corp", new DateTimeOffset(2025, 2, 2, 0, 0, 0, TimeSpan.Zero));
        var payloadBytes = JsonSerializer.SerializeToUtf8Bytes(payloadObj, LicensingJsonContext.Default.LicensePayload);
        var signatureBytes = privateKey.SignData(payloadBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        
        var licenseKey = $"{Convert.ToBase64String(payloadBytes)}.{Convert.ToBase64String(signatureBytes)}";
        
        var options = Options.Create(new LicenseOptions
        {
            DeclaredLicenseType = DeclaredLicenseType.Enterprise,
            LicenseKey = licenseKey,
            CertificateBase64 = "mocked-base64"
        });

        var loaderMock = new Mock<ICertificateLoader>();
        loaderMock.Setup(x => x.LoadFromBase64("mocked-base64", null)).Returns(publicCert);
        
        var manager = new HonorLicenseManager(options, loaderMock.Object, NullLogger<HonorLicenseManager>.Instance);

        // Act
        manager.ValidateLicense();

        // Assert
        manager.LicenseType.ShouldBe("Enterprise");
        manager.CompanyName.ShouldBe("Base64 Corp");
    }

    private static X509Certificate2 GenerateTestKeypair(out RSA privateKey)
    {
        privateKey = RSA.Create(2048);
        var request = new CertificateRequest("cn=test", privateKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
        return new X509Certificate2(cert.Export(X509ContentType.Cert));
    }
}