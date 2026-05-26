namespace Ama.Enterprise.Licensing.UnitTests.Services;

using System;
using System.IO;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Ama.Enterprise.Licensing.Services;
using Microsoft.Extensions.Logging;
using Moq;
using Shouldly;
using Xunit;

public sealed class CertificateLoaderTests : IDisposable
{
    private readonly Mock<ILogger<CertificateLoader>> loggerMock;
    private readonly CertificateLoader sut;
    private readonly X509Certificate2 testCertificate;
    private readonly string tempFilePath;

    public CertificateLoaderTests()
    {
        loggerMock = new Mock<ILogger<CertificateLoader>>();
        sut = new CertificateLoader(loggerMock.Object);

        testCertificate = GenerateTestCertificate();
        tempFilePath = Path.GetTempFileName();
    }

    [Fact]
    public void Constructor_WithNullLogger_ThrowsArgumentNullException()
    {
        Action action = () => new CertificateLoader(null!);
        action.ShouldThrow<ArgumentNullException>();
    }

    [Fact]
    public void LoadFromFile_WithValidFile_ReturnsCertificate()
    {
        var bytes = testCertificate.Export(X509ContentType.Cert);
        File.WriteAllBytes(tempFilePath, bytes);

        var result = sut.LoadFromFile(tempFilePath);

        result.ShouldNotBeNull();
        result.Subject.ShouldBe("CN=test");
    }

    [Fact]
    public void LoadFromFile_WithMissingFile_ReturnsNull()
    {
        var missingPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

        var result = sut.LoadFromFile(missingPath);

        result.ShouldBeNull();
    }

    [Fact]
    public void LoadFromFile_WithNullPath_ThrowsArgumentException()
    {
        Action action = () => sut.LoadFromFile(string.Empty);
        action.ShouldThrow<ArgumentException>();
    }

    [Fact]
    public void LoadFromFile_WithInvalidFile_ReturnsNull()
    {
        File.WriteAllText(tempFilePath, "not a real cert");

        var result = sut.LoadFromFile(tempFilePath);

        result.ShouldBeNull();
    }

    [Fact]
    public void LoadFromBase64_WithValidBase64_ReturnsCertificate()
    {
        var bytes = testCertificate.Export(X509ContentType.Cert);
        var base64 = Convert.ToBase64String(bytes);

        var result = sut.LoadFromBase64(base64);

        result.ShouldNotBeNull();
        result.Subject.ShouldBe("CN=test");
    }

    [Fact]
    public void LoadFromBase64_WithInvalidBase64_ReturnsNull()
    {
        var result = sut.LoadFromBase64("this_is_not_base64!");

        result.ShouldBeNull();
    }

    [Fact]
    public void LoadFromBase64_WithNullOrEmpty_ThrowsArgumentException()
    {
        Action action = () => sut.LoadFromBase64(" ");
        action.ShouldThrow<ArgumentException>();
    }

    [Fact]
    public void LoadFromPem_WithValidPem_ReturnsCertificate()
    {
        var pem = testCertificate.ExportCertificatePem();
        
        var result = sut.LoadFromPem(pem);

        result.ShouldNotBeNull();
        result.Subject.ShouldBe("CN=test");
    }

    [Fact]
    public void LoadFromPem_WithInvalidPem_ReturnsNull()
    {
        var result = sut.LoadFromPem("-----BEGIN CERTIFICATE-----\ninvalid\n-----END CERTIFICATE-----");

        result.ShouldBeNull();
    }

    [Fact]
    public void LoadFromPem_WithNullOrEmpty_ThrowsArgumentException()
    {
        Action action = () => sut.LoadFromPem(null!);
        action.ShouldThrow<ArgumentException>();
    }

    [Fact]
    public void LoadFromStore_WithNullThumbprint_ThrowsArgumentException()
    {
        Action action = () => sut.LoadFromStore(string.Empty);
        action.ShouldThrow<ArgumentException>();
    }

    [Fact]
    public void LoadFromStore_WithNonExistentThumbprint_ReturnsNull()
    {
        var fakeThumbprint = new string('A', 40);
        
        var result = sut.LoadFromStore(fakeThumbprint);

        result.ShouldBeNull();
    }

    public void Dispose()
    {
        testCertificate.Dispose();
        
        if (File.Exists(tempFilePath))
        {
            try
            {
                File.Delete(tempFilePath);
            }
            catch
            {
                // Ignore cleanup errors in tests
            }
        }
    }

    private static X509Certificate2 GenerateTestCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("cn=test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var cert = request.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
        return new X509Certificate2(cert.Export(X509ContentType.Pfx));
    }
}