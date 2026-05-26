namespace Ama.Enterprise.Licensing.Services;

using System;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;

/// <summary>
/// Implementation of <see cref="ICertificateLoader"/> for retrieving X.509 certificates.
/// </summary>
public sealed class CertificateLoader : ICertificateLoader
{
    private readonly ILogger<CertificateLoader> logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="CertificateLoader"/> class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public CertificateLoader(ILogger<CertificateLoader> logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        this.logger = logger;
    }

    /// <inheritdoc />
    public X509Certificate2? LoadFromFile(string filePath, string? password = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(filePath);

        try
        {
            if (!File.Exists(filePath))
            {
                logger.LogWarning("Certificate file does not exist at path: {FilePath}", filePath);
                return null;
            }

            if (password is null)
            {
                try
                {
                    return X509CertificateLoader.LoadCertificateFromFile(filePath);
                }
                catch (System.Security.Cryptography.CryptographicException)
                {
                    return X509CertificateLoader.LoadPkcs12FromFile(filePath, null);
                }
            }

            return X509CertificateLoader.LoadPkcs12FromFile(filePath, password);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load certificate from file: {FilePath}", filePath);
            return null;
        }
    }

    /// <inheritdoc />
    public X509Certificate2? LoadFromBase64(string base64String, string? password = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(base64String);

        try
        {
            var bytes = Convert.FromBase64String(base64String);

            if (password is null)
            {
                try
                {
                    return X509CertificateLoader.LoadCertificate(bytes);
                }
                catch (System.Security.Cryptography.CryptographicException)
                {
                    return X509CertificateLoader.LoadPkcs12(bytes, null);
                }
            }

            return X509CertificateLoader.LoadPkcs12(bytes, password);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load certificate from Base64 string.");
            return null;
        }
    }

    /// <inheritdoc />
    public X509Certificate2? LoadFromPem(string pemString)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pemString);

        try
        {
            return X509Certificate2.CreateFromPem(pemString);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load certificate from PEM string.");
            return null;
        }
    }

    /// <inheritdoc />
    public X509Certificate2? LoadFromStore(string thumbprint, StoreName storeName = StoreName.My, StoreLocation storeLocation = StoreLocation.CurrentUser)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(thumbprint);

        try
        {
            // Azure App Service paths depend on uppercase thumbprints without spaces
            var sanitizedThumbprint = thumbprint.Replace(" ", string.Empty, StringComparison.Ordinal).ToUpperInvariant();

            try
            {
                using var store = new X509Store(storeName, storeLocation);
                store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
                
                var certCollection = store.Certificates.Find(X509FindType.FindByThumbprint, sanitizedThumbprint, validOnly: false);
                
                if (certCollection.Count > 0)
                {
                    return certCollection[0];
                }
            }
            catch (Exception ex)
            {
                // Accessing the X509Store can throw CryptographicException on Linux systems where no store exists.
                logger.LogDebug(ex, "X509Store search failed or is unsupported on this platform. Falling back to Linux certificate file paths.");
            }

            // Fallback for Azure App Service Linux environments which mount certificates directly to the file system
            var azureLinuxPrivatePath = $"/var/ssl/private/{sanitizedThumbprint}.p12";
            if (File.Exists(azureLinuxPrivatePath))
            {
                logger.LogInformation("Certificate loaded from Azure Linux private fallback path: {Path}", azureLinuxPrivatePath);
                return X509CertificateLoader.LoadPkcs12FromFile(azureLinuxPrivatePath, null);
            }

            var azureLinuxPublicPath = $"/var/ssl/certs/{sanitizedThumbprint}.der";
            if (File.Exists(azureLinuxPublicPath))
            {
                logger.LogInformation("Certificate loaded from Azure Linux public fallback path: {Path}", azureLinuxPublicPath);
                return X509CertificateLoader.LoadCertificateFromFile(azureLinuxPublicPath);
            }

            logger.LogWarning("Certificate with thumbprint {Thumbprint} not found in store {StoreName} at {StoreLocation} or Azure Linux fallback paths.", sanitizedThumbprint, storeName, storeLocation);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load certificate using thumbprint: {Thumbprint}", thumbprint);
            return null;
        }
    }
}