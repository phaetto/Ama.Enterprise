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

            return password is null 
                ? new X509Certificate2(filePath) 
                : new X509Certificate2(filePath, password);
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
            
            return password is null 
                ? new X509Certificate2(bytes) 
                : new X509Certificate2(bytes, password);
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
            using var store = new X509Store(storeName, storeLocation);
            store.Open(OpenFlags.ReadOnly | OpenFlags.OpenExistingOnly);
            
            var certCollection = store.Certificates.Find(X509FindType.FindByThumbprint, thumbprint, validOnly: false);
            
            if (certCollection.Count > 0)
            {
                return certCollection[0];
            }

            logger.LogWarning("Certificate with thumbprint {Thumbprint} not found in store {StoreName} at {StoreLocation}.", thumbprint, storeName, storeLocation);
            return null;
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to load certificate from store using thumbprint: {Thumbprint}", thumbprint);
            return null;
        }
    }
}