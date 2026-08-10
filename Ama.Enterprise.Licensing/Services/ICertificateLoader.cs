namespace Ama.Enterprise.Licensing.Services;

using System.Reflection;
using System.Security.Cryptography.X509Certificates;

/// <summary>
/// Defines a contract for loading X.509 certificates from different origins.
/// </summary>
public interface ICertificateLoader
{
    /// <summary>
    /// Loads a certificate from a file path.
    /// </summary>
    /// <param name="filePath">The absolute or relative path to the certificate file.</param>
    /// <param name="password">The optional password for the certificate.</param>
    /// <returns>The loaded certificate, or null if it cannot be loaded.</returns>
    X509Certificate2? LoadFromFile(string filePath, string? password = null);

    /// <summary>
    /// Loads a certificate from a Base64 encoded string.
    /// </summary>
    /// <param name="base64String">The Base64 encoded representation of the certificate.</param>
    /// <param name="password">The optional password for the certificate.</param>
    /// <returns>The loaded certificate, or null if it cannot be loaded.</returns>
    X509Certificate2? LoadFromBase64(string base64String, string? password = null);

    /// <summary>
    /// Loads a certificate from a PEM string.
    /// </summary>
    /// <param name="pemString">The PEM formatted string containing the certificate.</param>
    /// <returns>The loaded certificate, or null if it cannot be loaded.</returns>
    X509Certificate2? LoadFromPem(string pemString);

    /// <summary>
    /// Loads a certificate from the operating system certificate store via its thumbprint.
    /// </summary>
    /// <param name="thumbprint">The thumbprint of the certificate.</param>
    /// <param name="storeName">The name of the store. Defaults to My.</param>
    /// <param name="storeLocation">The location of the store. Defaults to CurrentUser.</param>
    /// <returns>The loaded certificate, or null if it cannot be found.</returns>
    X509Certificate2? LoadFromStore(string thumbprint, StoreName storeName = StoreName.My, StoreLocation storeLocation = StoreLocation.CurrentUser);

    /// <summary>
    /// Loads a public certificate from an embedded resource in the specified assembly.
    /// </summary>
    /// <param name="assembly">The assembly containing the embedded resource.</param>
    /// <param name="resourceName">The exact logical name of the embedded resource.</param>
    /// <returns>The loaded public certificate, or null if it cannot be loaded.</returns>
    X509Certificate2? LoadFromEmbeddedResource(Assembly assembly, string resourceName);
}