namespace Ama.Enterprise.Licensing.Models;

using System.Text.Json.Serialization;

/// <summary>
/// Source-generated AOT JSON serialization context resolving native bounds for license parsing structurally.
/// </summary>
#pragma warning disable CRDTPROJ0006 // Local configurations or non-distributed payloads should not be serialized via P2P contexts.
[JsonSerializable(typeof(LicensePayload))]
[JsonSourceGenerationOptions(WriteIndented = false)]
public sealed partial class LicensingJsonContext : JsonSerializerContext
{
}
#pragma warning restore CRDTPROJ0006