namespace Ama.Enterprise.P2p.AspNetCore.Models;

using System.Text.Json.Serialization;

/// <summary>
/// Generic context guaranteeing isolated AOT interoperability evaluating exclusively standard ASP.NET Core endpoint bindings.
/// </summary>
[JsonSerializable(typeof(AspNetCorePeerEndpoint))]
public partial class AspNetCoreJsonContext : JsonSerializerContext
{
}