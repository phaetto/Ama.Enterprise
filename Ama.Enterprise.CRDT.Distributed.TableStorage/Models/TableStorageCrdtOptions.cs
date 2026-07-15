namespace Ama.Enterprise.CRDT.Distributed.TableStorage.Models;

/// <summary>
/// Configuration structure holding Azure Table Storage endpoints and table bindings.
/// </summary>
public sealed class TableStorageCrdtOptions
{
    /// <summary>
    /// Gets or sets the Azure Table Storage connection string used to authenticate the client.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the target table name where distributed CRDT state and journal structures reside.
    /// </summary>
    public string TableName { get; set; } = "DistributedCrdtStorage";

    /// <summary>
    /// Gets or sets a value indicating whether to use binary serialization for storage payloads.
    /// When true, a registered binary <see cref="Ama.CRDT.Services.Serialization.ICrdtSerializer"/> (like MessagePack) will be used.
    /// When false, the default <see cref="Ama.CRDT.Services.Serialization.JsonCrdtSerializer"/> is utilized allowing for human-readable rows.
    /// </summary>
    public bool UseBinarySerialization { get; set; }
}