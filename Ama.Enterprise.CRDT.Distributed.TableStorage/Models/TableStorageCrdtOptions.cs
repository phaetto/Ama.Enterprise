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
    /// Gets or sets a value indicating whether the underlying storage tables should be automatically created upon initialization.
    /// </summary>
    public bool CreateTableIfNotExists { get; set; } = true;
}