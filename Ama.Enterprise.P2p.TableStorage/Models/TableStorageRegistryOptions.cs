namespace Ama.Enterprise.P2p.TableStorage.Models;

/// <summary>
/// Configuration options for connecting to Azure Table Storage for the peer registry.
/// </summary>
public sealed class TableStorageRegistryOptions
{
    /// <summary>
    /// Gets or sets the Azure Table Storage connection string.
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the name of the table to use for storing peer records.
    /// </summary>
    public string TableName { get; set; } = "P2pPeers";

    /// <summary>
    /// Gets or sets the logical partition key used to isolate specific P2P clusters within the same table.
    /// </summary>
    public string PartitionKey { get; set; } = "DefaultCluster";
}