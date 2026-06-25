namespace Ama.Enterprise.CRDT.Distributed.Topology.ShowCase.Models;

/// <summary>
/// Data structure representing the status of an IoT device.
/// </summary>
public readonly record struct DeviceStatus(string DeviceId, bool IsOnline, int BatteryLevel);