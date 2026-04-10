namespace Ama.Enterprise.CRDT.Distributed.ShowCase.Models;

/// <summary>
/// Data structure representing an individual task item.
/// </summary>
public readonly record struct TaskItem(string Id, string Description, bool IsDone);