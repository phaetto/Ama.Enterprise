namespace Ama.Enterprise.FeatureFlags.UnitTests.Services;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Text.Json;
using Ama.CRDT.Models;
using Ama.Enterprise.FeatureFlags.Models.P2p;
using Ama.Enterprise.FeatureFlags.Services;
using Shouldly;
using Xunit;

public sealed class MemoryJournalTests
{
    private CrdtOperation CreateOperation(Guid id, string replicaId, long clock)
    {
        var json = $$"""{"Id":"{{id}}","ReplicaId":"{{replicaId}}","GlobalClock":{{clock}}}""";
        return JsonSerializer.Deserialize(json, FeatureFlagP2pJsonContext.Default.CrdtOperation)!;
    }

    [Fact]
    public void Append_ShouldThrowArgumentException_WhenDocumentIdIsEmpty()
    {
        var journal = new MemoryJournal();
        Should.Throw<ArgumentException>(() => journal.Append("", new List<CrdtOperation>()));
    }

    [Fact]
    public void Append_ShouldThrowArgumentNullException_WhenOperationsIsNull()
    {
        var journal = new MemoryJournal();
        Should.Throw<ArgumentNullException>(() => journal.Append("doc1", null!));
    }

    [Fact]
    public async Task Append_ShouldStoreOperations()
    {
        var journal = new MemoryJournal();
        var id1 = Guid.NewGuid();
        var op = CreateOperation(id1, "rep1", 2);
        
        journal.Append("doc1", new[] { op });

        var results = new List<JournaledOperation>();
        await foreach (var jop in journal.GetOperationsByRangeAsync("rep1", 1, 3))
        {
            results.Add(jop);
        }

        results.Count.ShouldBe(1);
        results[0].Operation.Id.ShouldBe(id1);
        results[0].DocumentId.ShouldBe("doc1");
    }

    [Fact]
    public async Task AppendAsync_ShouldStoreOperations()
    {
        var journal = new MemoryJournal();
        var id2 = Guid.NewGuid();
        var op = CreateOperation(id2, "rep1", 2);
        
        await journal.AppendAsync("doc2", new[] { op });

        var results = new List<JournaledOperation>();
        await foreach (var jop in journal.GetOperationsByRangeAsync("rep1", 1, 3))
        {
            results.Add(jop);
        }

        results.Count.ShouldBe(1);
        results[0].DocumentId.ShouldBe("doc2");
    }

    [Fact]
    public async Task GetOperationsByDotsAsync_ShouldFilterCorrectly()
    {
        var journal = new MemoryJournal();
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        journal.Append("doc1", new[] 
        { 
            CreateOperation(id1, "rep1", 1),
            CreateOperation(id2, "rep1", 2),
            CreateOperation(id3, "rep1", 3)
        });

        var results = new List<JournaledOperation>();
        await foreach (var jop in journal.GetOperationsByDotsAsync("rep1", new long[] { 1, 3 }))
        {
            results.Add(jop);
        }

        results.Count.ShouldBe(2);
        results.ShouldContain(x => x.Operation.Id == id1);
        results.ShouldContain(x => x.Operation.Id == id3);
    }

    [Fact]
    public void Trim_ShouldRemoveOlderOperations()
    {
        var journal = new MemoryJournal();
        var id1 = Guid.NewGuid();
        var id2 = Guid.NewGuid();
        var id3 = Guid.NewGuid();

        journal.Append("doc1", new[] 
        { 
            CreateOperation(id1, "rep1", 1),
            CreateOperation(id2, "rep1", 2),
            CreateOperation(id3, "rep1", 3)
        });

        var gmvv = new Dictionary<string, long> { { "rep1", 2 } };
        
        journal.Trim(gmvv);

        var results = new List<JournaledOperation>();
        var sync = Task.Run(async () => 
        {
            await foreach (var jop in journal.GetOperationsByRangeAsync("rep1", 0, 10))
            {
                results.Add(jop);
            }
        }).GetAwaiter();
        sync.GetResult();

        results.Count.ShouldBe(1);
        results[0].Operation.Id.ShouldBe(id3);
    }
}