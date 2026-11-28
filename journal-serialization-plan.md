# Pre-Serialized & Pooled Journaling Architecture Plan

## 1. Problem Statement & Motivation

During high-throughput write bursts or when cluster peers are temporarily unreachable (delaying journal trimming), the distributed CRDT runtime can accumulate tens of thousands of `JournaledOperation` instances in memory. 

Currently, this causes three major memory and CPU bottlenecks:

1. **Deep Managed Object Graphs on the GC Heap**:
   - Each `JournaledOperation` holds a `CrdtOperation`, which in turn holds boxed `object? Value` domain payloads, `JsonPath` strings, vector clock structures, and metadata.
   - When queued inside `InMemoryWriteAheadBuffer`, `DiskWriteAheadBuffer` (up to `WalChannelCapacity = 10,000` operations), or stored inside `MemoryCrdtStorage`, these deep object graphs survive into Gen 1 / Gen 2, causing large memory spikes and expensive GC mark phases.
2. **Double Serialization in `DiskWal` Mode**:
   - `DiskWriteAheadBuffer.Enqueue` / `EnqueueAsync` serializes every `JournaledOperation` to `byte[]` to write length-prefixed frames to the local WAL segment file.
   - It then discards those serialized bytes and enqueues the unpacked `JournaledOperation` object into its in-memory `ConcurrentQueue<JournaledOperation>`.
   - When `CrdtJournalFlushService` drains the queue and calls `IDistributedCrdtStorage.AppendBatchAsync`, storage implementations (such as `TableStorageDistributedCrdtStorage` or SQLite showcase storages) serialize the `JournaledOperation` a **second time**.
3. **High Per-Item Allocation Churn**:
   - Allocating an individual `byte[]` per operation via `ICrdtSerializer.SerializeToBytes` creates massive Gen 0 array churn during write bursts.

---

## 2. Target Architecture

### 2.1. Unpacked Routing Header + Packed Pooled Payload
Storage engines and trimming services (`TrimJournalAsync`, `GetJournalOperationsAsync`) require access to routing and causal coordinates without deserializing the operation body:
- `ShardId` (pooled via `CrdtStringPool.Shared`)
- `DocumentId` (pooled via `CrdtStringPool.Shared`)
- `OriginReplicaId` (pooled via `CrdtStringPool.Shared`)
- `GlobalClock` (`long`)
- `OperationId` (`Guid`)

By introducing a lightweight `readonly record struct` header paired with a pooled byte payload (or slab slice), the runtime can index, route, deduplicate, and trim journal entries in $O(1)$ / $O(N)$ time with **zero** boxed `CrdtOperation` object graphs on the managed heap.

### 2.2. Single-Pass Serialization Pipeline
```
[Document Mutation / Patch]
        │
        ▼
[Serialize Once into Pooled Buffer / Slab]
        │
        ├──► DiskWriteAheadBuffer: Write pre-serialized bytes directly to WAL FileStream
        │
        ├──► Queue Header + Pooled Byte Slice in Write-Ahead Buffer (Zero Object Graph)
        │
        ▼
[CrdtJournalFlushService]
        │
        ▼
[IDistributedCrdtStorage.AppendSerializedBatchAsync]
        │
        ├──► MemoryCrdtStorage: Retain compact byte[] / pooled buffer until trimmed
        ├──► TableStorage / SQLite: Persist pre-serialized bytes directly to column/blob
        └──► Return rented buffer to ArrayPool<byte>.Shared once flushed
```

---

## 3. Proposed Data Structures & Contracts

### 3.1. `SerializedJournaledOperationDto`
A `readonly record struct` in `Ama.Enterprise.CRDT.Distributed/Models`:
- `string DocumentId`
- `string ShardId`
- `string OriginReplicaId`
- `long GlobalClock`
- `Guid OperationId`
- `ReadOnlyMemory<byte> SerializedPayload` (serialized `JournaledOperation` or `CrdtOperation`)

### 3.2. `PooledJournaledOperationBuffer` (or Slab Allocator)
To avoid renting a separate `ArrayPool<byte>` array for every tiny 120-byte operation (which suffers from power-of-two bucket slack), batches enqueued together can be packed into a single rented `byte[]` slab per batch or managed via a reference-counted pooled buffer released upon `AcknowledgeBatchAsync`.

### 3.3. `IDistributedCrdtStorage` Extensions
Add raw byte batch persistence and retrieval methods (with backward-compatible default interface implementations):
- `ValueTask AppendSerializedBatchAsync(IReadOnlyList<SerializedJournaledOperationDto> operations, CancellationToken cancellationToken = default)`
  - Default implementation deserializes each payload and delegates to `AppendBatchAsync(IEnumerable<JournaledOperation>, CancellationToken)`.
  - Optimized implementations (`MemoryCrdtStorage`, `TableStorageDistributedCrdtStorage`, `ShowCaseCrdtStorage`) store the pre-serialized bytes directly using the struct header for partition/row keys and clock indexing.

---

## 4. Implementation Tasks

### Phase 1: Models & Pooled Buffer Primitives
- [ ] **Task 1.1**: Create `SerializedJournaledOperationDto` (`readonly record struct` implementing `IEquatable<SerializedJournaledOperationDto>`) in `$/Ama.Enterprise.CRDT.Distributed/Models/SerializedJournaledOperationDto.cs`.
- [ ] **Task 1.2**: Create `PooledJournalOperationBatch` (or pooled entry wrapper) in `$/Ama.Enterprise.CRDT.Distributed/Models/` that rents a contiguous `byte[]` from `ArrayPool<byte>.Shared` to hold serialized bytes for an enqueued batch of operations and returns the buffer when acknowledged/disposed.

### Phase 2: Write-Ahead Buffer (`ICrdtWriteAheadBuffer`) Refactoring
- [ ] **Task 2.1**: Update `DiskWriteAheadBuffer` so `Enqueue` and `EnqueueAsync` serialize operations once, write the frame to the WAL `FileStream`, and stage the pre-serialized pooled representation in the in-memory queue instead of holding unpacked `JournaledOperation` object references.
- [ ] **Task 2.2**: Update `InMemoryWriteAheadBuffer` to optionally pack enqueued `JournaledOperation` instances into pooled byte buffers (or support `SerializedJournaledOperationDto`) so large in-memory WAL queues do not spike the GC heap.
- [ ] **Task 2.3**: Ensure `AcknowledgeBatchAsync` and `Dispose` / `DisposeAsync` deterministically return all rented `ArrayPool<byte>.Shared` buffers even on cancellation or shutdown errors.

### Phase 3: Storage Interface & Built-in Storage Providers
- [ ] **Task 3.1**: Extend `IDistributedCrdtStorage` with `AppendSerializedBatchAsync` (including a safe default fallback implementation to `AppendBatchAsync`).
- [ ] **Task 3.2**: Update `BufferedDistributedCrdtStorageDecorator` and `CrdtJournalFlushService` to pass pre-serialized operation batches from the write-ahead buffer directly to `IDistributedCrdtStorage.AppendSerializedBatchAsync`.
- [ ] **Task 3.3**: Refactor `MemoryCrdtStorage` to store journal entries as compact metadata headers (`OriginReplicaId`, `GlobalClock`, `OperationId`, `DocumentId`) + `byte[]` payload (or pooled buffer) instead of unpacked `JournaledOperation` instances, deserializing only when `GetJournalOperationsAsync` is requested during anti-entropy delta sync.
- [ ] **Task 3.4**: Update `TableStorageDistributedCrdtStorage` (when `UseBinarySerialization = true`) to write `SerializedJournaledOperationDto.SerializedPayload` directly into the chunked binary `TableEntity` properties without re-serializing.
- [ ] **Task 3.5**: Update `ShowCaseCrdtStorage` (in both `Ama.Enterprise.CRDT.Distributed.ShowCase` and `Ama.Enterprise.CRDT.Distributed.Topology.ShowCase`) to insert `SerializedPayload` directly into the SQLite `Payload` `BLOB` column.

### Phase 4: Memory Estimation & Telemetry
- [ ] **Task 4.1**: Update `ICrdtMemoryEstimator` / `CrdtMemoryEstimator` if needed to report queued WAL and in-memory journal byte footprints accurately.

### Phase 5: Verification & Benchmarks
- [ ] **Task 5.1**: Add integration tests in `JournalingAndSnapshottingIntegrationTests.cs` verifying:
  - Single-pass serialization from `DiskWriteAheadBuffer` and `InMemoryWriteAheadBuffer` through `IDistributedCrdtStorage`.
  - Crash recovery (`RecoverAsync`) from disk WAL segments into `IDistributedCrdtStorage`.
  - Causal range queries (`GetJournalOperationsAsync`) and journal trimming (`TrimJournalAsync`) with serialized journal storage.
- [ ] **Task 5.2**: Add memory footprint assertions in `ScopeMemoryOverheadIntegrationTests.cs` verifying reduced managed heap allocation when thousands of operations are buffered in the journal.