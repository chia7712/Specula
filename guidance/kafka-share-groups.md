# Apache Kafka: Share Groups (KIP-932) Modeling Guidance

Target source: `./kafka`. The broker-side share partition lives in
`core/src/main/java/kafka/server/share/` and `server/src/main/java/org/apache/kafka/server/share/`;
the durable state is managed by `share-coordinator/`. Treat the checked-out implementation as
ground truth. This is the newest subsystem in scope and the code is still changing, so bug
archaeology should weight the last 12 months heavily.

## Required

### Goal

- Find bugs that violate the share group delivery contract: a record delivered to two
  consumers at the same time, a record acknowledged by one consumer and later redelivered,
  a record that is never delivered or never archived (stuck in flight forever), delivery
  counts that are lost or double-incremented across leader change or coordinator failover,
  and share partition state that disagrees with the share coordinator after a crash.

### Scope

- In scope: `SharePartition` (the in-memory per-partition state machine: `InFlightBatch`,
  `InFlightState`, `RecordState` AVAILABLE / ACQUIRED / ACKNOWLEDGED / ARCHIVED, acquisition
  locks and `AcquisitionLockTimerTask`, `acquire`, `acknowledge`, `releaseAcquiredRecords`,
  `nextFetchOffset`, `maybeInitialize`, `updateCacheAndOffsets`, the start offset and
  `stateEpoch`), `SharePartitionManager` (per-member sessions, `DelayedShareFetch` and the
  purgatory keys), the persister path from `SharePartition` to the share coordinator
  (`writeState`, `readStateAndMaybeUpdateLeaderEpoch`, `initializeState`, `deleteState` in
  `ShareCoordinatorShard`), and the share coordinator's snapshot / update record compaction
  (`ShareSnapshot` vs `ShareUpdate` records and `snapshotUpdateRecordsPerSnapshot`).
- Boundaries/exclusions: the share group membership protocol in the group coordinator
  (`ShareGroup` heartbeat and assignment), the client `ShareConsumer`, fetch byte accounting
  and `PartitionMaxBytesStrategy`, and the log layer. Read them only to establish contracts
  (for example, what `leaderEpoch` and `stateEpoch` mean to the coordinator).

### Priority Questions

1. When an acquisition lock times out at the same moment the consumer's acknowledge for the
   same records arrives, can the records be released to another consumer and also be marked
   ACKNOWLEDGED, or can the delivery count be incremented twice for one delivery?
   Expected behavior: exactly one of timeout and acknowledge wins; the delivery count changes
   once per delivery attempt.
2. When the partition leader moves to another broker while records are ACQUIRED, can the
   new leader re-deliver them before the old leader's acknowledgments are persisted (double
   delivery), or can it initialize from a persisted state that lacks those records so they are
   never delivered again?
   Expected behavior: the new leader's initial state reflects every acknowledgment the
   coordinator accepted; everything in flight on the old leader returns to AVAILABLE.
3. When the share coordinator's `writeState` append fails or is delayed and `SharePartition`
   has already updated its in-memory state (or its start offset), can the in-memory and
   durable state diverge permanently, so that after a restart records before the start
   offset are lost or records after it are archived without delivery?
   Expected behavior: `SharePartition` only advances state that the coordinator durably
   accepted, or re-syncs from the coordinator on failure.
4. With `share.record.lock.duration.ms`, `max delivery attempts` and `ARCHIVED` on
   exhaustion, can a record be archived while a consumer still holds it and later acknowledges
   it, or can a batch that is partially acknowledged (per-offset state) be archived or
   re-delivered as a whole?
   Expected behavior: per-offset state is respected when a batch is split, and archiving only
   happens for records that are not currently acquired.
5. When `stateEpoch` or `leaderEpoch` are bumped (group reset, partition leader change) and
   a late `writeState` from the previous epoch arrives at the coordinator, can it overwrite
   newer state, or can the share coordinator's snapshot compaction drop an update that a
   later snapshot never incorporated?
   Expected behavior: epoch fencing rejects stale writes, and every accepted update is
   reflected in the next snapshot.

### Must-cover Interactions

- `SharePartition.acquire` ↔ `AcquisitionLockTimerTask` expiry ↔ `SharePartition.acknowledge`
  / `releaseAcquiredRecords` for the same offsets, under the partition's lock and the timer
  thread.
- `SharePartition.maybeInitialize` → `readStateAndMaybeUpdateLeaderEpoch` on the coordinator
  ↔ in-flight `writeState` from the previous leader with a lower leader epoch.
- `SharePartition` state transitions → persister `writeState` → coordinator append → response
  → `SharePartition` callback, including failure and timeout at each hop.
- `updateCacheAndOffsets` on log start offset advance (retention) ↔ records in flight below
  the new start offset.
- `ShareCoordinatorShard.writeState` snapshot vs update decision ↔ coordinator failover
  and replay of `ShareSnapshot` / `ShareUpdate` records.
- `SharePartitionManager` member session close or fence ↔ records that member had acquired.
- Consider: a consumer that acknowledges after its session was fenced, two share fetches
  from the same member in flight, leader change during the persister write, and the
  coordinator moving between an `initializeState` and the first `writeState`.

### Assumptions

- The share coordinator partition for a share partition key is served by one coordinator at a
  time with per-key serialization; verify this in `ShareCoordinatorService`.
- Consumers may be slow, may retry acknowledgments, and may disappear without releasing.

## Optional

### Known Incidents and References

- KIP-932 (queues for Kafka), KIP-1103 (share group additions) and any later share-group KIPs
  in the repo's `docs/`.
- Archaeology: `git log --since=12.months` for "SharePartition", "share coordinator",
  "acquisition lock", "delivery count", "stateEpoch", "ShareSnapshot".

### Suggested Starting Points

- `core/src/main/java/kafka/server/share/SharePartition.java`
- `core/src/main/java/kafka/server/share/SharePartitionManager.java`
- `core/src/main/java/kafka/server/share/DelayedShareFetch.java`
- `server/src/main/java/org/apache/kafka/server/share/fetch/InFlightBatch.java`,
  `InFlightState.java`, `AcquisitionLockTimerTask.java`, `AcquisitionLockTimeoutHandler.java`
- `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorShard.java`
- `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorService.java`
- `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/PersisterStateBatchCombiner.java`
- Tests: `core/src/test/java/kafka/server/share/SharePartitionTest.java`,
  `SharePartitionManagerTest.java`,
  `share-coordinator/src/test/java/.../ShareCoordinatorShardTest.java`.
  `SharePartitionTest` already drives the state machine with a mock persister and mock time;
  it is the harness to reuse.

### Additional Exploration

- Share group deletion (`deleteState`) racing with a late `writeState`.
- Interaction between share partition start offset and the log's `logStartOffset` when
  retention deletes records that are still ACQUIRED.
