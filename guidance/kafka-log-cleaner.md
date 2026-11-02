# Apache Kafka: Log Cleaner (Compaction) Modeling Guidance

Target source: `./kafka` (Apache Kafka trunk, module `storage`, package
`org.apache.kafka.storage.internals.log`). Treat the checked-out implementation as ground truth.
This target is about correctness only. Memory usage, throughput and I/O efficiency are out of scope.

## Required

### Goal

- Find bugs where log compaction loses the latest value of a key, resurrects a deleted key,
  leaves the log in a state that cannot be recovered after a crash, or where the cleaner
  state machine deadlocks or cleans a partition it must not touch.

### Scope

- In scope: `LogCleaner`, `Cleaner` (the per-thread clean loop), `LogCleanerManager`
  (the in-progress / paused / aborted state machine and checkpoints), the segment replacement
  path in `UnifiedLog.replaceSegments` and `LocalLog.replaceSegments` (`.cleaned` → `.swap` →
  rename → `.deleted`), log recovery of those files on startup (`LogLoader`),
  `CleanedTransactionMetadata` (abort marker retention), and the interaction of cleaning with
  concurrent append, truncation, segment deletion by retention, and partition deletion or
  reassignment.
- Boundaries/exclusions: the compression and record format layer, remote storage (tiered
  storage), the producer state snapshot format, leader election, and replication. Read them
  only to establish what the cleaner may assume. Do not model performance or memory.

### Priority Questions

1. When the broker crashes at any point between writing a `.cleaned` segment and deleting
   the replaced segments, can recovery end up with a log that has a key's latest value missing,
   an older value visible after a newer one, or duplicate offsets?
   Expected behavior: after recovery, the visible log is equivalent to either the pre-clean
   or the post-clean log, never a mix.
2. When a partition is truncated, deleted, or has segments removed by retention while a
   cleaning of it is in progress, can the cleaner write a `.swap` or `.cleaned` segment into a
   directory that no longer exists, replace segments that were already deleted, or move the
   checkpoint past data that was never cleaned?
   Expected behavior: cleaning is aborted or waits, and the checkpoint never claims an offset
   range that was not cleaned.
3. When a tombstone (null value) or a transaction abort marker is removed by compaction, can
   a consumer that was behind still observe the key as present, or observe records of an
   aborted transaction as committed?
   Expected behavior: tombstones and abort markers are retained for at least
   `delete.retention.ms` after the clean that first could have removed the data they cover.
4. Can `LogCleanerManager` leave a partition permanently in a state where it is never cleaned
   again (for example paused with a count that is never decremented, or in-progress for a
   thread that died), or clean the same partition on two threads at once?
   Expected behavior: every partition eligible for cleaning is eventually cleaned, and at most
   one thread cleans a partition at a time.
5. When the cleaner checkpoint file is written non-atomically or the checkpoint is ahead of
   the actual log end offset after a truncation, can the next clean skip dirty data forever?
   Expected behavior: the first dirty offset never exceeds the log end offset and is reset on
   truncation below it.

### Must-cover Interactions

- `Cleaner.doClean` → `UnifiedLog.replaceSegments` → `LocalLog.replaceSegments`: the file
  rename sequence and which renames are the atomic commit point.
- `LogLoader` startup ↔ leftover `.cleaned`, `.swap`, `.deleted` files: which are discarded
  and which are completed, and whether a `.swap` without its original segments is handled.
- `LogCleanerManager.grabFilthiestCompactedLog` / `doneCleaning` / `abortCleaning` /
  `pauseCleaningForNonCompactedPartitions` ↔ `UnifiedLog.truncateTo`,
  `UnifiedLog.deleteOldSegments`, partition deletion in `LogManager`.
- `Cleaner.buildOffsetMap` and the dirty range computation ↔ `UnifiedLog.logEndOffset`,
  `firstUnstableOffset` (transactions) and `activeSegment`: the active segment is never cleaned.
- `CleanedTransactionMetadata` ↔ abort markers and `delete.retention.ms`.
- Consider: a crash after each individual rename, truncation during `buildOffsetMap`,
  two cleaner threads, and a partition reassigned away and back during cleaning.

### Assumptions

- The file system renames a single file atomically and preserves ordering of fsync'd writes.
  Directory-level fsync behavior should be read from the code, not assumed.
- Only one `LogCleaner` instance per broker; multiple cleaner threads share one
  `LogCleanerManager`.

## Optional

### Known Incidents and References

- Historical cleaner bugs are a good seed for archaeology: search `git log` for
  "LogCleaner", "compaction", ".swap", ".cleaned", "firstDirtyOffset", "LogCleanerManager".
- KIP-98 (transactions) for abort marker semantics in compacted topics.
- KIP-405 (tiered storage) only to confirm which paths are excluded.

### Suggested Starting Points

- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaner.java`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/Cleaner.java`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleanerManager.java`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogCleaningState.java`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java`
  (`replaceSegments`, `truncateTo`, `deleteOldSegments`)
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LocalLog.java`
  (`replaceSegments`, `deleteSegmentFiles`)
- `storage/src/main/java/org/apache/kafka/storage/internals/log/LogLoader.java`
- `storage/src/main/java/org/apache/kafka/storage/internals/log/CleanedTransactionMetadata.java`
- Tests with harness ideas: `storage/src/test/java/.../LogCleanerTest.java`,
  `LogCleanerManagerTest.java`, `LogLoaderTest.java`.

### Additional Exploration

- Compaction of the `__consumer_offsets` and `__transaction_state` internal topics, where a
  lost or resurrected key becomes a coordinator bug.
- Interaction with `ProducerStateManager` snapshots when cleaning removes the last record of
  a producer id.
