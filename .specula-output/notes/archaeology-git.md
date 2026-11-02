# Transaction coordinator: git bug archaeology

Repo: `kafka/` (HEAD as of 2026-11-02). Scope paths (directory filters, so deleted/renamed files are included:
`TransactionMetadata.scala`, `TransactionLog.scala`, `ProducerIdManager.scala`, `DelayedTxnMarker.scala`,
`TransactionMarkerChannel.scala`):

- `core/src/main/scala/kafka/coordinator/transaction/`
- `transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/`

Command: `git log --format='%h %ad %s' --date=short -- <both paths>`.

## Statistics

| Metric | Value |
|---|---|
| Commits touching scope paths (2017-04 .. 2026-10) | **236** |
| Bug-fix commits (after manual title + message scan) | **77** |
| of which Critical (EOS violation / hanging txn / divergent outcome) | 7 |
| of which High (stuck txn, unavailability, fatal client error) | 33 |
| of which Medium | 21 |
| of which Low (log/test/compat/cleanup) | 16 |
| Excluded as feature work although in-scope | KIP-98 initial impl (f69d94158c), KAFKA-5132 timeout abort (4951849163), KAFKA-5279 id expiration (20e2008785), KIP-890 epoch bump (ede0c94aaa, e14a81b4b2), KIP-939 2PC (3ef10a3f61, 3a3159b01e), Java migration moves |

Bug-fix rate over time (by commit date): 2017 (initial impl hardening): 23; 2018-2020: 20; 2021-2023: 8; 2024-2026: 26 (KIP-890/TV2 epoch semantics and failover persistence dominate the recent wave; 11 in 2026 alone).

### Hotspot: files touched by bug-fix commits (coordinator paths only)

| File | Bug-fix commits | All commits |
|---|---|---|
| TransactionStateManager.scala | 32 | 104 |
| TransactionCoordinator.scala | 31 | 88 |
| TransactionMetadata.scala (+ .java: 3) | 17 (+3) | 37 (+5) |
| TransactionMarkerChannelManager.scala | 14 | 64 |
| TransactionMarkerRequestCompletionHandler.scala | 12 | 26 |
| TransactionLog.scala (+ .java: 2) | 6 (+2) | 34 (+6) |
| DelayedTxnMarker.scala (deleted in KAFKA-9777) | 5 | 6 |
| ProducerIdManager.scala / RPCProducerIdManager.java | 4 / 4 | 23 / 5 |

Highest bug density relative to churn: TransactionMarkerRequestCompletionHandler (12/26) and TransactionMetadata (17+3 of 42).
Tests most often changed with fixes: TransactionStateManagerTest (28), TransactionCoordinatorTest (23),
TransactionMarkerChannelManagerTest (14), TransactionCoordinatorConcurrencyTest (10).

## Mechanism key

| Code | Mechanism |
|---|---|
| M1 | pendingState lifecycle: left set / wrongly reset / checked in wrong order after a failed or retried append |
| M2 | Stale coordinator epoch, load/unload races, cache ownership vs. replica leadership |
| M3 | Epoch bump + ongoing-transaction fencing semantics (InitProducerId, EndTxn epoch checks, timeout abort) |
| M4 | Epoch overflow / producerId rotation / epoch fields not persisted across failover |
| M5 | Marker delivery and completion (pending-marker map, retries, deleted partitions, error handling) |
| M6 | Expiration and timeout-abort races |
| M7 | Locking, deadlock and thread lifecycle |
| M8 | Transaction-log loading and serde robustness |
| M9 | ProducerId block allocation (ProducerIdManager) |
| M10 | Partition add / TV2 verification path |
| M11 | Misc (compat error codes, metrics, logs, tests) |

## Bug-fix commits

Component: EndTxn = EndTxn/two-phase append; Mk = markers; Load = load/unload/coordinator epoch; Init = InitProducerId/fencing; TV2 = TV2/verification; Exp = expiration/timeout; Pend = pendingState/append failure; Oth = other.

| sha | JIRA | date | summary | root cause | comp | sev | mech |
|---|---|---|---|---|---|---|---|
| b1093f64c0 | KAFKA-21349 | 2026-10 | Wait for HW before loading txn metadata | Load read to LEO incl. uncommitted, truncatable records; new leader could send COMMIT markers for a PREPARE_COMMIT the next leader lacks, which then times out and aborts: txn committed on some partitions and aborted on others | Load | Critical | M2 |
| 9dfd65db3d | KAFKA-21181 | 2026-10 | NOT_COORDINATOR from stale cache | Cache entry kept after replica became follower until onResignation; answers from stale cache never fenced, TV2 EndTxn got fatal INVALID_TXN_STATE | Load | High | M2 |
| 814cfa34eb | KAFKA-21181 | 2026-10 | Allow empty commit in TV2 | Coordinator with lagging metadata sees txn as Empty/Complete*; EndTxn(COMMIT) at current epoch returned fatal ITS; now writes empty PrepareCommit (unreplicable on stale leader, so retriable error) | EndTxn | High | M2 |
| b392ff0322 | KAFKA-20357 | 2026-09 | Persist lastProducerEpoch | Not written to txn log, so after failover a retried InitProducerId of a PID rotation got PRODUCER_FENCED | Init | Medium | M4 |
| c328f3d97b | KAFKA-20785 | 2026-08 | PRODUCER_FENCED when commit races timeout-abort | TV2 retry-tolerant epoch check accepted the pre-abort epoch, state check then returned fatal ITS (Streams thread dies) | EndTxn | High | M3 |
| b033b2e8f4 | KAFKA-20114 | 2026-05 | PID retry backoff race | Ordering in maybeRequestNextBlock vs. handleUnsuccessfulResponse | Oth | Low | M9 |
| eb111f6695 | KAFKA-20310 | 2026-03 | Persist previous/nextProducerId | valueToBytes never set tagged fields; failover lost nextProducerId so producer stuck at exhausted epoch, and retries after rotation fenced | Init | High | M4 |
| 783e501377 | KAFKA-20090 | 2026-03 | Rotate PID on exhausted epoch during completion | `!isEpochFence` guard skipped rotation on timeout-fence abort at MAX-1; txn could stay ONGOING | EndTxn | High | M4 |
| 27102b3187 | KAFKA-20090 | 2026-03 | Recover from epoch = Short.MaxValue | Repeated timeout fences could drive epoch to MAX; transitions threw; now log and allow markers at MAX | Init | High | M4 |
| a739b05e51 | KAFKA-20058 | 2026-02 | backoffDeadlineMs race | Async send reset backoff after failure handler set it | Oth | Low | M9 |
| 5bb44d63dd | KAFKA-20031 | 2026-01 | Detect overlapping PID block | Sanity check missed block starting at last PID of current block (duplicate PID) | Oth | Medium | M9 |
| c284a92472 | KAFKA-19990 | 2025-12 | Handle exceptions in AllocateProducerIds response | Auth/version exceptions not retried | Oth | Medium | M9 |
| 5bbc421a13 | MINOR | 2025-08 | readTxnRecordValue: non-empty partitions | Java migration built metadata with immutable `Set.of()` then added partitions | Load | Medium | M8 |
| ba7b5c9b32 | KAFKA-19367 | 2025-06 | Follow-up: no hasFailedEpochFence in TV2 | Flag blocked re-bump after failed fence append in TV2 | Init | Medium | M1 |
| 0b2e410d61 | KAFKA-19367 | 2025-06 | InitPID with TV2 double-increments epoch | Fence bump + TV2 abort bump; at 32766 overflowed to negative, IllegalArgumentException | Init | High | M4 |
| 9cb271f1e1 | KAFKA-18654 | 2025-02 | TV2 server-side retry of AddPartitions | CONCURRENT_TRANSACTIONS during marker propagation surfaced to producer | TV2 | Medium | M10 |
| bf05d2c914 | KAFKA-18672 | 2025-02 | Serde validates value version | Future record versions misinterpreted on downgrade | Load | Medium | M8 |
| ccab9eb8b4 | KAFKA-18660 | 2025-01 | TV2 epoch overflow handling | Wrong PID/epoch returned on overflow; pending check after PID check caused self-fence with new PID | EndTxn | High | M4 |
| 32dbbe6a1f | KAFKA-18464 | 2025-01 | Empty abort fences producer (TV2) | Epoch check ran before pending-state check; commit then empty abort self-fenced | EndTxn | High | M1 |
| dbae448a05 | KAFKA-18137 | 2024-12 | Unload removed loading partitions wrongly | Removed `(partition, newEpoch)` key which never exists; load completed after unload, stale state kept | Load | Medium | M2 |
| 08c876c1fe | (none) | 2024-11 | Empty batch during load | currOffset not advanced past empty last batch; infinite load loop | Load | High | M8 |
| 8107bda695 | KAFKA-17654 | 2024-10 | Flaky ProducerIdManagerTest | test | Oth | Low | M9 |
| 4529cc4e98 | MINOR | 2024-10 | transactionPartitionVerificationEnable return type | config accessor type | Oth | Low | M11 |
| 5e4c7dae22 | KAFKA-15915 | 2024-04 | Flaky ProducerIdManagerTest | test injection timing | Oth | Low | M9 |
| 1d6e0b8727 | KAFKA-16352 | 2024-03 | Txn stuck in PrepareCommit/Abort | transactionsWithPendingMarkers removal by key only, so an old-epoch operation deleted the new entry | Mk | High | M5 |
| e4249b69bd | KAFKA-15784 | 2023-12 | Atomic verify + TxnOffsetCommit | Verification and in-memory offset update not atomic | TV2 | Medium | M10 |
| 1d8b07ed64 | KAFKA-15129 | 2023-07 | Remove marker-channel metrics on shutdown | metrics leak | Mk | Low | M11 |
| 1dbcb7da9e | KAFKA-14694 | 2023-06 | RPCProducerIdManager must not block | Handler threads blocked under global lock waiting on controller | Init | High | M9 |
| ef09a2e3fc | KAFKA-14904 | 2023-04 | Pending state blocked verification | Verify returned CONCURRENT_TRANSACTIONS while Ongoing pending; retry produced OutOfOrderSequence; also restores missing lock | TV2 | Medium | M10 |
| 290eeed7ba | KAFKA-14880 | 2023-04 | Epoch -1 metadata expirable | Failed first append left epoch -1; prepareDead threw, killing expiration task | Exp | Medium | M6 |
| 22bb3e7a58 | KAFKA-14417 | 2022-12 | InitProducerId error code compat | REQUEST_TIMED_OUT not retriable for old clients | Init | Medium | M9 |
| c5ec390fa6 | KAFKA-13099 | 2021-07 | Expiration respects max batch size | Unbounded tombstone batch hit MESSAGE_TOO_LARGE; ids never expired | Exp | Medium | M6 |
| 2eeae09ca3 | KAFKA-9666 | 2020-07 | No epoch increase on failed fence append | Failed fence append still bumped in-memory epoch, exhausting epochs under min-ISR loss | Init | High | M1 |
| 0ffbc6e75f | KAFKA-10080 | 2020-06 | Duplicate completion appends | maybeWriteTxnCompletion not safe concurrently; completion written twice, could land after next txn began | Mk | Critical | M5 |
| 75e8ee1d1a | KAFKA-9777 | 2020-03 | Remove txn marker purgatory | DelayedOperation tryLock could skip completion; txn stuck | Mk | High | M5 |
| 88bc0fdf82 | KAFKA-9600 | 2020-03 | Strict epoch on client EndTxn | Client EndTxn with higher epoch accepted | EndTxn | High | M3 |
| 4c96b7deba | KAFKA-9749 | 2020-03 | KAFKA_STORAGE_ERROR retriable for markers | ISE on that error left PendingCommit/Abort forever | Mk | High | M5 |
| 6a88d32b9f | KAFKA-8803 | 2020-03 | Remove timestamp check (again) | Clock going back made Ongoing transition throw; txn never expires (hanging) | Exp | High | M6 |
| fbe2e604cc | KAFKA-9235 | 2020-01 | Stop coordinator after replica deletion | Leader removed by reassignment kept cache (StopReplica path) | Load | High | M2 |
| dbc9cd638f | KAFKA-9144 | 2020-01 | Track marker timestamps for producer expiry | Producer state expired early, coordinator epoch went backwards, recovery failed | Load | High | M2 |
| 618226c544 | KAFKA-9065 | 2020-01 | Endless loop while loading | Gap between last record and LEO (compaction) | Load | High | M8 |
| 0f0eb49e35 | KAFKA-9307 | 2019-12 | Loading resilient to previous errors | Failed unload left metadata; reload refused | Load | High | M2 |
| a094504267 | KAFKA-9293 | 2020-01 | NPE in DumpLogSegments parser | tombstone handling | Oth | Low | M8 |
| bd3bde7623 | MINOR | 2020-01 | TransactionLogTest toString | test | Oth | Low | M11 |
| fecb977b25 | KAFKA-8710 | 2019-11 | KIP-360 epoch bump for txn producers | Producers unrecoverable after UNKNOWN_PRODUCER_ID | Init | Medium | M3 |
| 26a001d133 | MINOR | 2019-04 | Load log message | wrong message | Load | Low | M11 |
| ed94cf4d31 | KAFKA-7519 | 2018-10 | Clear pending state when expiration fails | Pending Dead never cleared; txnId untouchable (CONCURRENT_TRANSACTIONS forever) | Exp | High | M1 |
| 4a5cba87bc | KAFKA-7286 | 2018-09 | Large records stall loading | Fixed load buffer smaller than a record | Load | High | M8 |
| 3afe2ed8e3 | MINOR | 2018-06 | Load buffer read | used return value of readInto incorrectly | Load | Medium | M8 |
| 3f611044cc | KAFKA-6052 | 2018-03 | WriteTxnMarkers retry in InterBrokerSendThread | Markers never re-sent (Windows) | Mk | High | M5 |
| 3c9e30a2f7 | MINOR | 2017-10 | Tighten locking for expired abort | check-then-act on producerId outside lock | Exp | Medium | M6 |
| 501a5e2627 | KAFKA-6119 | 2017-10 | Bump epoch when timing out txns | Timeout abort without epoch bump: zombie producer could keep writing/commit | Exp | Critical | M3 |
| 69e8463c06 | KAFKA-6131 | 2017-10 | Atomic putIfAbsent for marker queues | getOrElseUpdate race lost markers | Mk | High | M7 |
| efefb452df | KAFKA-6042 | 2017-10 | Deadlock with delayed operations | DelayedTxnMarker lock ordering | Mk | High | M7 |
| f8621b4174 | KAFKA-5970 | 2017-10 | ReentrantLock for delayed op | blocking on metadata monitor | Oth | High | M7 |
| aea53108a7 | KAFKA-5428 | 2017-06 | Abort batches only in fatal state | (client fix; coordinator log only) | Oth | Low | M11 |
| 9d6f0f40ce | KAFKA-5416 | 2017-06 | Don't reset pending state on retried append | Reset pending, then successful retry threw ISE in completeTransitionTo; stuck | Pend | High | M1 |
| 1e03b71187 | KAFKA-5415 | 2017-06 | Remove timestamp check | NTP regression broke transitions | Exp | High | M6 |
| 8e3ed7028e | KAFKA-5357 | 2017-06 | Unexpected exceptions in marker thread | recursion/exception killed sender thread | Mk | High | M5 |
| 1c882ee5fb | KAFKA-5283 | 2017-06 | Epoch/sequence overflow | No new PID on epoch overflow | Init | Medium | M4 |
| 049abe7efa | KAFKA-5351 | 2017-05 | Reset pending on append error | Pending kept; CONCURRENT_TRANSACTIONS forever | Pend | High | M1 |
| 2cc8f48ae5 | KAFKA-5308 | 2017-05 | Handle UNSUPPORTED_FOR_MESSAGE_FORMAT markers | unhandled error stuck txn | Mk | Medium | M5 |
| 80223b14ee | KAFKA-5202 | 2017-05 | Topic deletion while sending markers | markers retried forever to deleted partition | Mk | High | M5 |
| 34e379f107 | KAFKA-5171 | 2017-05 | Reject empty transactional id | validation | Init | Low | M11 |
| d662b09c9f | KAFKA-5268 | 2017-05 | Clear partitions before Complete; check Ongoing in AddPartitions | Partitions kept after Complete made AddPartitions of next txn a no-op: partitions never got markers | TV2 | Critical | M10 |
| c64cfd2e2b | KAFKA-5231 | 2017-05 | Bump epoch when aborting on InitPid | Abort markers at old epoch; zombie not fenced | Init | Critical | M3 |
| 794e6dbd14 | KAFKA-5130 | 2017-05 | Cache refactor + synchronization fixes | In-memory updated before append; no coordinator-epoch recheck | Load | Critical | M2 |
| 1bf6483316 | KAFKA-5280 | 2017-05 | Read-write lock on metadata map | check-then-act on ownership; append with stale coordinator epoch after emigration/immigration | Load | Critical | M2 |
| 24a4e6146a | HOTFIX | 2017-05 | Txn log compression codec | creation failure | Load | Low | M8 |
| 3085d4f435 | KAFKA-5182 | 2017-05 | Close coordinator threads on shutdown | thread leak / hang on shutdown | Oth | Low | M7 |
| 1f2451d4e7 | KAFKA-5059 | 2017-05 | Remove broken locking; fix AddPartitions after complete | broken lock usage, InitPid recursion | EndTxn | Medium | M7 |
| 7892b4e6c7 | KAFKA-5128 | 2017-05 | Check IBP in transactional APIs | compat | Oth | Low | M11 |
| 3cf8745243 | MINOR | 2024-12 | Persist clientTransactionVersion across transitions | TV lost on transitions (TV2 flag dropped) | TV2 | Medium | M4 |
| 5e9d4de748 | KAFKA-14869 | 2023-04 | Ignore unknown record types | downgrade load failure | Load | Medium | M8 |
| 87bef1d1b0 | KAFKA-9788 | 2020-06 | Distinct load sensor names | metrics conflated | Oth | Low | M11 |
| 9c8f75c4b6 | KAFKA-10223 | 2020-07 | NOT_LEADER_OR_FOLLOWER | error code compat | Oth | Low | M11 |
| 619fd7aeb6 | KAFKA-5131 | 2017-05 | Complete PrepareX on partition immigration | New coordinator did not finish in-flight commit/abort | Load | High | M2 |


## Mechanism groups and current-code status

Line numbers refer to HEAD: `TC` = `core/.../TransactionCoordinator.scala`, `TSM` = `core/.../TransactionStateManager.scala`,
`TMCM` = `core/.../TransactionMarkerChannelManager.scala`, `TMRCH` = `core/.../TransactionMarkerRequestCompletionHandler.scala`,
`TM` = `transaction-coordinator/.../TransactionMetadata.java`.

### M2: Stale coordinator epoch / load-unload / leadership (10 primary, 3 Critical)
619fd7aeb6, 794e6dbd14, 1bf6483316, 0f0eb49e35, fbe2e604cc, dbc9cd638f, dbae448a05, 9dfd65db3d, 814cfa34eb, b1093f64c0.
- Pattern: the in-memory cache and the replica's leadership diverge. Bugs keep appearing, the most recent (b1093f64c0, 2026-10) a Critical split commit/abort.
- Current guards: coordinator-epoch recheck after append (TSM:787-822); read lock held across local append (TSM:880-910); `isCoordinatorFor` (TSM:403-405, used at TSM:422); HW wait before load (TSM:625-695); epoch-keyed loading removal (TSM:731-737).
- **Still live (partly).** The safety argument is now spread across three mechanisms (stateLock, cache coordinatorEpoch, replica leader epoch, and HW-wait). Open edges:
  - The HW-wait abandon path (TSM:620-624, 639-641, 691-695) removes `loadingPartitions` but keeps the old-epoch cache entry. If the broker stays leader (listener could not be added, `onFailed`), the partition answers NOT_COORDINATOR until the next leader change. This is an availability hole.
  - Empty commit on a stale coordinator (TC:859-863, 881-905) is safe only if a stale leader cannot get a replicated write. That relies on unclean election being disabled (TC:1046) and on KRaft ISR invariants. Model it with `min.insync.replicas=1` and ELR (KIP-966).

### M1: pendingState lifecycle after failed/retried append (6 primary)
049abe7efa, 9d6f0f40ce, ed94cf4d31, 2eeae09ca3, 32dbbe6a1f, ba7b5c9b32 (+ 290eeed7ba and 4c96b7deba are related side effects).
- Current: pending reset only when epoch unchanged and not retried (TSM:829-849). The success path with `getTransactionState` = Left (TSM:788-791) and the failure path with Left/None (TSM:853-861) leave pendingState as is. That is fine only if an unload or reload follows.
- **Still live:** `PREPARE_EPOCH_FENCE` is set under one lock (TC:288, TC:1097) and consumed under a later lock in `endTransaction` (TC:804, TV1 TC:569-582). Any request that runs in between is treated as the fence:
  - A client `EndTxn(ABORT)` takes over the fence. In TV1 the bump is lost: `setProducerEpoch(producerEpoch)` uses the client's epoch.
  - A client `EndTxn(COMMIT)` calls `prepareAbortOrCommit` while the fence is still pending. `prepareTransitionTo` then throws an IllegalStateException (TM:318-320), and the client gets UNKNOWN_SERVER_ERROR.
  - If `getTransactionState` returns Left in that window (NOT_COORDINATOR via `isCoordinatorFor`, or LOAD_IN_PROGRESS), the fence stays pending on the stale entry.
- Not a demonstrated EOS violation, but a good model-checking target.

### M3: Epoch bump + ongoing-txn fencing semantics (5 primary, 2 Critical)
c64cfd2e2b, 501a5e2627, 88bc0fdf82, fecb977b25, c328f3d97b (also ba7b5c9b32, 0b2e410d61 overlap).
- Current: TV2 epoch validity table (TC:806-831); KAFKA-20785 special case (TC:893-905).
- **Live:** TV2 `isRetry` (epoch+1) is ambiguous between a client retry and a coordinator-initiated abort. KAFKA-20785 patched only the COMMIT-in-COMPLETE_ABORT cell. `COMPLETE_COMMIT + ABORT + isRetry` still returns ITS (TC:884-886). The `retryOnOverflow` path is computed from `prevProducerId` only (TC:806-807). Enumerate all state/op/epoch cells in the model.

### M4: Epoch overflow / PID rotation / persisted fields (8 primary)
1c882ee5fb, ccab9eb8b4, 0b2e410d61, 27102b3187, 783e501377, eb111f6695, b392ff0322, 3cf8745243.
- Five fixes in 2025-2026 alone. Current: rotation on any exhausted completion (TC:833-857); recovery when epoch equals MAX (TM:141-147, TM:242-246, "must not happen unless there is a bug").
- **Live:** this is the most active recent area. Failover persistence was fixed field by field: next/prev PID in 2026-03, then lastProducerEpoch in 2026-09. Check the remaining fields that `completeTransitionTo` validates against (TM:349-445), such as `nextProducerId` while in PREPARE_* and `clientTransactionVersion`, round-trip through the log in `TransactionLog.valueToBytes`/`readTxnRecord`.

### M5: Marker delivery and completion (8 primary, 1 Critical)
80223b14ee, 2cc8f48ae5, 8e3ed7028e, 3f611044cc, 4c96b7deba, 75e8ee1d1a, 0ffbc6e75f, 1d6e0b8727.
- Current: compare-and-remove on `transactionsWithPendingMarkers` (TMCM:299-305, 380, 385, 434); deleted-partition skip (TMCM:375-398).
- **Live (assumption-dependent):**
  - TMRCH:160-167 treats `INVALID_PRODUCER_EPOCH` or `TRANSACTION_COORDINATOR_FENCED` as "this txn can now be ignored". It drops the pending-marker entry while the txn stays in PrepareX with pending CompleteX. If the same coordinator epoch still owns the partition, nothing finishes it, the same "stuck" outcome as KAFKA-9749/16352. TV2 makes the leader check strict (`markerEpoch <= current` rejected, `ProducerAppendInfo.java:124-144`, idempotent-retry exemption only when no txn is open).
  - The deleted-partition skip (TMCM:388-396) matches partitions by topic name, not topic ID. After a delete and re-create of the topic, markers go to the new incarnation.

### M6: Expiration / timeout-abort races (5 primary; +ed94cf4d31, 501a5e2627 secondary)
1e03b71187, 6a88d32b9f, 3c9e30a2f7, c5ec390fa6, 290eeed7ba (+ ed94cf4d31, 501a5e2627).
- **Live 1:** `abortTimedOutTransactions` (TC:1077-1115) works from a snapshot taken by `timedOutTransactions()` (TSM:129-149) and re-checks only `producerId` and pending under the lock (TC:1086-1098). It does not re-check state, epoch or timeout.
  - If the txn moved to PREPARE_* (pending empty between `completeTransitionTo` in TSM:819 and `prepareComplete` in TC:951-975), `prepareFenceProducerEpoch` throws an IllegalStateException. Valid previous states are ONGOING only (TransactionState.java:109). The exception aborts the rest of that round.
  - If a new TV2 txn started with the same PID (epoch+1), a fresh, non-timed-out txn is fenced and aborted.
- **Live 2:** the expiration tombstone callback (TSM:264-286) looks up the txnId in the current cache entry with no null check.
  - After a reload at a new epoch that replayed the tombstone, `get` returns null and `txnMetadata.inLock` throws a NullPointerException.
  - If the id was reloaded, the else-branch (TSM:284) clears the pendingState of a fresh object that another in-flight transition owns, which is the KAFKA-5416 pattern.
  - b1093f64c0 narrowed this (expiration is skipped while loading, TSM:251) but did not close it: the callback can fire after the load completes.

### M7: Locking / deadlock / threads (5 primary)
1f2451d4e7, 3085d4f435, f8621b4174, efefb452df, 69e8463c06 (+ 75e8ee1d1a, 1bf6483316). Purgatory is gone; lock order is stateLock then txnMetadata lock. The new HW listener runs with partition locks held and defers to the scheduler (TSM:618-624). Looks closed.

### M8: Log loading and serde (9 primary)
24a4e6146a, 3afe2ed8e3, 4a5cba87bc, 618226c544, 08c876c1fe, a094504267, 5e9d4de748, bf05d2c914, 5bbc421a13. These are recurring infinite-loop and buffer bugs in the load loop (TSM:458-535). They are implementation robustness issues, not protocol issues, and low priority for TLA+.

### M9: ProducerId block allocation (8 primary)
1dbcb7da9e, 22bb3e7a58, 5e4c7dae22, 8107bda695, c284a92472, 5bb44d63dd, a739b05e51, b033b2e8f4. Three concurrency races in RPCProducerIdManager in 2026 alone. Duplicate PIDs (5bb44d63dd) would be Critical if a controller bug produced them; out of scope for the coordinator state model.

### M10: Partition add / TV2 verification (4 primary, 1 Critical)
d662b09c9f, ef09a2e3fc, e4249b69bd, 9cb271f1e1. The coordinator side now permits verification during pending Ongoing (TC:362-410). The `AddPartitions` existence check is on the coordinator (`KafkaApis.scala:1965`).

### M11: Misc (9 primary)
Error-code compatibility, metrics, logs and tests.

## TODO / FIXME / XXX in current coordinator code

`grep -rniE 'todo|fixme|xxx|hack'` over both coordinator source directories finds **none**. The only hit is a false positive: TSM:571 contains "false" inside a call.
Adjacent code on the coordinator protocol path:
- `core/src/main/scala/kafka/server/KafkaApis.scala:1794`: TODO, WriteTxnMarkers appends one record per producerId instead of a single control-record append. Partial marker application within one request is possible.
- `core/src/main/scala/kafka/server/ReplicaManager.scala:1132`: TODO about topic IDs (unrelated; alterReplicaLogDirs).
Self-described "should not happen" spots instead of TODOs:
- TM:141-142 and TM:242-245 log an error for epoch == Short.MaxValue ("must not happen unless there is a bug").
- TC:599-603 and TC:906-910 call `fatal` + IllegalStateException for DEAD/PREPARE_EPOCH_FENCE.
- TMCM:250-251 and TMCM:400-403 throw on unexpected getTransactionState results.

## Candidate open weaknesses (for modeling priority)

1. **Fence hijack window (M1/M3).** `PREPARE_EPOCH_FENCE` is set at TC:288 and TC:1097 and consumed later at TC:804 and TC:569. A concurrent client EndTxn is processed as the fence. COMMIT throws an IllegalStateException (TM:318); ABORT takes over the fence, and in TV1 loses the bump.
2. **Timeout abort on a stale snapshot (M6).** TC:1086-1098 re-checks only producerId. It can fence a fresh TV2 txn, or throw an IllegalStateException in the PREPARE_* gap and skip the remaining expirations in that round.
3. **Expiration callback after reload (M6/M2).** TSM:270-284: no null check (NullPointerException), and it can clear a foreign pendingState.
4. **Marker error = ignore (M5).** TMRCH:160-167 drops pending markers while the txn stays PrepareX/pending CompleteX if the coordinator epoch did not change. This is a hanging-txn risk if TV2 strict marker epochs ever reject a legitimate marker.
5. **Stale-leader empty commit (M2).** TC:859-905 rests on ISR/unclean invariants. Worth one TLA+ scenario with ELR or min ISR 1.
6. **HW-wait abandon keeps old-epoch cache (M2).** TSM:620-695: the coordinator can be unavailable while still leader.
7. **Failover persistence (M4).** Fixed field by field. Audit the full TxnTransitMetadata to TransactionLogValue round-trip.
