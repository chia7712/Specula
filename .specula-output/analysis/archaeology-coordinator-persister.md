# Bug archaeology: share coordinator, persister, SharePartitionManager, DelayedShareFetch, sessions, DLQ

Scope: commits since 2025-09-01 (Kafka HEAD as cloned) that touch
`share-coordinator/src/main`, `server-common/.../share/persister/`,
`core/.../share/SharePartitionManager.java`, `core/.../share/DelayedShareFetch.java`,
`server/.../share/session/`, `server/.../share/dlq/`.
Evidence: `git show <hash>` (squash-merge messages carry the PR description). No JIRA contents were
consulted or invented. Line numbers refer to the current checkout.

## 1. Counts

| Bucket | Count |
|---|---|
| Commits in slice (all are in `commit-list.txt`) | **95** |
| Correctness-relevant, fully read (`git show`) | **44** |
| of which bug fixes (a stated wrong behavior fixed) | **27** |
| Refactor / perf / metrics / logging / build / API rename (message + stat only) | 51 |
| group-coordinator share commits checked for initializeState/deleteState contract | 5 relevant (of ~70 hits) |

Not-correctness-relevant (skimmed): 342a8e677, 3c7f99ad3, 76a1c839c, 6b96273ba, 23b809a8a, 68800a027,
01cc865d4, af934aa24, e9ff2cca8, ed3af7212, 2db09f677, f83a871ce, cb9280ece, a2eae88a2, 849bf6c7a,
ec10a35c9 (runtime API: timeout now builder-level), 0166a0342, e90b9bd26, 114dbfcaa, 244e848e0,
a6b184772, 2f047bedb, b4d1ca780, b347f4bd2, a332a8456, b4c977544, ddec811aa (shared reaper thread),
10805c978, db3986549, c58ce9cac, 98ed67e73 (`this` escape in ctor), 5d86a6b82, 9c618eecc, f8705000e,
32e49cee1, 9ff2c5b1c, 69d4bfe9b, d069b7cd1, 0ccf386ca, 1189910a1, a94c28925, 500b73c09, 3eb0b8c1b
(metric leak), b58aae288 (adaptive linger), 1146f9777, 557182124, 6c0778aea (DLQ REJECT wiring, mostly
SharePartition), f2f31100c, a7f9e519f, e0d2d650e, 81447c182, eab19942a/b235414c8 (state RPC versions
for header v3; gated on MV).

## 2. Correctness-relevant commits

Severity scale: Critical safety (delivery-contract violation / durable state loss) / High / Medium / Low.

### 2.1 Share coordinator (ShareCoordinatorShard / Service / combiner / offsets manager)

| Hash | KAFKA | Summary | Root cause mechanism | Sev | Fix complete? |
|---|---|---|---|---|---|
| b77567478 | MINOR (#23074) | Honor higher start offset in writeState | Out-of-order `writeState` (same epochs) overwrote startOffset with a lower value -> redelivery after leader change. Now `newStartOffset = max(req, current)` and batches pruned at that value (ShareCoordinatorShard.java:670, 696, 711). | High | Yes for startOffset. Note `merge()` on replay (1034) still takes `newData.startOffset()` verbatim; safe only because every ShareUpdate is generated with the max already. |
| 2225a16c7 | MINOR (#23112) | Prevent deliveryCompleteCount regression | Same reordering: DCC copied verbatim. Now chosen by startOffset comparison (>: incoming, ==: max, <: current) (676-682). | Low (lag metric) | Mostly. With equal startOffset, `max` can over-count after an ambiguous write failure: SharePartition decrements its DCC on rollback (SharePartition.java:2532, 2571) but coordinator keeps the max. Replay `merge()` (1041) uses `newData.deliveryCompleteCount()` directly (fine, precomputed). |
| 1df8219b3 | MINOR (#23190) | Idempotent initializeState | Out-of-order/duplicate `initializeState` overwrote the stored state with a fresh snapshot (startOffset reset, batches wiped). Now no-op iff `stateEpoch == stored && startOffset == stored` (576-580). | High | **Partial.** Dedup key includes `startOffset`, which the share partition advances after init. A duplicate/stale init (same stateEpoch, startOffset -1 or 0) arriving after any write that moved startOffset is NOT detected and is accepted, because init fencing is `stored > req` (956), so equal epoch passes -> full state reset. Relies on GC never re-sending init after assignment. See 3.2. |
| cde0d5687 | KAFKA-20743 | Reject negative leader/state epochs | Sentinel `-1` bypassed fencing (`leaderEpoch != -1 && ...`) in write/read/init. Now INVALID_REQUEST for negatives (774-780, 832-836, 951-953) and fencing is unconditional (788-795, 846, 956). | Medium | Yes; reverses 6665a1950. |
| 6665a1950 | KAFKA-20694 | Read path: treat leaderEpoch -1 as "do not update" | Read fenced `-1` as stale, write did not. Superseded 10 days later by cde0d5687 (which rejects -1 everywhere). | Low | Superseded. Shows epoch-sentinel semantics were unstable through 2026-06. |
| 9dc859e95 | KAFKA-20710 | Fence stale periodic jobs (prune, cold snapshot) | `shouldRunPeriodicJob` boolean: disable/re-enable via metadata let old timer tasks and async completions keep rescheduling (duplicate job chains). Generation counter added (ShareCoordinatorService `shouldRunPeriodicJobForGeneration`). | Low | Yes. |
| 6eebb863a | MINOR (#21426) | Off-by-one in snapshotUpdateCount on replay | First ShareUpdate replay set count 0 -> limit+1 updates per snapshot (284). | Low | Yes. Residual: snapshot replay resets the count only when `>= limit` (264-268), so init/cold snapshots do not reset it (harmless, only affects compaction cadence). |
| eae10c2e6 | MINOR (#21291) | `snapshot.update.records.per.snapshot` range [0,500] | Validation moved into ConfigDef. | Low | Yes. |
| 3e9ae03b9 | KAFKA-20034 | DCC = 0 when initialized with non-negative startOffset | Altered offsets produced DCC -1. ShareGroupOffset.java:169, PartitionFactory. | Low | Yes. |
| ff5025a21 | KAFKA-19695 | Redundant-offset calc stuck after topic delete | Tombstoned key kept its last offset in `ShareCoordinatorOffsetsManager.offsets` forever -> prune never advanced. Now removed on tombstone. | Medium (unbounded __share_group_state growth) | Yes. Note the removal makes the tombstone itself prunable; consistent because all older records of the key are below it. |
| 46b4636d6 | KAFKA-20691 | Sweep-line rewrite of `PersisterStateBatchCombiner` | Perf (O(n^2) -> O((n+k)log p)). Semantics: per offset the winner is max by (deliveryCount, deliveryState id) (PersisterStateBatchCombiner.java:46-52). | Medium (risk: rewrite of the core merge) | Semantics preserved per tests. Modeling note: merge is a per-offset **max**, hence commutative/idempotent; this is what makes reordered/retried writes safe. Ordering of states: AVAILABLE 0 < ACQUIRED 1 < ACKNOWLEDGED 2 < ARCHIVING 3 < ARCHIVED 4 (RecordState.java). |
| 998ed9b2b | KAFKA-20690 | Combiner test hole + wrong expected output | Test-only correctness issue; one expectation was wrong. | Low | Yes. |
| 5a4dcdef7 | MINOR (#21363) | Topic-deletion cleanup moved into onMetadataUpdate | API move; `maybeCleanupShareState` tombstones all keys for deleted topic ids (632-647). | Low | n/a |
| b900f2fe5 | KAFKA-19797 | DCC in writeState / ShareUpdate / ShareSnapshot | New field; source of 2225a16c7 bug. | Low | n/a |
| 8f9338ba4 | KAFKA-20972 | initializeState takes a single key | Refactor; "Other RPCs will be updated separately" (1/N). | - | n/a |

### 2.2 Persister (PersisterStateManager / DefaultStatePersister)

| Hash | KAFKA | Summary | Root cause mechanism | Sev | Fix complete? |
|---|---|---|---|---|---|
| 5dd015386 | KAFKA-19927 | Retry NETWORK_EXCEPTION / REQUEST_TIMED_OUT client errors | Disconnect/timeout used to fail the RPC immediately -> SharePartition rollback on transient errors. Now retried with backoff for all 5 RPC handlers + find-coordinator. | High | Yes for transient errors, but **creates ambiguous-outcome retries**: a timed-out request may already be applied. Safe for write/read (max-merge, epoch fencing) but not for init/delete (see 3.2, 3.3). Also, after `maxRPCRetryAttempts`, SharePartition rolls back an op the coordinator may have applied (see 3.4). |
| b03c9966a | MINOR (#20884) | Handle auth / version-mismatch client responses | Missing branches. Mapped to UnknownServerException in SharePartition. | Low | Yes. |
| e75634f3e | KAFKA-20803 | Demultiplex batched write responses once | Perf: O(N^2) scan on sender thread. Introduced `PartitionResultIndex` (PersisterStateManager.java:200-230, 1646). | Medium | Index uses `byPartition.put` (225): if one coalesced request carries two entries for the same key, both handlers receive the **last** result (previously `findFirst`). Pre-existing duplicate-key issue, see 3.5. |
| eab19942a, b235414c8 | KAFKA-20925 | State RPC versions follow request header version | Version gating on MV. | Low | n/a |

### 2.3 SharePartitionManager / share sessions

| Hash | KAFKA | Summary | Root cause mechanism | Sev | Fix complete? |
|---|---|---|---|---|---|
| 70be719f6 | KAFKA-19950 | Mark session stale when removed before disconnect | Client re-sends INITIAL_EPOCH on a new connection; old connection's disconnect later removed the **new** session and fired `onMemberLeave` -> `releaseSession` released the live member's acquired records. Fix: `SessionKeyAndState.stale`. | High (records released under a live consumer -> concurrent redelivery) | **Partial**, see 3.6: the disconnect path still looks the session up by key, not by connection id, and the stale check is outside the lock. |
| c85a8c17a | MINOR (#23510) | Atomic remove+create on INITIAL_EPOCH | Two concurrent INITIAL_EPOCH requests double-incremented `numMembersPerGroup` -> `onGroupEmpty` never fires -> SharePartitions never evicted. Fix: `synchronized (cache)` in `newContext`. | Medium | **Partial**: `releaseSession` (SharePartitionManager.java:408-422) still does get-partitions then `cache.remove(key)` without the cache lock. |
| d193373f2 | KAFKA-21344 | Partition rotation for share fetches | Fairness: epoch-parity rotation + HashMap ordering starved a partition. | Medium (liveness/starvation) | Yes. |
| bd8ea36a1 | KAFKA-19846 | memberId string<->Uuid conversion | Member ids with `/` `+` failed decode. | Medium | Yes. |
| 214bb07d6 | KAFKA-19845 | RENEW ack type plumbing | Feature. | Low | n/a |
| 3eb0b8c1b | KAFKA-21096 | Deregister SharePartition metrics on teardown | Leak of fenced SharePartitions via gauges. | Low | Yes. |

### 2.4 DelayedShareFetch / purgatory

| Hash | KAFKA | Summary | Root cause mechanism | Sev | Fix complete? |
|---|---|---|---|---|---|
| f2b857133 | KAFKA-20783 (1/N) | Async log reader in DelayedShareFetch | Large rewrite; source of the 3/N..6/N lock fixes. | Medium | - |
| ac0d7e245 | KAFKA-20783 (3/N) | Completion handling | Minor completion fix. | Low | - |
| e9b8bde3b | KAFKA-20783 (4/N) | Clear held partitions when min-bytes fails | After releasing locks, `partitionsAcquired` stayed populated -> onComplete re-processed / re-released already released locks. | Medium | Yes for that path. |
| 2cf3c3ad9 | KAFKA-20783 (5/N) | Release locks on exception for local reads inside remote completion | Lock leak -> partition never fetchable again by other requests until... (fetch lock held forever). | High (liveness: partition stuck) | Yes, 2 call sites. |
| 45443a6a6 | KAFKA-20783 (6/N) | Release-exactly-once on all async paths | Several paths double-released or leaked fetch locks; double release **steals** a lock held by another request because `releaseFetchLock` force-clears on owner mismatch. | High | Fix done in DelayedShareFetch, but the force-clear in SharePartition.java:1569-1576 remains (see 3.7). |
| 8fc539cb3 | KAFKA-20783 | try/finally around exception handling | Lock leak if handler throws. | Medium | Yes. |
| 29254d681 | KAFKA-20425 | Skip purgatory triggers when no data | Only partitions that returned data trigger `completeDelayedShareFetchRequest`. | Low (latency) | Introduces lost-wakeup window (3.8). 45443a6a6 also changed all error paths to `releasePartitionLocks` without any trigger (DelayedShareFetch.java:282, 501, 692, 782, 812, 1287, 1303). |
| 71312a111 | MINOR (#23892) | Skip second log read when maxFetchRecords already reached | Wasted read; locks released on same path. | Low | Yes. |
| ea6afaa46 | MINOR (#23911) | Fetch offset metadata kept per DelayedShareFetch | Stale per-SharePartition LogOffsetMetadata cache outlived requests -> minBytes estimate wrong; released records only served after timeout. | Medium (liveness/latency) | Yes. |
| c205e6910 | KAFKA-19020 | Strict max fetch records (AcquireMode) | Introduces `maybeInitializeOffsetStateUpdate(targetOffset)`: offsets past target become AVAILABLE with `deliveryCount-1` (InFlightBatch.java:214). Only in-memory, never persisted at a lower count, so no conflict with the combiner's max rule. | Medium | n/a |

### 2.5 DLQ (server/.../share/dlq)

| Hash | KAFKA | Summary | Root cause mechanism | Sev | Fix complete? |
|---|---|---|---|---|---|
| 93d0db451 | KAFKA-20723 [2/N] | Merge records when coalescing DLQ produce | Two entries for the same (topic, partition) in one ProduceRequest: broker keeps only the last -> DLQ records silently lost. | High (DLQ loss) | Yes for DLQ. **Same duplicate-key pattern exists in the persister write path** (3.5). |
| ae9cf0f26 | KAFKA-20723 | Resolve copy records per produce round | Eager resolution capped content; one handler throwing in `onComplete` prevented siblings from completing; sync exception escaping into RequestCompletionHandler. | Medium | Yes (`completeHandlerSafely`). |
| 83947cc64 | KAFKA-20771 | Enforce max.message.bytes on DLQ produce | Oversized coalesced batch rejected -> all handlers failed. Chunking + defer. Also pins `CreateTime`. | Medium | Yes. |
| bd5fcec64 | MINOR (#22849) | DLQ timestamps used `hiResClockMs` | Near-epoch timestamps -> DLQ records deleted by next retention check. | High (DLQ loss) | Yes. |
| 805eb7b26 | KAFKA-20723 | Bound decompression memory | Unbounded allocation from compressed source batch. | Medium | Yes. |
| 28ec585cb | KAFKA-20723 [1/N] | Fail fast if DLQ manager not started | Enqueued requests never sent. | Low | Yes. |
| a589c272c | KAFKA-21356 | Move DLQ work off persister thread | DLQ record reads ran on the persister sender thread that also completes every write response (head-of-line blocking of writeState completions -> acquisition locks expire). | Medium | Yes. |
| 2d3518865, d12941eb5 | KAFKA-21197 / MINOR | Header preservation / validation order | Functional. | Low | n/a |

### 2.6 Group-coordinator commits changing the initializeState/deleteState contract (brief)

- be72089de KAFKA-20910: `alterShareGroupOffsets` now bumps group epoch before `initializeState`. Before, the init used the current epoch, so (init fencing being `>`) a lingering SharePartition's writes with the same stateEpoch were not fenced after an offset reset. 1df8219b3 explicitly relies on this.
- 97bbffa0a KAFKA-20742: new partitions of already-known topics initialize at startOffset 0 instead of -1 (avoid LATEST skipping records). Side effect: `alreadyKnownTopics` includes `initializingTopics` (GroupMetadataManager.java:3263-3266), so a *retry* of a brand-new topic's init (after `group.share.initialize.retry.interval.ms`) is sent with 0 instead of -1, ignoring `share.auto.offset.reset`, and it also defeats the 1df8219b3 dedup (startOffset differs). Semantic, not a safety bug.
- 56be35588 KAFKA-20442, 8dc39b570 KAFKA-19233, 6d16f687a KAFKA-19662: epochs / reset for unsubscribed topics; no change to the persister RPC contract.
- Assignment only uses `initializedTopics` (GroupMetadataManager.java:4473), so a SharePartition should never write before its init response was recorded by the GC.

## 3. Suspected incomplete fixes / unfixed sibling paths (current code)

### 3.1 Cross-incarnation epoch fencing reset by tombstone (High, candidate)
`handleShareSnapshot` tombstone removes `leaderEpochMap`, `stateEpochMap`, `snapshotUpdateCount` (ShareCoordinatorShard.java:251-255). After `deleteState` (group delete) and a re-init with a *lower* stateEpoch (a recreated group restarts its epoch near 1, cf. 5c8e3c58d), write fencing (788-795) and init fencing (956) only reject `stored > request`. A late/retried `writeState` from the previous incarnation (higher stateEpoch and leaderEpoch; persister retries with backoff, or a SharePartition still cached on a broker because `onGroupEmpty` is broker-local and driven by session disconnects, SharePartitionManager.java:938-949) is **accepted** into the new incarnation: its ACKNOWLEDGED/ARCHIVED batches win by the combiner max rule and its startOffset wins by `max` (670). Effect: records of the new group never delivered. The writes between delete and re-init get INVALID_REQUEST (785), which SharePartition maps to UnknownServerException, not a fence (SharePartition.java:2890-2905), so the stale SharePartition is not evicted by `fencedSharePartitionHandler`.

### 3.2 initializeState dedup depends on startOffset (High, partial fix of 1df8219b3)
ShareCoordinatorShard.java:576-580 + 956. Any init with equal stateEpoch but different startOffset overwrites (new snapshot, batches = [], leaderEpoch 0, ShareGroupOffset.java:163-180). A stale original init that arrives after its successful retry (5dd015386 retries REQUEST_TIMED_OUT/NETWORK_EXCEPTION; a timed-out request can still be applied) and after the first writes advanced startOffset resets the partition (redelivery of all acknowledged records; also leaderEpoch reset to 0 but leaderEpochMap keeps max, so fencing survives).

### 3.3 deleteState has no epoch at all (Medium-High, unfixed)
`DeleteShareGroupStateRequest` carries only group/topic/partition; `maybeGetDeleteStateError` (902-933) has no epoch check. Delete is retried by the persister on timeouts. Race: `deleteShareGroupOffsets` or `deleteGroups` -> deleteState in flight/retrying; the member re-subscribes, the topic moves deleting -> initializing (GroupMetadataManager.java ~3322) and init succeeds; the delayed delete then tombstones the new state. GC believes the partition is initialized, SharePartition reads get INVALID_REQUEST (READ_UNINITIALIZED, 840-844) -> partition stuck (liveness) or, if re-init happens later with startOffset -1, records skipped.

### 3.4 Ambiguous write outcome -> durable/in-memory divergence (Medium, by design but unmodeled)
CoordinatorRuntime write timeout -> REQUEST_TIMED_OUT -> mapped to COORDINATOR_NOT_AVAILABLE (CoordinatorOperationExceptionHelper) after the record was appended and replayed. Persister retries; on exhaustion SharePartition rolls back to ACQUIRED and decrements DCC (SharePartition.java:2560-2575), while the coordinator holds ACKNOWLEDGED. The consumer receives an ack error; old leader redelivers (count+1) while a new leader would see the record as ACKNOWLEDGED. Not a contract violation if ack errors are allowed to mean "maybe applied", but it is exactly Q3 and should be in the model (coordinator applies, response lost).

### 3.5 Duplicate key in one coalesced persister request (Medium, sibling of 93d0db451)
`addRequestToNodeMap` (PersisterStateManager.java:383-392) and `coalesceWrites` (1712-1744) never dedup by SharePartitionKey. SharePartition issues writes without per-partition serialization (ack, release, lock-timeout, DLQ phase 2 each call `writeShareGroupState`, SharePartition.java:2548, 2992, 3402), so two writes for one key can land in one request. Coordinator: `partitionFut.put(partitionData.partition(), future)` (ShareCoordinatorService.java:549, same pattern at 674/783/892/985) drops the first future; both ops are applied but the response has one entry. Broker: `PartitionResultIndex` `byPartition.put` (225) gives both handlers the last result. If the first op failed (e.g. coordinator moved between the two scheduled ops) the first caller sees success. Same issue for coalesced init/delete/read.

### 3.6 Session teardown by key instead of by connection (High, partial fix of 70be719f6)
ShareSessionCache.java:282-291 reads `stale()` after the synchronized removal (not under the lock, field not volatile), and `maybeRemoveAndNotifyListenersOnMemberLeave` (137-158) re-looks the session up **by key**. If a new INITIAL_EPOCH session for the same member is created on another connection between line 283 and line 140, the old connection's disconnect fires `onMemberLeave` -> `releaseSession` -> removes the new session and `releaseAcquiredRecords(memberId)` releases the records the live session just acquired (release is by memberId, SharePartitionManager.java:438). Fix pattern: compare `session.connectionId()` with the disconnecting connection. `releaseSession` itself (SharePartitionManager.java:408-422) is a non-atomic get-then-remove, the sibling c85a8c17a did not cover.

### 3.7 releaseFetchLock force-clears on owner mismatch (Medium, latent)
SharePartition.java:1569-1576: on mismatch it does `fetchLock.getAndSet(null)`, so any remaining double release in DelayedShareFetch (six fixes in KAFKA-20783 3/N..6/N show these are recurring) silently lets two DelayedShareFetch instances operate on one SharePartition concurrently. Safety then rests solely on `acquire` re-checking record state under the partition write lock.

### 3.8 Purgatory lost wakeups (Low, liveness/latency only)
29254d681 + 45443a6a6: lock release on no-data and on every error path no longer enqueues `completeDelayedShareFetchRequest` (DelayedShareFetch.java:1114-1125 and the bare `releasePartitionLocks` calls at 282, 501, 692, 782, 812, 1287, 1303). A request parked because it could not get the fetch lock, while records became acquirable by a release/timeout whose own trigger ran during that window, waits until maxWait. Bounded by maxWait.

## 4. Mechanism groups

1. **Reordered / duplicated persister RPCs at the coordinator** (b77567478, 2225a16c7, 1df8219b3, 5dd015386; open: 3.2, 3.3, 3.5). The coordinator's design answer is "merge is a max": startOffset max, combiner max on (deliveryCount, state), DCC heuristic. Init and delete are not max-mergeable and remain exposed.
2. **Epoch sentinels and fencing** (6665a1950 -> cde0d5687, be72089de; open: 3.1). Fencing is monotone only within one incarnation of the key.
3. **Snapshot / update compaction and pruning** (6eebb863a, eae10c2e6, ff5025a21, 9dc859e95, 46b4636d6, 998ed9b2b). No open safety issue found: pruning is keyed to snapshot offsets only, updates always follow their snapshot.
4. **Share session lifecycle vs acquired records** (70be719f6, c85a8c17a; open: 3.6).
5. **DelayedShareFetch fetch-lock accounting and purgatory triggers** (KAFKA-20783 x6, 29254d681, 71312a111, ea6afaa46; open: 3.7, 3.8).
6. **DLQ write path loss/robustness** (93d0db451, bd5fcec64, 83947cc64, ae9cf0f26, 805eb7b26, a589c272c).

## 5. TODO / FIXME
None in: share-coordinator/src/main/java/org/apache/kafka/coordinator/share/*.java,
server-common/.../share/persister/*.java, SharePartitionManager.java, DelayedShareFetch.java,
server/.../share/session/*.java, server/.../share/dlq/*.java (grep `TODO|FIXME|XXX`).
The only explicit unfinished-work marker is 8f9338ba4's "Other RPCs will be updated separately" (1/N).

## 6. Modeling implications
- Model the persister channel as lossy + reordering + duplicating with "applied but response lost" (5dd015386 makes duplicates normal).
- Model the coordinator write as: fence (`stored > req` only), then startOffset=max, batches=per-offset max(deliveryCount, stateId), DCC rule; init = overwrite unless (epoch, startOffset) equal; delete = unconditional tombstone that also clears epoch maps.
- Invariant candidates: (a) no accepted write from an older incarnation/epoch after an init or delete; (b) durable ACKNOWLEDGED/ARCHIVED offsets below the in-memory view never revert except via init; (c) a session release never releases records acquired by a different (newer) session of the same member.
