# Bug archaeology: SharePartition / server.share.fetch / SharePartitionCache

Scope: every commit since 2025-09-01 touching
`core/src/main/java/kafka/server/share/SharePartition.java`,
`server/src/main/java/org/apache/kafka/server/share/fetch/`, or
`core/src/main/java/kafka/server/share/SharePartitionCache.java`.

Evidence: `git show <hash>` (commit message + diff) only. JIRA and GitHub were not reachable, so
nothing below quotes JIRA text. Line numbers are for the current checkout (HEAD = `eab19942a`).
`SP` = `core/src/main/java/kafka/server/share/SharePartition.java`,
`IFS` = `server/.../share/fetch/InFlightState.java`, `IFB` = `server/.../share/fetch/InFlightBatch.java`,
`RS` = `server/.../share/fetch/RecordState.java`.

## 1. Counts

| Bucket | Count |
|---|---|
| Commits touching the slice paths (`git log --since=2025-09-01 -- <paths>`) | **48** (all 48 are in `commit-list.txt`) |
| ... of which shallow-clone graft root (`342a8e677`, 6914 files, not a real change to these files) | 1 |
| Bug fixes with a correctness impact on the state machine or the persister path (full diff read) | 15 |
| Feature or behavior changes that are correctness-relevant (full diff of slice files read) | 14 |
| Refactor, logging, build, tests, config plumbing (message read, diff skimmed) | 18 |
| TODO / FIXME / XXX / HACK in `SP`, `fetch/`, `SharePartitionCache.java` | **0** |

## 2. Correctness-relevant commits

Severity: **Crit** = safety (double delivery, lost or acked-then-redelivered records), **High** =
records stuck forever, durable/in-memory divergence, or deadlock, **Med** = bounded liveness or
counter issue, **Low** = defensive or cosmetic.

### 2a. Bug fixes

| Hash | KAFKA | Summary | Root cause (mechanism) | Component | Sev | Fix complete? |
|---|---|---|---|---|---|---|
| 18dbd5227 | KAFKA-19954 | Overlapping batches in cache after re-initialization | After re-init, start offset 5 is inside log batch 0-99. Acquire adjusts `baseOffset` up to `startOffset`, then searches `subMap(baseOffset..)`. Once start offset moves to 10, the cached key 5 falls below the search window, so a new entry 0-99 (later 10-99) is created that overlaps 5-99. The same offset is then held by two InFlightBatches, so it can be acquired twice. Fix: `subMap(min(firstBatch.baseOffset, baseOffset), ..)` and acquire the new batch from the adjusted `baseOffset`. | acquire / start offset | **Crit** | **Partial.** The overlap path still passes the unadjusted `firstBatch.baseOffset()` to `checkForFullMatch` (SP:907), `shouldThrottleRecordsDelivery` and `acquireSubsetBatchRecords` (SP:943). It is only harmless while no AVAILABLE offset sits below `startOffset`, and the rollback path in M5 can create one. |
| 55764f837 | KAFKA-19859 | Start offset moved backwards on ack after LSO movement | `maybeUpdateCachedStateAndOffsets` set `startOffset = higherKey(lastAcked)` or `lastAcked+1` without checking that this is greater than the current `startOffset`. `updateCacheAndOffsets` (LSO move) can leave cached batches below `startOffset`. Fix: guard both branches with `> startOffset`. | start offset | High | **Incomplete.** (1) The first branch, `lastOffsetAcknowledged == lastCachedOffset`, still sets `startOffset = endOffset = lastCachedOffset + 1` with no guard (SP:2647). The trigger is a cache that sits entirely below a new LSO, e.g. batch 0-4 ACQUIRED, LSO moves to 10, then the lock times out: 0-4 become ARCHIVED and start offset regresses from 10 to 5. `nextFetchOffset` explicitly handles `startOffset > cachedState.last.lastOffset` (SP:619), so this state is reachable. (2) The gap-window branch, `startOffset = Math.min(gapWindow.gapStartOffset, startOffset)` (SP:2683), can also regress, because `updateCacheAndOffsets` never moves or clears `persisterReadResultGapWindow` (SP:1247-1290). |
| 25aae9903 | KAFKA-19808 | Batch misalignment when partition is at max in-flight capacity | When the partition is at capacity and a fetch lands inside the cached range, `lastOffsetToAcquire` was forced to `endOffset` even though the (dynamic-bytes) fetch returned fewer records. Acquire then built batches that did not match the cache. Symptoms: "Base offset not found" in the lock-timeout handler (the timer returns early and leaves records ACQUIRED, i.e. stuck) and gaps reported on topics that cannot have gaps. Fix: `min(lastOffset, endOffset)`, and use `lastOffsetToAcquire` for the subMap and full-match checks. | acquire / lock timeout | High | Looks complete for acquire. The timeout handler still just logs and returns when `floorEntry(firstOffset)==null` (SP:2962-2965), so any future misalignment again strands the ACQUIRED records with no timer. |
| dfa28c463 | KAFKA-19953 | Release of per-offset batch stops at first offset owned by another member | `releaseAcquiredRecordsForPerOffsetBatch` did `return Optional.empty()` on a member mismatch instead of `continue`. The member's later offsets stayed ACQUIRED until lock timeout. | session release | Med | Complete for this loop. Related gap: one offset with an ongoing transition makes the release return `InvalidRecordStateException` (SP:1170), which rolls back the whole member release (all partitions' batches in that call). |
| 71299c229 | KAFKA-19972 | Bump delivery count on session release | Session release used `DeliveryCountOps.DECREASE`, so a consumer that crashes in a loop gets the same records forever and they are never archived (poison record). Fix: NO_OP. | delivery count | Med | Complete at call sites. `IFS:163` still has the dead `ops != DECREASE` exemption, and no caller uses DECREASE now. |
| 7b017fadd | (MINOR) | Throttling looked at batches/offsets with an ongoing transition | `shouldThrottleRecordsDelivery` counted non-AVAILABLE or ongoing-transition batches, which caused early breaks or wrong isolation. | delivery throttling | Med | Complete (SP:2108-2130). |
| e1cfea338 | KAFKA-20505 | Deadlock: futures completed under the SharePartition write lock | `rollbackOrProcessStateUpdates` completed the request future while holding the lock. Dependent actions take purgatory locks, which is a lock-order inversion. Reproduced in franz-go tests. | concurrency | High | The commit says the other sites were reviewed. Current code completes futures after unlock (SP:510-528, 2599-2606, 3403-3415). OK. |
| f771a1e9b | (MINOR) | `Uuid` compared with `!=` in init and write response validation | Reference comparison. A distinct-but-equal Uuid in the persister response made init fail or made a successful write look failed (which triggers a rollback). | init / persister | Med | Complete. No other `topicId() !=` in share code. |
| 08e287870 | KAFKA-20672 | Records persisted as ARCHIVING never finish after re-init | DLQ phase 1 (ARCHIVING) was persisted, but phase 2 was lost on leader change. ARCHIVING is non-terminal, so the start offset stalls forever. Fix: collect ARCHIVING batches in `maybeInitialize` and resume `initiateDLQAndArchive` after init. | init / DLQ | High | Complete for init. Side effect: the old fenced instance may still run its own phase 2, giving duplicate DLQ records (see M6). |
| 8fe1157ac | (MINOR) | Persister batch with `last < first` accepted at init | No validation, so the cache got corrupt. Fix: fail init. | init | Low | Complete (SP:503-509). Overlapping persister batches (`first <= previousLast`) are still not rejected (SP:511-514 only detect gaps). |
| ea6afaa46 | (MINOR) | Stale fetch-offset `LogOffsetMetadata` cached on SharePartition | The cache outlived the request. After trimming or failure, minBytes estimates went wrong and released records waited for the request timeout. Moved into DelayedShareFetch. | fetch path | Med | Complete (removed from SP). |
| d193373f2 | KAFKA-21344 | Partition starvation in share fetch rotation | `sessionEpoch % size` rotation plus HashMap-ordered response reorder starved one partition. | fetch fairness (`fetch/PartitionRotateStrategy`, `ShareFetch`) | Med | Complete in scope. |
| 3eb0b8c1b | KAFKA-21096 | SharePartition metrics leak | Gauges captured fenced SharePartitions. Added `close()`. | lifecycle | Low | **Leaves a sibling gap:** `close()` and `markFenced()` (SP:1129, 1589) cancel no acquisition-lock timers and do not stop DLQ phase 2. `writeShareGroupState` does not check `partitionState` (SP:2842). See M6. |
| 43062a0ee | KAFKA-19957 | Accumulate per-offset acquired records | Response had one AcquiredRecords per offset. Merges contiguous offsets within one in-flight batch. | response shape | Low | n/a |
| aef1fac12 | (MINOR) | Invalid ack type gave NPE | `requireNonNull` → now `InvalidRequestException`. | ack validation | Low | Complete. |

### 2b. Feature / behavior changes that affect the state machine

| Hash | KAFKA | Change | Model-relevant consequence |
|---|---|---|---|
| c48bc49a8 | KAFKA-20245 [1/N] | New `ARCHIVING` RecordState (id 3), ARCHIVED becomes id 4 | `RS.validateTransition` treats ARCHIVING as a normal non-terminal state: ARCHIVING→ACQUIRED/AVAILABLE/ACKNOWLEDGED all pass (RS:57-66). Only caller-side checks keep it out. `archive()` bypasses validation (IFS:183). |
| 6c0778aea | KAFKA-20245 [2/N] | REJECT → ARCHIVING when DLQ enabled | Two-phase archive: persist ARCHIVING, enqueue DLQ, then archive + persist ARCHIVED. |
| cdc9eb2c3 | KAFKA-20245 [3/N] | Max-delivery exhaustion → ARCHIVING (`tryUpdateState`), timeout path drives DLQ | Rollback-after-expiry path not updated (M5). |
| 37d0f70e0 / d069b7cd1 / f2f31100c / bbd81e8d0 | DLQ | DLQ gated on group DLQ topic config, manager wiring | `isDLQEnabledForGroup()` is read at each transition, so dynamic config can change between phase 1 and 2. Phase 2 is unconditional (SP:3340). |
| e658d545d | KAFKA-19843 | RENEW ack | Cancels and reschedules the lock timer in place, with no state transition and no persist. It is applied immediately even if a later batch in the same request fails and rolls back. The cancel makes a fired-but-waiting timer no-op via `isCancelled()` (SP:2955). |
| ee2204f21 | KAFKA-20161 | `share.renew.acknowledge.enable` gate | Config only. |
| bcd3191c7 / fceff8ac2 / b4aa1087a | KAFKA-17541 | Throttled delivery: halve acquired subset when deliveryCount >= ceil(max/2), deliver last attempt alone | Throttling relies on `== maxDeliveryCount - 1` (SP:2017), which assumes the count never skips past it (see a670e2b20). |
| a670e2b20 | KAFKA-20159 | `share.delivery.count.limit` is dynamic | `maxDeliveryCount()` is re-read each call. Acquire never checks the limit (it only applies on ACQUIRED→AVAILABLE, IFS:163). After lowering the limit, AVAILABLE records with count >= new limit are delivered again, so the limit is exceeded by one. `RollbackState` snapshots the max at transition start (IFS:207). |
| 37c0b9deb | KAFKA-20160 | `share.partition.max.record.locks` dynamic | Lowering it makes `maxRecordsToAcquire <= 0`, which takes the "fetch in-between" branch from 25aae9903 (SP:1743-1757). |
| c205e6910 / a9cc8f21e | KAFKA-19020 | `record_limit` acquire mode | New split path `IFB.maybeInitializeOffsetStateUpdate(target, delayMs)` (IFB:205-219). Offsets after `target` start AVAILABLE with `deliveryCount-1` (=0). |
| c31f699e6 / b900f2fe5 | KIP-1226 | `deliveryCompleteCount` (in-flight terminal count) persisted in writeState | Many increment and decrement sites. Undercount on the rollback-after-expiry → ARCHIVED path (M5). |
| d5e624e91 | KAFKA-19693 | `PersisterBatch(updatedState, stateBatch)` refactor | Rollback holds a reference to the **InFlightState object**. If the batch is split afterwards, the reference is detached (M3). |
| bd8ea36a1 | KAFKA-19846 | memberId kept as String on broker (`/`,`+` broke Uuid decode) | Member-ownership checks are now string compares. |
| b03c9966a | (MINOR) | Persister auth/version errors handled | Maps to UnknownServerException → rollback. |
| 0a003d093 | KAFKA-19823 | PartitionMaxBytesStrategy gave 0 bytes | Out of model scope (fetch bytes). |

### 2c. Not correctness-relevant (message read)
342a8e677 (graft root), fac683804 (API-usage annotations, 947 files), 0166a0342 (package move),
f8705000e (LogReader/PartitionMetadataProvider wiring), 2f047bedb, 2700824ab (GroupConfigManager),
3e3f0c431 (client empty ShareFetch), 7d54f7b03 (DelayedOperationKey record), 3f8ca851e, 26b6634ec
(log text), 5e6150caf (hotfix build), fceff8ac2 (logging), b4aa1087a (tests), d069b7cd1, bbd81e8d0,
f2f31100c (DLQ plumbing).

## 3. Mechanism groups

### M1. Start offset / LSO / gap-window handling (most frequent: 4 fixes)
55764f837 (start offset regress), 18dbd5227 (acquire below start offset, overlapping batches),
25aae9903 (last offset beyond fetched data), 8fe1157ac (bad persister batch).
Shared root cause: three independent cursors (`startOffset`, `endOffset`, `persisterReadResultGapWindow`)
plus cached batches that may straddle or sit entirely below `startOffset` after an LSO move. Each fix
patched one site.
**Open sibling paths:**
- SP:2647: `startOffset = lastCachedOffset + 1` without the `> startOffset` guard that 55764f837 added
  to the other two branches. Repro sketch: acquire 0-4, `updateCacheAndOffsets(10)`, let the lock
  expire. Expected start offset 10, actual 5. Self-heals through the OOR → `updateCacheAndOffsets`
  path in `ShareFetchUtils.java:84-87`, but a writeState in between persists the lower start offset.
- SP:2683 and SP:651: the gap window is never adjusted on LSO movement. `nextFetchOffset` can return
  `gapStartOffset < LSO`. The fetch gets OFFSET_OUT_OF_RANGE, and `updateCacheAndOffsets(LSO)` then
  returns early because `logStartOffset <= startOffset` (SP:1252). Nothing changes, so this is a
  **suspected livelock**: the partition gets no deliveries until `endOffset` changes, which needs an
  acquire, which needs a successful fetch. Needs a unit test: init with startOffset S and a persister
  batch at S+10, `updateCacheAndOffsets(S+5)`, then check `nextFetchOffset()`.
- SP:907/943: the overlap path in acquire still uses `firstBatch.baseOffset()` rather than
  `max(baseOffset, startOffset)` (residue of 18dbd5227).

### M2. Lock timeout vs acknowledge / release / renew
25aae9903 (timeout handler cannot find base offset, so records stay ACQUIRED), dfa28c463, e658d545d (RENEW),
7b017fadd (ongoing transitions). Current protection: the timer sets `hasExpired` before taking the
lock (`AcquisitionLockTimerTask.run`). The handler bails if `isCancelled()` (SP:2955), only acts on
`state==ACQUIRED`, and rollback consults `hasExpired` (IFS:228). Every exit from ACQUIRED cancels the
timer, so priority Q1 (double win) looks safe in code. Cancelling a running task does set
`isCancelled()` (`TimerTask.cancel` nulls the entry).
Residual: the handler does not check member id or that the batch's current task `== timerTask`. It
relies entirely on cancel discipline, so this invariant is worth putting in the model.

### M3. Batch vs per-offset state split
dfa28c463, 7b017fadd, 43062a0ee, c205e6910 (new 2-arg split), 18dbd5227.
**Open sibling path:** `IFB.maybeInitializeOffsetStateUpdate()` (IFB:225-249) copies
state/deliveryCount/memberId/timer into per-offset states but **not `rollbackState`**. Every caller
except one first ensures the batch has no ongoing transition: acquire SP:930-938, release (ACQUIRED)
SP:1096, ack SP:2226, timeout (ACQUIRED) SP:2973. The exception is `archiveRecords` (SP:1448-1451),
which only checks `batchState() == AVAILABLE`. An AVAILABLE batch with an ongoing RELEASE or
session-release write can be split by an LSO move or by compaction stale-batch archiving
(`maybeArchiveStaleBatches`, which runs inside acquire). The pending `PersisterBatch.updatedState`
then points at the detached batch InFlightState. On write failure the rollback applies to that dead
object, while the per-offset entries stay AVAILABLE with no ongoing marker, so they can be acquired
by another member before or despite the failed persist. The original member was told its release
failed and so still believes it holds the records. That is concurrent delivery to two members
(Q4-adjacent). Severity Med-High, needs test.

### M4. In-memory state advanced before or without persist
e1cfea338 (future completion), 08e287870 (ARCHIVING never finished), b03c9966a.
Intentional design points that keep the divergence (relevant to Q2/Q3):
- The timeout handler applies AVAILABLE/ARCHIVED/ARCHIVING in memory even when writeState fails
  (SP:2994-3011, comment at SP:2997), and then calls `maybeUpdateCachedStateAndOffsets()`, which may
  advance `startOffset` in memory only. That call's result is ignored, so purgatory is not notified
  (liveness nit).
- DLQ phase 2 archives in memory and persists ARCHIVED best-effort (SP:3388-3413).
- LSO movement archives in memory only and the start offset is written lazily (SP:1287-1289).
- Acquire is never persisted, so delivery-count increments for in-flight deliveries are lost on
  leader change (Q2, design-level).
- `rollbackOrProcessStateUpdates` (SP:2510-2615) is the only path that rolls back on persist failure.

### M5. Rollback after lock expiry (`InFlightState.completeStateTransition(false)`)
Not fixed by any commit, but it sits under three of them (cdc9eb2c3, c31f699e6, 55764f837):
- IFS:228-233: if the lock expired during the ack/release write and the write fails, the state becomes
  `ARCHIVED` when `deliveryCount >= max`, else `AVAILABLE`.
  (a) It never yields `ARCHIVING`, so **DLQ is skipped** even when DLQ is enabled. cdc9eb2c3 updated
  `tryUpdateState` but not this path.
  (b) Nothing persists this ARCHIVED or AVAILABLE, and no `maybeUpdateCachedStateAndOffsets` runs in
  the failure branch.
  (c) `deliveryCompleteCount` is not incremented for a transition to ARCHIVED (SP:2524-2533,
  2563-2572 only decrement), so it undercounts.
  (d) It sets AVAILABLE even for offsets below `startOffset` after an LSO move. The release and
  timeout paths use `offset < startOffset ? ARCHIVED : AVAILABLE` (SP:1160, 1204, 3025, 3091), and
  `archiveRecords` only archives AVAILABLE, so an ongoing ACKNOWLEDGED record below the new LSO comes
  back as AVAILABLE below `startOffset`. That feeds the M1 livelock and the `firstBatch.baseOffset()`
  residue.

### M6. Initialization vs concurrent ops / stale instances
08e287870, 8fe1157ac, f771a1e9b, 18dbd5227, 3eb0b8c1b.
**Open gap:** a SharePartition can be removed and re-created **with the same leaderEpoch**. Paths:
`onGroupEmpty` (SharePartitionManager:937-949), init failure (:801), and the fenced handler for
GroupIdNotFound/UnknownTopicOrPartition (:821). The old instance is only `markFenced()` + `close()`
(SP:1589, 1129). Its acquisition-lock timers, in-flight `rollbackOrProcessStateUpdates` callbacks and
DLQ phase 2 keep running, and they call `writeShareGroupState` without checking `partitionState`
(SP:2842). The coordinator fences only `leaderEpochMap > requestEpoch`, a strict comparison
(ShareCoordinatorShard.java:788), so same-epoch stale writes are accepted. This can overwrite state
the new instance persisted, e.g. ACKNOWLEDGED rewritten to AVAILABLE, with an older startOffset and
deliveryCompleteCount (priority Q5 and the guidance's "late writeState" interaction). It also gives
duplicate DLQ enqueues with 08e287870. Severity High (suspected). Good TLA+ target.

### M7. Delivery count semantics
71299c229, bcd3191c7, 7b017fadd, a670e2b20. Open items: dead DECREASE exemption (IFS:163). The
dynamically lowered limit is not enforced at acquire (SP:962, 2029). The throttle checks use `==`
(SP:2017, 2049), which a lowered limit can skip past.

### M8. Lock ordering / liveness
e1cfea338 (deadlock), ea6afaa46, d193373f2 (starvation). No open sibling found.

## 4. Suspected incomplete fixes / unfixed sibling paths (summary)

| # | Location | Related commit | Issue | Sev |
|---|---|---|---|---|
| S1 | SP:2647 | 55764f837 | `startOffset = lastCachedOffset+1` without `> startOffset` guard, so start offset regresses when the whole cache is below the LSO | Med-High |
| S2 | SP:2683, SP:651, SP:1252 | 55764f837 / gap window | Gap window not updated on LSO move: start offset regresses, and the OOR loop is a suspected livelock | High (suspected) |
| S3 | IFB:225-249 via SP:1448-1451 | d5e624e91 / dfa28c463 | Split during an ongoing AVAILABLE transition drops `rollbackState`, so rollback is lost and records can be re-acquired before persist | Med-High |
| S4 | IFS:228-233 | cdc9eb2c3 | Rollback-after-expiry gives ARCHIVED without DLQ, not persisted, deliveryCompleteCount undercount | Med |
| S5 | IFS:228-233 | 55764f837 / LSO | Rollback-after-expiry gives AVAILABLE below startOffset | Med (suspected) |
| S6 | SP:907, SP:943 | 18dbd5227 | Overlap path still uses unadjusted `firstBatch.baseOffset()` | Low-Med |
| S7 | SP:1129, SP:1589, SP:2842 + coordinator strict `>` | 3eb0b8c1b / 08e287870 | Fenced instance keeps writing with the same leader epoch, so stale state overwrites the new instance | High (suspected) |
| S8 | SP:2962-2965 | 25aae9903 | Timeout handler silently returns when the base offset is missing, so records stay ACQUIRED with no timer | Med |
| S9 | SP:2999 | e1cfea338 | Timeout-path `maybeUpdateCachedStateAndOffsets()` result ignored, so no purgatory wakeup | Low |
| S10 | SP:962, SP:2029, IFS:163 | a670e2b20 / 71299c229 | Lowered delivery limit not enforced at acquire; dead DECREASE branch | Low |
| S11 | RS:57-66 | c48bc49a8 | Validator allows ARCHIVING→ACQUIRED/AVAILABLE/ACKNOWLEDGED; only caller checks prevent it | Low (model invariant) |

## 5. Suggested model invariants from this archaeology
- `startOffset` is monotonic and `startOffset >= LSO` after `updateCacheAndOffsets` (S1, S2).
- No two cached batches overlap (18dbd5227).
- `nextFetchOffset >= startOffset` (S2, S5).
- Every record with `hasOngoingStateTransition` stays non-acquirable until its write resolves, including across split (S3).
- A record that is ARCHIVED with DLQ enabled and `deliveryCount >= max` went through ARCHIVING (S4).
- Writes from a fenced SharePartition instance are never applied (S7).
- An ACQUIRED record always has a live, non-cancelled lock timer whose range covers it (S8, 25aae9903).
