# Deep analysis: SharePartition in-memory record state machine (KIP-932)

Target revision: kafka `8aa57006f` (cloud checkout). Files read in full:
`core/src/main/java/kafka/server/share/SharePartition.java` (3603 lines),
`server/src/main/java/org/apache/kafka/server/share/fetch/{InFlightBatch,InFlightState,AcquisitionLockTimerTask,AcquisitionLockTimeoutHandler,RecordState,DeliveryCountOps}.java`,
`server-common/.../util/timer/{TimerTask,TimerTaskEntry}.java`, and the callers in
`ShareFetchUtils.java` / `DelayedShareFetch.java`. Relevant tests in `SharePartitionTest.java` were checked.
Abbreviations: SP = SharePartition.java, IFS = InFlightState.java, IFB = InFlightBatch.java.

No Kafka code was changed and no tests were run (building core is heavy). Every finding below is from
reading the code path by path. Findings marked "Test-verifiable" can be reproduced deterministically with
the existing `SharePartitionTest` harness (mock persister returning an incomplete `CompletableFuture`, `MockTimer`).

---

## 1. Concurrency structure

### 1.1 Locks and threads

| Primitive | Guards | Notes |
|---|---|---|
| `lock` (`ReentrantReadWriteLock`, SP:184, 388) | `cachedState`, every `InFlightBatch`/`InFlightState`, `startOffset`, `endOffset`, `findNextFetchOffset`, `persisterReadResultGapWindow`, `partitionState` | All mutating paths take the **write** lock. Nested private helpers take it again (it is reentrant). Read lock only in getters and `countTerminalRecordsUpToNewLSO`, `findLastOffsetAcknowledgedAndMetadata`, `fetchSubMap`, `numInFlightRecords`. |
| `fetchLock` (`AtomicReference<Uuid>`, SP:191, 1547-1584) | Serializes fetch + `acquire` + `updateCacheAndOffsets` (both called from `ShareFetchUtils.processFetchResponse` while the fetch lock is held) | Not held by acknowledge, release, timer, persister callbacks. |
| `deliveryCompleteCount` (`AtomicInteger`) | Lag metric, also sent in every `writeState` (SP:2849) | Updated under the write lock, read without it. |
| `AcquisitionLockTimerTask.hasExpired` (volatile, AcquisitionLockTimerTask:35,73) | Set by the **timer thread before it takes the SP lock** | Read in `IFS.completeStateTransition(false)` (IFS:228) under the lock. |
| `TimerTask.timerTaskEntry` (TimerTask.java) | `cancel()` nulls it; `isCancelled()` = entry == null | Expiry does **not** null it, so `cancel()` after expiry still marks the task cancelled. The handler's `isCancelled()` check (SP:2955) is therefore effective even when `run()` already started. |

Threads that touch the state machine: request handler threads (acknowledge, release on session close,
fetch/acquire, LSO update), the `SystemTimer` reaper/executor (`AcquisitionLockTimerTask.run`),
persister response threads (the `writeState` future callbacks: SP:2549, 2992, 3403) and the DLQ manager
completion thread (SP:3382).

### 1.2 Pending ("in transition") state

`IFS.startStateTransition` (IFS:203-210) applies the new state **immediately** (`state`, `deliveryCount`,
`memberId`) and stores the old triple in `rollbackState`. `hasOngoingStateTransition()` = `rollbackState != null`
(IFS:131-135). The acquisition-lock task is **not** cancelled at this point; it is cancelled only at commit
(IFS:220-224) or on rollback after expiry (IFS:228-234).

`completeStateTransition(commit)` (IFS:219-244):
- commit or `isTerminalState` (set only by `archive()`, IFS:183-188): cancel and clear timer, clear rollback.
- rollback: if the timer task `hasExpired()`, state := ARCHIVED if `previous.deliveryCount >= maxDeliveryCount`
  else AVAILABLE, memberId := "" and cancel the timer. Otherwise restore the previous state and memberId.
  The delivery count is **never** restored (no transition started through `startStateTransition` changes it:
  all callers use `DeliveryCountOps.NO_OP`).

How other operations treat a pending record:
| Operation | Pending check | Effect |
|---|---|---|
| `tryUpdateState` (IFS:150-161) | returns null if pending | blocks acquire (full batch SP:962, subset SP:2029) and timeout (SP:3024, 3090) |
| acquire pre-checks | SP:930, 956, 1992, 2115, 2128 | skip pending AVAILABLE |
| acknowledge | batch: SP:2228; offset: SP:2332 (after the `state != ACQUIRED` check at SP:2324) | reject with `InvalidRecordStateException` |
| timeout handler | checks `state == ACQUIRED` (SP:3023, 3084); a pending record's state is already the *target* (ACKNOWLEDGED/AVAILABLE/ARCHIVED/ARCHIVING), so it is skipped | the expired task stays attached; rollback later observes `hasExpired` |
| release (session close) | only touches `state == ACQUIRED` (SP:1097, 1157, 1202), and ACQUIRED is never a pending target, so pending records are skipped | |
| `nextFetchOffset` | SP:669, 676 skip pending AVAILABLE | |
| `canMoveStartOffset` / `findLastOffsetAcknowledgedAndMetadata` | SP:2738-2743, 2791, 2798 | do not advance past pending |
| `archiveRecords` (LSO move / compaction stale batches / aborted txn) | **no pending check** (SP:1441-1457, 1483, 1508) | full batch: `archive()` sets `isTerminalState`, so a later rollback keeps ARCHIVED (by design, SP:1238-1243). Split path: see **F1**. |
| `IFB.maybeInitializeOffsetStateUpdate()` (IFB:225-249) | **does not copy `rollbackState`** into the per-offset states | see **F1** |

Note: `state == ACQUIRED && rollbackState != null` cannot happen. No caller starts a transition *into*
ACQUIRED (acquire uses `tryUpdateState`), and rollback clears `rollbackState`. So the guards that test
"state == ACQUIRED" are implicitly "not pending". This is a useful invariant for the model.

### 1.3 Atomicity boundaries per operation

**acquire** (SP:749-999). Before the lock: `stateNotActive()`, `startOffset()` (separate read lock,
SP:774), `lastOffsetAndMaxRecordsToAcquire` (read lock, SP:1723-1769). Under one write-lock section
(SP:792-998): `maybeArchiveStaleBatches`, cache lookup, per-batch/per-offset `tryUpdateState(ACQUIRED, INCREASE)`
plus scheduling a new timer task (`timer.add`) plus attaching it, new batches via `createBatches`/`addBatches`,
`startOffset`/`endOffset` updates, gap window update, aborted-txn archive (`archiveRecords(..., ACQUIRED, true)`
SP:3181). Nothing is persisted. The TOCTOU between the max-records computation and the lock only makes the
computation more conservative (concurrent operations can only shrink the in-flight count, and acquires are
serialized by the fetch lock).

**acknowledge** (SP:1010-1073): phase A under the write lock (SP:1019-1068). It validates and calls
`startStateTransition` per batch or offset, collects `PersisterBatch`, updates `deliveryCompleteCount` eagerly,
and may split batches (`maybeInitializeOffsetStateUpdate`). RENEW cancels and reschedules the timer in phase A
and is never rolled back. Phase B runs outside the lock: `rollbackOrProcessStateUpdates` (SP:2512-2617). On a
synchronous error, rollback runs under a new lock section (SP:2518-2538). Otherwise `writeShareGroupState` is
issued **outside the lock** (SP:2548) and phase C runs on the persister thread under the lock (SP:2555-2594):
commit or rollback each state, `maybeUpdateCachedStateAndOffsets`. Then, outside the lock: complete the future,
notify delayed fetch, and start the DLQ phase 2 for ARCHIVING batches (SP:2607-2615).

**releaseAcquiredRecords(member)** (SP:1082-1126): same three phases as acknowledge. Phase A scans the whole cache.

**timer expiry** (SP:2945-3017). `run()` sets `hasExpired=true` before taking the lock (AcquisitionLockTimerTask:73).
Under the lock: `isCancelled` check, split the batch if it straddles startOffset, then `tryUpdateState(AVAILABLE|ARCHIVED, NO_OP)`.
This is **committed immediately**, with no pending window, and the task is cancelled and cleared. Outside the lock,
`writeShareGroupState` runs fire-and-forget: the result is ignored apart from logging (SP:2992-3009). After the
write completes, `maybeUpdateCachedStateAndOffsets` and DLQ phase 2 run. The delayed fetch is notified right away (SP:3015).

**DLQ phase 2** (SP:3376-3417). After enqueue, under the lock: `archiveAction.run()` (`IFS.archive()` → terminal),
`deliveryCompleteCount += n`. Outside the lock: write ARCHIVED, then `maybeUpdateCachedStateAndOffsets` under the lock.

**updateCacheAndOffsets(LSO)** (SP:1247-1298). One write-lock section: count terminal records below the LSO,
archive AVAILABLE records below the LSO (no persist), set `startOffset = LSO` and maybe `endOffset`.
**Not persisted**; the gap window is not touched.

---

## 2. Priority question answers

### PQ1: timer expiry vs acknowledge for the same offsets

In the normal (unsplit, no-LSO) paths **exactly one wins**, and the delivery count changes once per acquisition:

1. Ack first (phase A done), then the timer fires. The record's state is the ack target, so the handler skips it
   (SP:3023/3084). Write succeeds: commit cancels the task. Write fails: rollback sees `hasExpired` and moves the
   record to AVAILABLE/ARCHIVED (IFS:228-234). The ack never takes effect.
2. Timer fires and its handler holds the lock first. The record becomes AVAILABLE with memberId "". The late ack
   fails: batch level gets the EMPTY member check at SP:2287, offset level gets `state != ACQUIRED` at SP:2324.
3. Member B acquires after the expiry. A's ack fails the memberId check (SP:2293, SP:2341).
4. `run()` has set `hasExpired`, the handler is blocked, and the ack's write fails with rollback at once.
   Rollback → AVAILABLE and **cancels** the task, so the handler sees `isCancelled()` and returns (SP:2955).
   Only one transition happens.
5. RENEW cancels the old task under the lock (SP:2365, 2449). If that task's handler is already blocked, it sees
   cancelled and returns.

The delivery count is incremented only in acquire (SP:962, 2029; new batches start at 1, SP:1875/1885/1902/1945).
Ack, release and timeout use NO_OP, and rollback does not touch it. A double increment for one delivery does not
happen. **Burned** increments (count up, no delivery) do happen through **F1** (exception after `tryUpdateState`).

Exception: the "exactly one" argument depends on the invariant that a non-cancelled timer task exists only for
the *current* acquisition. The handler itself does not check task identity or member (**F6**). **F1** breaks the
invariant, and then a stale task can release a later member's acquisition early.

Design limitation (Code-review-only, not a bug in the implementation): an ack carries no delivery-attempt id.
If member A's lock expires and **A itself** re-acquires the same offsets, a delayed ack from A's first delivery
is accepted for the second.

### PQ4: max delivery / archiving

- **ARCHIVED while acquired?** Not through the normal paths. LSO movement and stale-batch archive only archive
  `initialState == AVAILABLE` (SP:1358, 1337). The aborted-transaction archive uses `initialState == ACQUIRED`
  (SP:3181), but only on records acquired in the same `acquire` call, which are filtered out of the response
  (SP:3183), so no consumer holds them. Archiving on max delivery happens only when a record leaves ACQUIRED
  (release, ack RELEASE, timeout) via `tryUpdateState`'s `deliveryCount >= max` rule (IFS:163-165).
- **Per-offset awareness.** Each of acquire, ack, release, timeout and archive has a batch branch and an offset
  branch. Pair by pair:
  - release: complete (SP:1186-1227) vs offset (SP:1142-1184). Consistent (ARCHIVED-below-start rule,
    deliveryCompleteCount restricted to in-flight). Straddling batches are split first (SP:1096-1103).
  - timeout: complete (SP:3019-3065) vs offset (SP:3067-3127). Consistent; the split is done first (SP:2971-2978).
  - ack: complete (SP:2416-2499) vs offset (SP:2301-2414). The batch branch checks member and pending before the
    state check, the offset branch checks state first. Both reject. Consistent.
  - acquire: full (SP:956-977) vs subset (SP:1963-2071). Consistent, both guarded against pending.
  - archiveRecords: full (`archiveCompleteBatch`, SP:1501-1522) vs split (SP:1447-1453 → 1465-1499).
    **Inconsistent with pending transitions (F1).** The full path sets the terminal flag on the pending state
    object. The split path drops the pending transition and leaves live timer tasks on AVAILABLE offsets.
  - `canMoveStartOffset` batch vs offset branch (SP:2737-2748): **the offset branch can NPE (F4).**
- The rollback-after-expiry decision (IFS:216-217) is a **third copy** of the "release" rule. It ignores both
  the below-startOffset rule (**F2**) and DLQ (**F7**).

---

## 3. Findings

### F1. A pending (in-flight persist) AVAILABLE batch split by LSO movement loses its pending transition and leaves live acquisition-lock tasks on AVAILABLE offsets
- **Where:** `archiveRecords` SP:1441-1453 calls `IFB.maybeInitializeOffsetStateUpdate()` IFB:225-249 when
  `batchState == AVAILABLE`. There is no `batchHasOngoingStateTransition()` guard, unlike acquire (SP:930) and
  ack (SP:2228).
- **Trace:**
  1. Member A holds batch [10-19] ACQUIRED with task T. A's session closes (`releaseAcquiredRecords`,
     SP:1203) or A sends a full-batch RELEASE ack (SP:2467). `startStateTransition(AVAILABLE)` runs:
     state=AVAILABLE, memberId="", rollbackState set, **T still attached**. writeState is in flight.
  2. The log start offset moves to 15. The fetch gets OFFSET_OUT_OF_RANGE, so `ShareFetchUtils:83-87` calls
     `updateCacheAndOffsets(15)`, which calls `archiveRecords(10, 15, ..., AVAILABLE)`. The batch is not a full
     match and is AVAILABLE, so `maybeInitializeOffsetStateUpdate()` runs. Because T != null, every offset gets
     a **new timer task** with the remaining delay (IFB:232-238), state AVAILABLE, **no rollbackState**. T is
     cancelled and `batchState` becomes null. The old `InFlightState` object, still referenced by the pending
     `PersisterBatch`, is orphaned.
  3. Offsets 10-14 are archived (`archive()` cancels their tasks). Offsets 15-19 are AVAILABLE, not pending,
     **each with a live timer task**.
  4. Consequences:
     - (a) 15-19 can be acquired before the release is persisted.
     - (b) `acquireSubsetBatchRecords` calls `tryUpdateState(ACQUIRED, INCREASE)`, which succeeds (deliveryCount++).
       It then calls `scheduleAcquisitionLockTimeout` (a new task is added to the timer), and
       `updateAcquisitionLockTimeoutTask` **throws `IllegalArgumentException`** (IFS:105-108). The offset is left
       ACQUIRED by B with the old task attached. The new task is in the timer but referenced by nothing. `acquire`
       throws, `DelayedShareFetch:493` catches it, and B gets nothing. Any records acquired earlier in the same
       call are also never delivered, which burns their delivery attempt.
     - (c) If the per-offset task fires while the offset is still AVAILABLE, the handler skips it (SP:3084) and
       does **not** clear the reference, so the next acquire of that offset hits (b) again.
     - (d) The leaked task from (b) is never cancelled. When it fires, the handler releases **whatever is
       ACQUIRED at that offset**, because there is no identity or member check (SP:2955-2985, F6). That can cut
       short a later, legitimate acquisition by member C, so C's lock is not honored and D can acquire the record
       while C is still processing it.
     - (e) When the release write completes, commit or rollback runs on the orphan (SP:2563/2582), so a write
       failure no longer restores anything in the cache.
- **Compensation checked:** the Javadoc at SP:1238-1243 claims a rollback cannot resurrect AVAILABLE because
  of the terminal flag. That only holds for the full-batch path. Tests: `testLsoMovementWithPendingAcknowledgements`
  (SP-Test:11669) covers pending **ACKNOWLEDGED** (not AVAILABLE) offsets. `testLsoMovementForArchivingOffsetsWithStartOffsetNotFullMatchesPostReleaseAcknowledgement`
  (6535) covers RELEASE but with an already-completed write. No test covers a pending AVAILABLE batch split by LSO.
- **Confidence:** High for the mechanism (direct code trace). Medium for practical likelihood: an LSO move has
  to land inside a batch while a release/RELEASE-ack write is outstanding.
- **Classification:** Test-verifiable: a mock persister returns an incomplete future, then
  `releaseAcquiredRecords`, `updateCacheAndOffsets(mid)`, and `acquire` → expect IAE. Model-checkable for the
  downstream double-holder effect (d).

### F2. Rollback after lock expiry can make a record **below startOffset** AVAILABLE, which can livelock the partition
- **Where:** IFS:216-217 (`state = previous.deliveryCount >= max ? ARCHIVED : AVAILABLE`), called from
  SP:2524/2563. It does not consider `startOffset`. Every other release path archives offsets below startOffset
  (SP:1160, 1204, 3025, 3091).
- **Trace:**
  1. Member A acks offset 5 with ACCEPT or REJECT. It is pending as ACKNOWLEDGED/ARCHIVED and the write is outstanding.
  2. The LSO moves to 10 (`updateCacheAndOffsets`). That archives only AVAILABLE records, so the pending offset
     5 keeps its non-AVAILABLE state. startOffset is now 10.
  3. The lock expires (the handler skips the pending offset).
  4. The write fails (coordinator unavailable or timeout). The rollback sees `hasExpired` and sets offset 5 to
     AVAILABLE and `findNextFetchOffset=true` (SP:2564). The same happens for a pending release batch below the
     LSO if it was not archived by the LSO path, because its target is ARCHIVED (SP:1204).
  5. `nextFetchOffset()` does not filter offsets below startOffset (SP:644-686) and returns 5. The fetch at 5
     gets OFFSET_OUT_OF_RANGE, and `updateCacheAndOffsets(10)` returns early because `logStartOffset <= startOffset`
     (SP:1252-1255). The next fetch is again at 5, so this loops. `findLastOffsetAcknowledgedAndMetadata` stops at
     the AVAILABLE offset (SP:2791/2798), so the cache is never trimmed either.
- **Confidence:** Medium. The code path is clear, but it needs three things to coincide: a pending write,
  LSO movement, and lock expiry followed by write failure.
- **Classification:** Model-checkable (an interleaving of ack, LSO move, timer and persister failure). Once the
  interleaving is known, it is also test-verifiable with a mock timer and persister.

### F3. The persister-read gap window is not adjusted when the LSO moves, so `nextFetchOffset` can stay below startOffset (fetch OOR livelock)
- **Where:** `updateCacheAndOffsets` (SP:1247-1298) never touches `persisterReadResultGapWindow`.
  `nextFetchOffset` returns `gapStartOffset` whenever the window is active and the first cached key is greater
  than it (SP:643-653). The window stays active while `endOffset == window.endOffset` (SP:2751-2753).
- **Trace:**
  1. Init reads persisted `startOffset=0` and batches [10-19] AVAILABLE. That gives gap window
     (gapStart=0, end=19) and endOffset 19 (SP:518-553). The persisted start can lag the log start offset
     (idle group plus retention, then a leader move).
  2. The log start offset is 5. `nextFetchOffset()` returns 0, the fetch gets OOR, and `updateCacheAndOffsets(5)`
     archives nothing (the first key 10 is at least 5, SP:1434), sets startOffset=5 and leaves endOffset at 19.
  3. `nextFetchOffset()` still returns 0. Fetch OOR, then `updateCacheAndOffsets(5)` returns early (SP:1252).
     This repeats forever. Only `acquire` advances the gap window (SP:1693-1712), and it is never reached.
- **Compensation checked:** none. `testLsoMovementWithGapsInCachedStateMap*` (SP-Test:6700-6760) cover cache
  gaps without a persister gap window.
- **Confidence:** High that `nextFetchOffset() < startOffset` after these steps. Medium-high for the end-to-end
  livelock, which relies on the OOR handling in ShareFetchUtils:83-99 as read.
- **Classification:** Test-verifiable (deterministic): initialize from a mock `readState` with a gap, call
  `updateCacheAndOffsets(5)`, and assert `nextFetchOffset() >= startOffset()`.

### F4. `canMoveStartOffset` NPEs when `floorEntry(startOffset)` is a per-offset batch that ends before startOffset
- **Where:** SP:2722-2747. `entry.getValue().offsetState().get(startOffset)` is null if
  `startOffset > entry.lastOffset()`. That happens when startOffset (set by `updateCacheAndOffsets`) falls in a
  gap after a per-offset batch, or past the last per-offset batch.
- **Trace:** the cache is {[2-6], [10-14] split into per-offset state (record_limit mode or a partial ack), [20-24]}.
  `updateCacheAndOffsets(18)` sets startOffset=18. The LSO path does not remove batches (compare
  `testLsoMovementWithGapsInCachedStateMap`, SP-Test:6700). Any later ack/release/timeout write completion then
  calls `maybeUpdateCachedStateAndOffsets` → `canMoveStartOffset` → `floorEntry(18) = [10-14]` →
  `offsetState().get(18)` is null → **NullPointerException** under the lock.
  - In the ack/release completion (SP:2592) the NPE escapes after `finally` (the future still completes
    normally), and the **DLQ phase-2 loop (SP:2607-2615) is skipped**. Records committed as ARCHIVING stay
    ARCHIVING forever, and ARCHIVING is non-terminal, so startOffset stops advancing.
  - In the timer completion (SP:2999) the DLQ loop (SP:3002) is skipped too.
  - The NPE repeats for as long as [10-14] is the floor of startOffset, and the batch can only be removed by the
    method that throws. So startOffset is frozen and the in-flight count grows until `maxInFlightRecords` stops
    new acquisition.
  - Without a later batch beyond the gap the NPE is transient: it stops once a batch keyed at startOffset is
    acquired.
- **Confidence:** High for the NPE given the cache shape. Medium for reachability: it needs the LSO to land in a
  cache gap (DeleteRecords to an arbitrary offset, or compacted/non-contiguous batches) next to a per-offset batch.
- **Classification:** Test-verifiable (deterministic).

### F5. `deliveryCompleteCount` undercounts when a rollback after expiry yields ARCHIVED from a non-terminal target
- **Where:** SP:2532, 2571 subtract only when the target was terminal and the result is not. If the target was
  AVAILABLE (RELEASE) or ARCHIVING (DLQ reject) and the rollback produces ARCHIVED (IFS:216), nothing is added.
- **Impact:** The lag metric and the `deliveryCompleteCount` sent to the coordinator (SP:2849) are low by n.
  `updateCacheAndOffsets`/`maybeUpdateCachedStateAndOffsets` later subtract by counting states, so the counter
  can drift low and even go negative. Low severity.
- **Confidence:** High. **Classification:** Test-verifiable.

### F6. The timeout handler does not verify the task's identity or owner (latent)
- **Where:** SP:2946-2986. The only guard is `timerTask.isCancelled()`. `memberId` is used only for logging, and
  there is no check that the record's current `acquisitionLockTimeoutTask() == timerTask`.
- **Impact:** Correctness rests on the discipline that every replaced task is cancelled. That holds today on all
  paths except F1(b), where a task is added to the timer (SP:2038 → 2932) and then the attach throws.
  Defense-in-depth gap; it makes F1 able to release a third member's lock early.
- **Confidence:** High (code fact). **Classification:** Code-review-only. Model it: the timer action should carry
  the task id, and an invariant should be "fired task == current task of every record it changes".

### F7. The rollback-after-expiry path (IFS:228-234) differs from the timer path
- It writes nothing to the persister. The timer path writes AVAILABLE/ARCHIVED (SP:2991). So the durable delivery
  count and ARCHIVED decision for these records stay stale until a later write; a failover can then redeliver a
  record that was archived in memory for max deliveries.
- It produces ARCHIVED, never ARCHIVING, even with DLQ enabled. The timer path produces ARCHIVING plus DLQ
  (IFS:163-164 via `tryUpdateState`). So a record that exhausted its deliveries can skip the DLQ.
- When the rollback wins the race, the handler returns early on `isCancelled` and skips
  `maybeCompleteDelayedShareFetchRequest` (SP:3015). The rollback path (SP:2555-2576) does not notify either, so a
  parked delayed share fetch is not woken for the newly AVAILABLE records.
- **Confidence:** High (code fact). Severity low-medium. **Classification:** Code-review-only (the DLQ bypass is
  test-verifiable).

### F8 (hypothesis, outside focus, unverified). Persister write reordering
Timer writes are fire-and-forget (SP:2992). If a timeout write W1 (AVAILABLE, dc=k) is retried with backoff
inside `PersisterStateManager` (retriable error, then re-enqueue) while the following ack write W2 (ACKNOWLEDGED)
from member B is sent first, the coordinator applies the batches last-writer-wins and can end with AVAILABLE.
That would cause redelivery of an acknowledged record after failover. `PersisterStateManager` keeps only one
write in flight per node (PersisterStateManager.java:1609-1640), but I did not verify ordering across retries.
Hand this to the persister/coordinator analysis.

### Non-findings (checked and OK)
- An ack and a timer can never both take effect on the same delivery (PQ1 cases 1-5).
- A pending record cannot be acquired. B's acquisition cannot be acked by A (memberId checks SP:2293, 2341).
- The record_limit split (IFB:205-219) gives unacquired offsets AVAILABLE with dc-1. That matches the
  `DECREASE`-free archive rule because `tryUpdateState` is called with INCREASE on the next acquire.
- `findNextFetchOffset` is set to true on every path that creates AVAILABLE: commit SP:2583, rollback
  SP:2525/2564, timer SP:3051/3117, record_limit SP:1909, init SP:548, archive SP:1278/1342. Setting it to false
  only happens after a full scan (SP:637, 691). Pending AVAILABLE is skipped in the scan but sets the flag again
  on commit. I found no stale-flag bug apart from F2/F3, where the scan itself returns a wrong offset.
- `RecordState.validateTransition` (RecordState:50-68) allows ARCHIVING → AVAILABLE/ACQUIRED/ACKNOWLEDGED, but no
  caller attempts it: every state-changing caller pre-checks AVAILABLE, ACQUIRED or initialState, and ARCHIVING
  batches are never split. The model should assert ARCHIVING → ARCHIVED only.

---

## 4. TLA+ action / atomicity map

### State variables (per share partition)
- `rec[o]` for each offset o in the cache: `state ∈ {AVAILABLE, ACQUIRED, ACKNOWLEDGED, ARCHIVING, ARCHIVED}`,
  `dc ∈ Nat`, `owner ∈ Members ∪ {""}`, `pending ∈ {NONE} ∪ [prev: state×owner, txn: TxnId]`,
  `terminalFlag ∈ BOOLEAN`, `task ∈ TaskId ∪ {NONE}`.
- Batch granularity: `batchOf[o]` plus `split[b] ∈ BOOLEAN`. To expose F1/F4 the model needs batch-level state
  as **one shared object** for all its offsets, and a `Split(b)` step that copies (state, dc, owner, task) but
  **not** `pending` (IFB:225-249).
- `tasks[t]`: `[offsets, member, status ∈ {SCHEDULED, EXPIRED_RUNNING, CANCELLED, DONE}, expired ∈ BOOLEAN]`
  (the `hasExpired` flag is separate from cancelled).
- `startOffset`, `endOffset`, `findNext ∈ BOOLEAN`, `gapWindow ∈ [gapStart, end] ∪ {NONE}`, `lso` (the log's
  start offset, environment), `deliveryCompleteCount`.
- `writes`: a set or sequence of outstanding persister writes `[txn, batches, kind ∈ {ACK, RELEASE, TIMEOUT, DLQ2}]`.
  `durable[o]` = the coordinator's view, abstracted as last-writer-wins.
- `held[m]`: offsets the client believes it holds (for the double-delivery invariant).

### Actions (split points marked ‖)
| Action | Entry point | Atomic step(s) | Vars written |
|---|---|---|---|
| `NextFetchOffset` | SP:602 | one step under lock | findNext |
| `Acquire(m)` | SP:749 (+1771, 1842, 1931, 1963) | one step under lock (includes stale/aborted archive). Model the IAE as a branch: state/dc changed, a task created but not attached, and nothing returned to m | rec, tasks, startOffset, endOffset, gapWindow, held[m] |
| `AckBegin(m, ranges, type)` | SP:1010-1068 | one step: validate, `startStateTransition`, maybe Split, RENEW (cancel+reschedule), dcc += | rec.pending, tasks, writes |
| `AckSyncFail` | SP:2517-2540 | one step: rollback all in txn | rec, findNext, dcc |
| `ReleaseBegin(m)` | SP:1082-1121 | same as AckBegin, over all of m's ACQUIRED | |
| `TimerFire(t)` | AcquisitionLockTimerTask.run:73 | ‖ step 1 (no lock): `expired=TRUE` | tasks[t].expired |
| `TimerHandle(t)` | SP:2950-2989 | ‖ step 2 (lock): if not cancelled, release ACQUIRED offsets in range (no owner/identity check), cancel+clear, maybe Split if straddling start, enqueue TIMEOUT write | rec, tasks, findNext, writes |
| `WriteComplete(w, ok)` | SP:2549-2594 (ACK/RELEASE), 2992-2999 (TIMEOUT), 3403-3414 (DLQ2) | ‖ separate step per write, nondeterministic ok. ACK/RELEASE: commit or rollback each state (rollback reads `tasks[t].expired`), then `MaybeUpdateCache` | rec, durable, startOffset, endOffset, cachedState, dcc |
| `DlqPhase2(b)` | SP:3382-3399 | ‖ after the ARCHIVING commit (skipped if `MaybeUpdateCache` threw, F4) | rec (terminalFlag, ARCHIVED), dcc, writes |
| `MaybeUpdateCache` | SP:2627-2709 | inside WriteComplete's step. Model the NPE precondition explicitly (F4) | startOffset, endOffset, cachedState |
| `LsoAdvance(n)` | SP:1247 via ShareFetchUtils:86 | one step under lock: archive AVAILABLE below n (incl. pending; full batch sets terminalFlag, split loses pending), startOffset := n | rec, startOffset, endOffset, findNext |
| `Init` | SP:420-591 | one step under lock from `durable` (sets gapWindow, ARCHIVING → DLQ resume) | all |
| `RenewAck` | SP:2352-2367, 2439-2454 | part of AckBegin | tasks |

Splitting guidance:
1. Keep `TimerFire` and `TimerHandle` separate. The volatile `hasExpired` write before the lock is
   observable by rollback.
2. Keep `*Begin` and `WriteComplete` separate, and let the environment interleave `LsoAdvance`, `Acquire`,
   `TimerHandle` and other `*Begin` between them.
3. `Split` must be a sub-step that is reachable from `LsoAdvance` while pending is set (F1). It is not reachable
   from Ack, Acquire or Timer while pending, because those paths are guarded.
4. The persister write is asynchronous and can fail. A TIMEOUT write's failure does not affect memory; an
   ACK/RELEASE failure rolls back.

### Invariants / properties to check
- `NoDoubleHolder`: at most one member's lock is valid per offset at a time. Track the task-valid window:
  `held[m]` minus expired.
- `ExactlyOneOfTimeoutOrAck` per delivery: an ACKNOWLEDGED commit implies no AVAILABLE transition was applied
  for that delivery.
- `DcMonotoneOncePerAcquire`: `dc` increases only in Acquire, by 1, and only when the record is delivered.
  Fails under F1(b).
- `TaskConsistency`: `rec[o].task ≠ NONE ⇒ rec[o].state = ACQUIRED ∨ rec[o].pending ≠ NONE`, and any SCHEDULED
  task is the current task of every ACQUIRED offset in its range (F1, F6).
- `NoAvailableBelowStart`: `∀o < startOffset: rec[o].state ≠ AVAILABLE` (F2).
- `NextFetchGeStart`: `nextFetchOffset ≥ startOffset` (F2, F3).
- `ArchivingEventuallyArchived` (liveness): `ARCHIVING ~> ARCHIVED` (F4).
- `PendingImpliesWrite`: `rec[o].pending ≠ NONE ⇒ ∃ w ∈ writes covering o` (F1 breaks the reverse mapping:
  a write whose state object is orphaned).
- `DurableNeverRegresses` (with F8): durable ACKNOWLEDGED/ARCHIVED never becomes AVAILABLE.
