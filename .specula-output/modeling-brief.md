# Modeling Brief: Apache Kafka Share Groups (KIP-932)

Target: `./kafka` @ `8aa57006ff5d` (trunk, 2026-11-02). Guidance: `guidance/kafka-share-groups.md`.
Paths: `SP` = `core/src/main/java/kafka/server/share/SharePartition.java`, `SPM` = `SharePartitionManager.java`,
`IFS`/`IFB` = `server/src/main/java/org/apache/kafka/server/share/fetch/InFlightState.java` / `InFlightBatch.java`,
`PSM` = `server-common/.../share/persister/PersisterStateManager.java`,
`SCS`/`SCSvc` = `share-coordinator/.../share/ShareCoordinatorShard.java` / `ShareCoordinatorService.java`,
`PSBC` = `PersisterStateBatchCombiner.java`, `SSC` = `server/.../share/session/ShareSessionCache.java`.
Full evidence: `analysis-report.md` and `analysis/*.md`.

## 1. System Overview

- Java, about 12k LOC in scope: SharePartition (3.6k), DelayedShareFetch (1.4k), SPM (1k), the share coordinator (2.5k) and the persister (2.5k).
- **Category A (Distributed / Message-Passing).** The delivery contract spans these components:
  - a partition-leader-local state machine;
  - an asynchronous, retrying RPC channel (persister → share coordinator);
  - a replicated coordinator log with snapshot/update records, replay and failover;
  - leader change.
  
  There is also a local concurrency layer: one RW lock, a timer thread and persister callback threads. It is modeled as interleaved actions, not memory-model detail.
- Protocol: per-partition in-flight records with states AVAILABLE, ACQUIRED, ACKNOWLEDGED, ARCHIVING and ARCHIVED, plus delivery counts and acquisition-lock timeouts. Durable state is a `(startOffset, batches[(first,last,state,dc)])` per share-partition key in `__share_group_state`, fenced by `leaderEpoch` and `stateEpoch`.
- Key architectural facts (all verified):
  - **Acquire is never persisted.** Only ack/release (SP:2548), lock timeout (SP:2992) and DLQ phase 2 (SP:3402) write.
  - **Writes are deltas** carrying the sender's current startOffset.
  - **The coordinator merge is order-independent:** per-offset max of (deliveryCount, stateId) and max of startOffset (PSBC:37-44, SCS:670). Write time and replay use the same combiner.
  - **Fencing rejects only strictly lower epochs** (SCS:788-795); equal epochs are accepted.
  - **The persister has no per-key ordering;** retries resend the same payload with backoff (PSM:95-97, 899-940, 1629).
  - **Ack/release are three-step:** under the lock, `startStateTransition`; outside the lock, write; on the callback thread, commit or rollback (IFS:203-244).
  - **Timer expiry and the LSO move change memory first;** their writes are fire-and-forget.
  - **A removed SharePartition is not stopped.** `markFenced` only sets a flag (SP:1589) and `close()` does not cancel timers (SP:1129). The timeout handler, `acknowledge` and `release` never check `partitionState`.

## 2. Scenarios

### Scenario 1: Zombie SharePartition instance with an unchanged leader epoch (PQ2, PQ5)

**Mechanism**: SPM replaces a SharePartition instance without bumping any epoch the coordinator fences on. The old instance's timers, in-flight acks and releases, retried persister writes and DLQ callbacks keep writing with equal epochs, and the coordinator's ≥ rule accepts them.

**Evidence**:
- Code, removal without an epoch bump:
  - `onGroupEmpty`, fired when this broker's session cache drops the group's last session (SSC:259 → SPM:937-950);
  - init failure (SPM:794-806);
  - fencing by GroupIdNotFound or UnknownTopicOrPartition (SPM:813-824);
  - share-version toggle (SPM:626-636).
- Code, the recreated instance reuses the same leader epoch (SPM:761).
- Code, `removeSharePartitionFromCache` removes by key (SPM:843). A late error callback from the old instance can therefore evict and fence the healthy new one (SPM:360-365, 438-443, 801).
- Code, the combiner lets a higher-dc AVAILABLE beat ACKNOWLEDGED, and ARCHIVING beat ACKNOWLEDGED at equal dc (PSBC:37-44).
- Historical: 08e287870 (KAFKA-20672, ARCHIVING not finished after re-init), f771a1e9b (wrong instance compared), 3eb0b8c1b and 8fe1157ac. Each patched one re-initialization site; none fences the old instance.
- Found independently by 3 analyses (archaeology S7, manager F8/F9, persist F1).

**Affected code paths**: `SPM.removeSharePartitionFromCache`, `fencedSharePartitionHandler`, `ShareGroupListenerImpl.onGroupEmpty`, `SP.acquisitionLockTimeoutHandler` (SP:2946-2999), `SP.writeShareGroupState` (SP:2842), `SP.close/markFenced`, `SCS.writeState` validation (SCS:754-809).

**Suggested modeling approach**:
- Variables: a set of instance records `inst[i] = [status ∈ {ACTIVE,FENCED}, lEpoch, sEpoch, start, rec[o]]`, and `current` (the instance the SPM cache maps to).
- Actions: `RemoveInstance(i, bumpEpoch ∈ BOOLEAN)`. A fenced instance keeps `LockExpire`, `WriteComplete` and `AckBegin` enabled (`acknowledge` has no state check). Add `ErrorCallbackRemoveByKey`.
- Granularity: instance creation (`CreateInstance` → `SendRead` → `CoordRead` → `RecvRead`) is split, because writes can interleave with every step.

**Priority**: High.
**Rationale**: a direct path to "acknowledged then redelivered" and "acked record goes to DLQ". Three analyses agree, and no defense exists apart from epoch equality.

### Scenario 2: The coordinator's fence and incarnation state is not durable or monotone (PQ5)

**Mechanism**: on replay, the fence values `leaderEpochMap` and `stateEpochMap` are rebuilt as the max over *retained* records (SCS:288-297). Records written after an init or tombstone carry a lower epoch, so pruning plus failover lowers the fence. Delete and init are not idempotent and carry no leader epoch.

**Evidence**:
- Code, the leader-epoch fence can be lost:
  1. `initializeState` writes a snapshot with the request's leaderEpoch (default 0) (SCS:733-738, `ShareGroupOffset.fromRequest`).
  2. `writeState` keeps the stored epoch (SCS:669).
  3. An equal-epoch `readState` writes nothing (SCS:394).
  4. Pruning drops older records (the offsets manager keeps only the latest snapshot per key).
  5. After failover, the fence for that key falls to 0.
- Code, the tombstone clears both epoch maps (SCS:251-255). A recreated group restarts at a low stateEpoch, so late writes from the old incarnation pass both fences. A write before re-init gets INVALID_REQUEST, which is surfaced as an unknown error and does not evict the instance (SP:2890-2905).
- Code: `initializeState` treats a duplicate as a no-op only if startOffset also matches (SCS:576-580). A retried or late init after progress therefore writes an empty snapshot.
- Code: `deleteState` (SCS:505) has no epoch at all; its validator checks none (SCS:902-933).
- Historical:
  - 1df8219b3: added the duplicate-init check. The fix is partial.
  - cde0d5687 (KAFKA-20743) and 6665a1950: epoch sentinel handling went back and forth.
  - be72089de: alter-offsets bumps the epoch.
  - 5dd015386 (KAFKA-19927): timeouts are now retried, so duplicates are the normal case.

**Affected code paths**: `SCS.replay`/`handleShareSnapshot|Update` (SCS:227-300), `readStateAndMaybeUpdateLeaderEpoch` (SCS:357-413), `initializeState` (SCS:553-583), `deleteState`, `writeState` validation, `ShareCoordinatorOffsetsManager`, and the `CoordinatorRuntime` failover replay.

**Suggested modeling approach**:
- Variables: `clog` (a sequence of records `[kind ∈ {Snap,Upd,Tomb}, lEpoch, sEpoch, start, batches]`), `hwm`, `prunedTo`, and the coordinator's in-memory state defined as `Replay(SubSeq(clog, prunedTo+1, hwm))`.
- Actions:
  - `CoordWrite`, `CoordRead`, `CoordInit` and `CoordDelete`, each guarded by the real check order;
  - `Commit`;
  - `FailAppend` (truncate uncommitted);
  - `TimeoutResp` (error response, record still commits);
  - `Prune`;
  - `Failover` (in-memory := Replay(retained)).
- Network: requests can be duplicated and delayed.

**Priority**: High.
**Rationale**: the fence is the only protection against zombie leaders (Scenario 1). Losing it after an ordinary prune and failover means two leaders are accepted.

### Scenario 3: In-memory-first transitions and ambiguous write outcomes (PQ1, PQ3)

**Mechanism**: some transitions commit in memory before the write and ignore its failure (timeout, DLQ phase 2, LSO archiving, rollback after expiry). Ack/release roll back on a failed write that may actually have been applied. Durable and in-memory state then diverge in both directions.

**Evidence**:
- Code:
  - The timeout handler commits under the lock and sends a write whose failure it ignores (SP:2950-2999).
  - Rollback after expiry yields AVAILABLE or ARCHIVED, writes nothing and skips ARCHIVING/DLQ (IFS:228-234).
  - The LSO move writes nothing (SP:1293-1295).
  - REQUEST_TIMED_OUT maps to a retriable error. Once retries run out, ack/release roll back (SP:2556-2575) even though the coordinator may hold ACKNOWLEDGED.
- Code, coalesced results: two writes for the same key in one batched request both receive the *last* partition result (`partitionFut.put` at SCSvc:549; PSM:215-229; batching with no per-key de-duplication at PSM:383-392). A rejected write can therefore be reported as success.
- Historical:
  - 55764f837 (KAFKA-19859): start offset regression on ack after an LSO move.
  - 71299c229 (KAFKA-19972): delivery-count direction on session release.
  - cdc9eb2c3: the DLQ ARCHIVING path, which rollback does not follow.
  - 93d0db451: the same duplicate-key-in-batch pattern, fixed for DLQ only.

**Affected code paths**: `SP.acknowledge`/`releaseAcquiredRecords` → `rollbackOrProcessStateUpdates` (SP:2480-2600); `acquisitionLockTimeoutHandler`; `IFS.completeStateTransition`; `PSM` batching and retry; `SCSvc.writeState`.

**Suggested modeling approach**:
- Variables: `pending[o]` (rollback state), `expired[t]`, and `msgs` (a bag of write messages with payload and an attempt flag).
- Actions:
  - Timer firing is split into `TimerFire` (sets expired, no lock) and `TimerHandle` (under the lock).
  - `WriteComplete(ok | fail | ambiguous)`. On ambiguous, the record is applied at the coordinator while the broker sees failure.
  - `CoalesceResp`, so two messages for the same key share one outcome.

**Priority**: High.
**Rationale**: this is the core of PQ3. Divergence here also feeds the unwritten-delivery precondition of Scenario 1.

### Scenario 4: Batch → per-offset split loses the pending transition and leaks timers (PQ1, PQ4)

**Mechanism**: `InFlightBatch.maybeInitializeOffsetStateUpdate` (IFB:225-249) rebuilds per-offset `InFlightState` from (state, dc, member, timer) only. It drops the rollback state of a pending transition and re-arms timers per offset. `archiveRecords` (SP:1422-1462) is the one caller that splits without first excluding batches in an ongoing transition.

**Evidence**:
- Code (verified): the callers are the stale-batch archive in acquire (SP:1337), the LSO move (SP:1358) and the aborted-transaction archive (SP:3181).
- Consequences:
  - The write callback commits or rolls back a detached object, so the persister's view and the cache disagree.
  - The split offsets look non-pending and are re-acquirable while their original transition is still unresolved.
  - The timeout handler does not check that the firing task is the record's current task or that the owner matches (SP:2946-2986). A leaked task can therefore release a record now held by another member early, so two members can hold it.
- Historical: dfa28c463, 43062a0ee, c205e6910, 18dbd5227 (KAFKA-19954: overlapping batches after re-init caused double acquisition, critical), and 25aae9903 (KAFKA-19808: batch misalignment left records ACQUIRED with no timer). Batch versus per-offset handling has been fixed one site at a time.

**Suggested modeling approach**: represent a batch as `split ∈ BOOLEAN` with batch-level fields that are copied on split. Model `Split` faithfully: copy state, dc, owner and timer; clear pending. Keep task identity (`task[o] ∈ TaskIds`, `taskAlive[t]`) so that stale-task firing is expressible.

**Priority**: Medium-High.
**Rationale**: concrete and verified code mechanism that leads to double delivery. Reachability needs an LSO move or aborted transactions overlapping a pending write, which a model can explore.

### Scenario 5: Start-offset, LSO and gap-window cursor drift (liveness and contract)

**Mechanism**: `startOffset`, `endOffset`, the gap window `persisterReadResultGapWindow` and the cached batches can sit below the start offset after an LSO move. Each fix patched one site (four historical fixes).

**Evidence**:
- Code (verified):
  - The all-acknowledged branch sets `startOffset = lastCachedOffset+1` without the `> startOffset` guard that 55764f837 added to the other branches (SP:2645-2652). Example: acquire 0-4, LSO moves to 10, the lock expires, and startOffset regresses from 10 to 5.
  - The gap window is never adjusted on an LSO move (SP:643-653, 1247-1298). `nextFetchOffset` can then stay below the LSO, `updateCacheAndOffsets` returns early (SP:1252), and the fetch livelocks.
  - `canMoveStartOffset` can hit an NPE on a per-offset floor batch that ends before startOffset (SP:2740-2747). The NPE escapes write callbacks and skips DLQ phase 2.
  - Rollback after expiry yields AVAILABLE below startOffset (IFS:228).
- Historical: 55764f837, 18dbd5227, 25aae9903, 8fe1157ac.

**Suggested modeling approach**: abstract offsets to `0..N` with `lso` and the gap window as variables, and add `LsoAdvance`. Most items are deterministic and are routed to tests (§6.2). The model keeps only the `StartMonotone` and `NextFetchGeStart` invariants and the liveness property `ARCHIVING ~> ARCHIVED`.

**Priority**: Medium. Mostly test-verifiable.

### Scenario 6: Session lifecycle versus acquired records (contract and delivery-count burn)

**Mechanism**: release-on-disconnect is a non-atomic look-up-then-remove by session key, not by connection. Several post-acquire paths drop acquired records from the response without releasing them.

**Evidence**:
- Code, the disconnect race:
  - The stale flag is read outside the lock, and the lookup and release are separate steps (SSC:137-159, 282-296; SPM:408-422).
  - A reconnect in between with INITIAL_EPOCH creates a new session S2 with the same key. The release then tears down S2 and calls `releaseAcquiredRecords(memberId)`, releasing records just handed to S2.
  - Result: two consumers hold the same record concurrently, since S2's client still has the records.
- Code, records acquired then dropped from the response (each burns a delivery attempt, and with max attempts the record can be archived without ever being delivered):
  - throttled response (KafkaApis:4317-4335);
  - session epoch mismatch (ShareSessionContext:187-201);
  - exception partway through `processFetchResponse` (ShareFetchUtils:64-125, ShareFetch:195);
  - `addErroneousToResponse` overwriting a partition that has data (ShareFetch:224).
- Historical: 70be719f6 (KAFKA-19950) and c85a8c17a. Both fixes are partial.

**Suggested modeling approach**: `Disconnect` is split into `DisconnectLookup` and `DisconnectRelease`; add `NewSession(member)`. `Respond(fetch) ∈ {Delivered, Dropped}` folds all drop paths into one nondeterministic outcome.

**Priority**: Medium. The disconnect race is model-checkable; the drop paths go to tests.

## 3. Modeling Recommendations

### 3.1 Model
- **Durable versus in-memory split with a lossy, reordering, duplicating persister channel and ambiguous outcomes.** Why: Scenarios 1 and 3. How: a `msgs` bag; `CoordApply` uses the real join (max dc, then max stateId; max start).
- **Multiple SharePartition instances, including fenced ones that stay live.** Why: Scenario 1. How: `inst` records plus `current`; `RemoveInstance(bumpEpoch)`.
- **The coordinator log with snapshot/update/tombstone, prune and failover-by-replay.** Why: Scenario 2. How: `clog`; in-memory state = Replay(retained). The snapshot-every-N counter is not modeled, because ReplayEq holds for any snapshot placement (deep-coordinator Q2).
- **Two-step timer (fire/handle), three-step ack/release, and fire-and-forget timeout writes.** Why: Scenarios 3 and 4.
- **Batch split with task identity.** Why: Scenario 4.
- **Leader change through epoch bumps.** `RemoveInstance(TRUE)` plus `CreateInstance` with lEpoch+1. Group reset through `CoordInit` with sEpoch+1.

### 3.2 Do Not Model
- **Share group membership/assignment, the ShareConsumer client, fetch bytes, `PartitionMaxBytesStrategy`, remote fetch and partition rotation.** Out of scope per guidance; abstracted as nondeterministic `Acquire(member)` and `Respond`.
- **DLQ produce internals.** ARCHIVING → ARCHIVED is a single nondeterministic step (it may fail, or be skipped).
- **Transactions and record filtering.** Represented only as an optional nondeterministic `ArchiveAbortedAcquired` that triggers a split.
- **Metrics, `deliveryCompleteCount` and lag.** They do not affect delivery; DCC findings go to code review.
- **Purgatory wake-ups and the fetch lock.** Waits are bounded by maxWait; there is no safety impact (deep-manager Q5).
- **The snapshot-every-N counter and cold-snapshot timer.** Efficiency only.
- **Byte-level offsets, gap-window arithmetic and NPE paths.** Deterministic, so they go to tests (§6.2).

## 4. Proposed Extensions

| Extension | Variables | Purpose | Scenario |
|---|---|---|---|
| Instance versions | `inst`, `current`, `nextInst` | Zombie instance acting after removal | 1 |
| Persister channel | `msgs` (bag: inst, lEpoch, sEpoch, start, batches, kind, attempt) | Reorder, duplicate, retry, ambiguous outcome, coalesced response | 1, 3 |
| Coordinator log | `clog`, `hwm`, `prunedTo`, `cMem` (derived) | Fence rebuilt by replay; prune; failover | 2 |
| Group incarnation | `cStateEpoch`, `CoordInit`, `CoordDelete` | Delete/re-create and init idempotence | 2 |
| Pending transitions | `rec[o].pending`, `rec[o].prev` | Three-step ack/release; rollback after expiry | 3 |
| Timer identity | `rec[o].task`, `taskAlive`, `taskExpired` | Fire/handle split; leaked tasks after a split | 3, 4 |
| Batch split | `split`, batch-level fields | Pending state lost on split | 4 |
| LSO | `lso`, `start`, `endOff` | Archiving below the LSO; start monotonicity | 5 |
| Sessions | `sess[m]`, `conn` | Disconnect lookup/release race | 6 |
| Client view | `holds[m]` (offset → delivery id), `ackedOK` | Contract checks from the consumer's point of view | all |

## 5. Proposed Invariants

| Invariant | Type | Description | Targets |
|---|---|---|---|
| NoConcurrentDelivery | Safety | No offset is in `holds[m1]` and `holds[m2]` with both locks live (m1 ≠ m2, or two sessions of one member) | Scenarios 4 and 6, PQ1 |
| AckedNeverRedelivered | Safety | If an ack for offset o returned success to the client, no later Acquire hands o out (checked across instances) | Scenarios 1 and 3, PQ2 |
| DurableAckNeverRegresses | Safety | Once the durable state for o is ACKNOWLEDGED/ARCHIVED within a stateEpoch, it never becomes AVAILABLE/ACQUIRED/ARCHIVING | Scenarios 1 and 2 |
| AckedNotDLQed | Safety | An offset acked OK never reaches ARCHIVING | Scenario 1 |
| LeaderFence | Safety | At most one instance with lEpoch < cLeaderEpoch_max ever has a write accepted after a higher-epoch read was accepted | Scenario 2, PQ5 |
| IncarnationIsolation | Safety | No write sent under an earlier group incarnation is applied after CoordDelete/CoordInit with a new sEpoch | Scenario 2 |
| ReplayEq | Safety | `cMem` = Replay(retained committed log) after any Failover (expected to hold; sanity check) | Scenario 2 |
| OneOfTimeoutOrAck | Safety | For each delivery id, at most one of {lock-timeout release, ack commit} takes effect | PQ1 |
| DcOncePerDelivery | Safety | In-memory dc increases by exactly 1 per Acquire and never otherwise (within an instance) | PQ1 |
| NoArchiveWhileHeld | Safety | No ARCHIVED/ARCHIVING record has a live lock held by a member | PQ4 |
| TaskConsistency | Safety | Every live task belongs to an ACQUIRED record and is that record's current task | Scenario 4 |
| StartMonotone, NextFetchGeStart | Safety | Start offset never decreases (in memory or durable); next fetch ≥ start | Scenario 5 |
| EventuallyResolved | Liveness | Every ACQUIRED/ARCHIVING record eventually reaches ACKNOWLEDGED/AVAILABLE/ARCHIVED (weak fairness on timers and callbacks) | PQ1, Scenario 5 |
| InMemoryNotBehindDurable | Diagnostic | Expected to fail (Scenario 3); use to classify, not as a bug oracle | Scenario 3 |

## 6. Findings Pending Verification

### 6.1 Model-Checkable

| ID | Question | Expected violation | Scenario |
|---|---|---|---|
| MC-1 | Can a fenced instance (removed with no epoch bump, e.g. `onGroupEmpty` or a GroupIdNotFound fence) have a delayed or retried write or a timer-driven write accepted after the replacement instance committed ACKNOWLEDGED, so the acked record is redelivered or sent to the DLQ? | AckedNeverRedelivered, DurableAckNeverRegresses, AckedNotDLQed | 1 |
| MC-2 | Can a late error callback from the old instance evict and fence the new one by key, and does that composition widen MC-1? | AckedNeverRedelivered | 1 |
| MC-3 | After an offset reset (init with sEpoch+1), an equal-epoch read, prune and failover, does the leader-epoch fence fall so that an ex-leader with a lower epoch is accepted alongside the current leader? | LeaderFence, NoConcurrentDelivery | 2 |
| MC-4 | After delete and re-create (the tombstone clears both epoch maps), can a late write from the old incarnation merge into the new group's state? Can a retried `initializeState` with the same sEpoch, sent after progress, wipe accepted acks? | IncarnationIsolation, DurableAckNeverRegresses | 2 |
| MC-5 | When a write's outcome is ambiguous (applied, but the broker sees failure and rolls back), or two writes for one key are coalesced in one batch, does the in-memory state diverge permanently from durable state? Can a later lock timeout or init then redeliver an acked record or skip a non-terminal one? | AckedNeverRedelivered, EventuallyResolved | 3 |
| MC-6 | Can splitting a batch with a pending release or ack (via the LSO move or the aborted-transaction archive) leave a detached rollback and a leaked timer, which later releases a record now held by another member? | NoConcurrentDelivery, TaskConsistency | 4 |
| MC-7 | Can a disconnect lookup/release race with a reconnect release the new session's freshly acquired records while that client still holds them? | NoConcurrentDelivery | 6 |
| MC-8 | Within a single live instance (no zombies), can timer expiry, ack, release and rollback after expiry ever let both the timeout and the ack take effect, or double-increment dc? The code reading says no; this is a sanity baseline for PQ1. | OneOfTimeoutOrAck, DcOncePerDelivery | PQ1 |

### 6.2 Test-Verifiable

| ID | Description | Suggested test approach |
|---|---|---|
| T-1 | `startOffset` regresses in the all-acknowledged branch (SP:2645-2652) | SharePartitionTest: acquire 0-4, `updateCacheAndOffsets(10)`, expire the lock with MockTimer, assert startOffset ≥ 10 |
| T-2 | The gap window is not adjusted on an LSO move, so `nextFetchOffset` stays below the LSO and fetches livelock (SP:643-653, 1252) | Init with persisted start 0 and batch [10-19], then `updateCacheAndOffsets(5)`; assert `nextFetchOffset() ≥ 5` |
| T-3 | NPE in `canMoveStartOffset` with a per-offset floor batch ending before startOffset (SP:2740-2747) | LSO moves into a gap after a partial ack; then an ack completes |
| T-4 | A split during a pending write drops the rollback state (IFB:225-249 via SP:1441-1451) | Release under a delayed persister future, LSO move into the batch, fail the write; assert the offsets are not acquirable or are consistent |
| T-5 | Rollback after expiry yields ARCHIVED, skips the DLQ, and yields AVAILABLE below start (IFS:228-234) | Pending ack, lock expires, write fails, DLQ enabled |
| T-6 | Two writes for the same key in one batched persister request both get the last result (SCSvc:549, PSM:215-229) | PersisterStateManagerTest / ShareCoordinatorServiceTest with duplicate partition entries and differing outcomes |
| T-7 | Throttled ShareFetch drops acquired records (KafkaApis:4317-4335) | KafkaApisTest with a quota mock that throttles; assert the records are released or delivered |
| T-8 | Exception partway through `processFetchResponse` leaves earlier partitions ACQUIRED (ShareFetchUtils:64-125, ShareFetch:195) | DelayedShareFetchTest with 2 partitions, the second acquire throwing |
| T-9 | A session epoch mismatch (two fetches in flight from one member) drops acquired records (ShareSessionContext:187-201) | Two concurrent ShareFetch requests on the same session |

### 6.3 Code-Review-Only

| ID | Description | Suggested action |
|---|---|---|
| CR-1 | `deleteState` has no epoch; `initializeState` checks only stateEpoch; `readStateSummary` has no init or epoch check (SCS:429, 505, 867-933, 935-960) | Ask whether delete/summary need fencing |
| CR-2 | `close()`/`markFenced()` do not cancel acquisition timers; timeout, ack and release ignore `partitionState` (SP:1129, 1589, 1010, 2946) | Cancel timers on close; gate mutators on ACTIVE |
| CR-3 | `removeSharePartitionFromCache` removes by key, not by instance (SPM:843) | Use `remove(key, instance)` |
| CR-4 | The fetch-lock release clears a lock held by another fetch on owner mismatch (SP:1569-1576) | Do not clear on mismatch |
| CR-5 | `deliveryCompleteCount` updates are asymmetric across commit and rollback (SP:2487 vs 2532/2571) | Fix the accounting |
| CR-6 | `RecordState.validateTransition` allows ARCHIVING → ACQUIRED/AVAILABLE/ACKNOWLEDGED (RecordState:57-66); `DeliveryCountOps.DECREASE` is dead (IFS:163) | Tighten the validator; remove dead code |
| CR-7 | Stale comments: SCS:81 (writeState updates leaderEpoch), SCS:315/652; javadoc refers to a nonexistent `ShareSessionCache.tryEvict` | Update the docs |
| CR-8 | `acquire` checks the state outside the lock and does not re-check inside it (SP:760 vs 792) | Re-check under the lock |
| CR-9 | A lowered dynamic max-delivery-count is not enforced at acquire (SP:962, 2029) | Clarify the intent |
| CR-10 | The group coordinator's init retry sends start 0 instead of -1 (97bbffa0a), ignoring `share.auto.offset.reset` | Confirm with the group-coordinator owners |

## 7. Reference Pointers

- Analysis report: `analysis-report.md`. Per-area notes:
  - `analysis/archaeology-sharepartition.md` (48 commits)
  - `analysis/archaeology-coordinator-persister.md` (95 commits)
  - `analysis/deep-state-machine.md` (action map)
  - `analysis/deep-persist-init.md` (durable/in-memory TLA+ sketch)
  - `analysis/deep-coordinator.md` (runtime contract, replay)
  - `analysis/deep-manager-fetch.md` (session/fetch actions)
- Key source:
  - SP:420-600 (maybeInitialize), 749-1000 (acquire), 1010-1125 (ack/release), 1247-1300 (LSO), 1422-1462 (archiveRecords), 2480-2700 (rollback/commit, start-offset move), 2842-2910 (write and error mapping), 2946-3100 (timeout handler);
  - IFS:150-244 and IFB:225-249;
  - SCS:227-300 (replay), 357-413 (read with epoch bump), 553-583 (init), 640-720 (record generation), 754-809 (write validation);
  - PSBC:37-44;
  - PSM:95-97, 383-392, 899-940, 1629, 1712-1744.
- Historical fixes, used as reference only (closed and not modeling targets): KAFKA-19954 (18dbd5227), KAFKA-19859 (55764f837), KAFKA-19808 (25aae9903), KAFKA-20672 (08e287870), KAFKA-20505 (e1cfea338), KAFKA-19953 (dfa28c463), KAFKA-19972 (71299c229), KAFKA-19950 (70be719f6), KAFKA-20743 (cde0d5687), KAFKA-19927 (5dd015386), 1df8219b3, 46b4636d6 (sweep-line combiner), 93d0db451.
- Existing harness to reuse: `core/src/test/java/kafka/server/share/SharePartitionTest.java` (mock persister, MockTime/MockTimer) and `share-coordinator/src/test/.../ShareCoordinatorShardTest.java`.
- Limitation: Apache JIRA (issues.apache.org) was blocked by the network policy and the GitHub API scope excludes apache/kafka. Issue discussions were taken from squash-merge commit messages only.
