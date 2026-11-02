# Deep analysis: SharePartition durable-state path (persist / initialize / fencing)

Target: Apache Kafka share groups (KIP-932), checkout `8aa57006f`.
Scope: broker-side `SharePartition` <-> `PersisterStateManager` <-> `ShareCoordinatorShard`.
Guidance: `guidance/kafka-share-groups.md`, PQ2, PQ3, PQ5 and the logStartOffset item.

Abbreviations used for paths:
- SP = `core/src/main/java/kafka/server/share/SharePartition.java`
- SPM = `core/src/main/java/kafka/server/share/SharePartitionManager.java`
- SFU = `core/src/main/java/kafka/server/share/ShareFetchUtils.java`
- IFS = `server/src/main/java/org/apache/kafka/server/share/fetch/InFlightState.java`
- PSM = `server-common/src/main/java/org/apache/kafka/server/share/persister/PersisterStateManager.java`
- SCS = `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorShard.java`
- SCSvc = `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/ShareCoordinatorService.java`
- PSBC = `share-coordinator/src/main/java/org/apache/kafka/coordinator/share/PersisterStateBatchCombiner.java`
- CR = `coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java`

Name changes from the prompt: `isWriteShareGroupStateSuccessful` does not exist in this checkout. Write
success is decided inside `writeShareGroupState` (SP:2842-2889). `initialReadGapOffset` is now
`persisterReadResultGapWindow`, of type `GapWindow` (SP:288).

---

## 1. Mechanism summary (ground truth)

### 1.1 Initialization (`maybeInitialize`, SP:420-588)
- The state goes EMPTY -> INITIALIZING under the write lock (`emptyToInitialState`, SP:1665-1674). Only one
  initializer runs. Other callers get `LeaderNotAvailableException` while the state is INITIALIZING or FENCED
  (SP:1677-1690).
- `persister.readState` is sent with `(partition, leaderEpoch)`. `leaderEpoch` is final and is taken from
  `metadataProvider.leaderEpoch()` when SPM creates the instance (SPM:758-790, SP:169).
- On the coordinator, the read runs as a **write operation** (SCSvc:651-659,
  `readStateAndMaybeUpdateLeaderEpoch` SCS:357-414):
  - If the epoch is higher, it appends a ShareUpdate/Snapshot record carrying the new leaderEpoch. This bumps
    `leaderEpochMap`, which fences older writers.
  - If the epoch is equal, no record is written, but the response is deferred until all pending writes are
    committed (`waitForPendingWrites`, CR:878-889).
  - So a read observes every write the coordinator committed before it.
- Building the cache (SP:492-560):
  - `startOffset = persisted startOffset`, or the auto-reset value if it is -1 (SP:3135-3150).
  - `stateEpoch` comes from the response.
  - Every persisted batch becomes an `InFlightBatch` with `memberId=""`, the persisted state and the persisted
    deliveryCount (dc).
  - A batch below startOffset, or with last < first, causes FAILED.
  - The first hole (an offset >= startOffset that no batch covers) becomes
    `GapWindow(endOffset, gapStartOffset)`. Holes are refetched and acquired as fresh records with dc=0.
  - `maybeUpdateCachedStateAndOffsets()` runs immediately, so terminal prefixes are dropped.
  - ARCHIVING batches are collected, and the DLQ phase 2 is resumed after activation (SP:533-541, 3351-3365).
- **ACQUIRED is never persisted**. `writeShareGroupState` is called only from:
  - ack/release (`rollbackOrProcessStateUpdates`, SP:2548)
  - lock timeout (SP:2992)
  - DLQ phase 2 (SP:3402)

  Acquire increments dc in memory only. So a new leader sees every record that was in flight on the old
  leader as AVAILABLE (persisted batch) or as a gap (dc=0). The dc of the unpersisted delivery is lost.

### 1.2 Write path, ack/release (`acknowledge` SP:1010-1073, `releaseAcquiredRecords` SP:1082-1124)
- Under the write lock, each affected batch or offset gets `startStateTransition(newState, NO_OP dc)`
  (IFS:203-210). This **mutates `state` in place** and saves a `RollbackState`. `hasOngoingStateTransition()`
  stays true until `completeStateTransition`.
- `deliveryCompleteCount` is incremented eagerly (SP:2405, 2487).
- After the lock is released, `rollbackOrProcessStateUpdates` (SP:2512-2621) sends **one writeState with only
  the changed batches (a delta)**. The write also carries `stateEpoch`, `startOffset()` (read under the read
  lock *at send time*), `deliveryCompleteCount()` (at send time) and `leaderEpoch` (SP:2842-2849).
- On success: `completeStateTransition(true)`, then `maybeUpdateCachedStateAndOffsets()`. **The start offset
  moves only after the write that made the prefix terminal has succeeded.** The new start offset is persisted
  lazily by the *next* write.
- On failure:
  - `completeStateTransition(false)` restores ACQUIRED/member, or AVAILABLE/ARCHIVED if the lock expired
    while the write was in flight (IFS:219-244). The restored state is not written to the coordinator.
  - The start offset and cachedState are untouched, because they were never advanced.
  - Rollback of the in-memory state is complete.
- Readers that must not see uncommitted state check `hasOngoingStateTransition`:
  - `canMoveStartOffset` (SP:2711-2722)
  - `findLastOffsetAcknowledgedAndMetadata` (SP:2791, 2798)
  - `nextFetchOffset` (SP:681, 687)
  - `tryUpdateState` refuses (IFS:153). This blocks acquire, ack and timeout on records that are in
    transition.
- The timeout handler skips records in transition because their `state` is no longer ACQUIRED
  (SP:3020, 3089). This is confirmed by `SharePartitionTest.testAcquisitionLockTimeoutWithWriteStateRPCFailure`
  (SharePartitionTest:10129-10197).

### 1.3 Write path, acquisition-lock timeout (SP:2945-3013) and DLQ phase 2 (SP:3376-3414)
- **These paths do not use the transition protocol.** Under the lock, `tryUpdateState` sets the state directly
  (ACQUIRED -> AVAILABLE, or ARCHIVED/ARCHIVING at max dc, or ARCHIVED below startOffset) (SP:3020-3029,
  3093-3098).
- They then send the write and **ignore its failure**: "Even if write share group state RPC call fails, we will
  still go ahead with the state transition" (SP:2997-2999), followed by `maybeUpdateCachedStateAndOffsets()`.
- DLQ phase 2 does the same: "On failure, ARCHIVED stays in memory, the persister catches up when start
  offset advances" (SP:3399-3400, 3406-3410).

### 1.4 LSO movement (`updateCacheAndOffsets`, SP:1247-1305)
- This is triggered only lazily, by OFFSET_OUT_OF_RANGE on a share fetch (SFU:84-88).
- Only AVAILABLE records below the new LSO are archived (`archive()` sets `isTerminalState`, so a later
  rollback keeps ARCHIVED, IFS:183-188, 220).
- `startOffset := LSO` in memory. **Nothing is written.** The comment says the new startOffset is written
  "lazily during acknowledge/release" (SP:1299-1301).
- ACQUIRED records below the LSO stay ACQUIRED until the lock times out. At timeout they go to ARCHIVED,
  because `lastOffset < startOffset` (SP:3022, 3094).
- Acks for offsets below startOffset are **silently skipped and reported as success**, with nothing written
  (SP:1035-1039, 2318-2320).
- While such an ACQUIRED batch heads cachedState, `findLastOffsetAcknowledgedAndMetadata` returns -1 and the
  start offset cannot be recomputed. Progress stalls for at most one lock duration. The bound is real
  (timer-driven), so this is not stuck forever.

### 1.5 Fencing and replacement (SPM)
- `removeSharePartitionFromCache` (SPM:837-851) calls `markFenced()` (SP:1589-1596), which only sets
  `partitionState = FENCED`, removes the listener and calls `close()`. `close()` only deregisters metrics
  (SP:1129-1136).
- **Acquisition-lock timers, in-flight persister futures and DLQ callbacks of the removed instance keep
  running and writing.**
- `acquire` and `maybeAcquireFetchLock` check `stateNotActive()` (SP:760, 1548).
- **`acknowledge`, `releaseAcquiredRecords` and the timeout handler do not check the state.** A request that
  obtained the reference before removal can still ack on the old instance (SPM:357, 432).
- Removal triggers:

  | Trigger | Where | Changes leaderEpoch? |
  |---|---|---|
  | `onBecomingFollower` / `onFailed` / `onDeleted` listener | SPM:896-923 | yes, when leadership moves |
  | Write/read errors NotLeaderOrFollower (from FENCED_LEADER_EPOCH **and** FENCED_STATE_EPOCH, SP:2900), GroupIdNotFound, UnknownTopicOrPartition | SPM:813-823 | **no** |
  | init failure | SPM:794-806 | **no** |
  | `onGroupEmpty` | SPM:937-950 | **no** |
  | share-version toggle | SPM:626-636 | **no** |

- The next fetch recreates the instance with `metadataProvider.leaderEpoch()` (SPM:758-761). That is the
  **same** leader epoch unless leadership changed.

### 1.6 Coordinator write semantics (SCS)
- Fencing: rejected iff `leaderEpochMap[key] > req.leaderEpoch` (FENCED_LEADER_EPOCH) or
  `stateEpochMap[key] > req.stateEpoch` (FENCED_STATE_EPOCH) (SCS:788-795). **An equal epoch is accepted.**
- `newStartOffset = max(req.startOffset, cur.startOffset)` (SCS:670). The start offset never regresses.
- Batches are merged with `PersisterStateBatchCombiner` (SCS:740-751, 1030-1048). Batches below startOffset
  are pruned or clipped (PSBC prune).
- **On overlap the winner is decided by priority, not by arrival order**: higher deliveryCount wins, and on
  equal dc the higher state id wins (`PRIORITY_DESC`, PSBC:37-44). State ids are AVAILABLE=0, ACQUIRED=1,
  ACKNOWLEDGED=2, ARCHIVING=3, ARCHIVED=4.
- So the merge is a join on the lattice `(dc, stateId)`. It is commutative and idempotent.

### 1.7 PersisterStateManager ordering and retries
- Writes are batchable. After coordinator lookup, a handler is appended to
  `nodeRPCMap[node][WRITE][groupId]` (PSM:383-394).
- At most one WRITE request is in flight per (RPCType, node), via the `inFlight` gate (PSM:1629, 1636-1660).
  Everything that queued meanwhile is coalesced into one request, **including several entries for the same
  share partition** (PSM:1712-1741).
- Retries:

  | Error | Treatment |
  |---|---|
  | COORDINATOR_NOT_AVAILABLE, COORDINATOR_LOAD_IN_PROGRESS, NOT_COORDINATOR, UNKNOWN_TOPIC_OR_PARTITION (partition level) | retried |
  | NETWORK_EXCEPTION, REQUEST_TIMED_OUT (client level) | retried |
  | anything else, including FENCED_LEADER_EPOCH, FENCED_STATE_EPOCH, GROUP_ID_NOT_FOUND | surfaced |

  - A retry resets the coordinator node and re-enqueues **the same handler, with the same payload and the
    same startOffset**, through a timer with that handler's own exponential backoff (1 s base, 30 s max,
    jitter, 5 attempts) (PSM:95-97, 899-940, 1677-1690).
- **There is no per-key sequencing.** A retried write W1 re-enters `nodeRPCMap` after its backoff, which can
  be after a younger W2 for the same key has been sent and applied.
- Duplicate entries in one request: SCSvc schedules one write op per entry, in request order. The ops are
  serialized on the shard. The response map, however, is keyed by partition (`partitionFut.put`,
  SCSvc:520-548), so **only the last entry's result is returned**. The client side `PartitionResultIndex` is
  also last-wins (PSM:200-236). Every handler of that partition receives the same (last) result.

---

## 2. Answers to the questions

### Q1 / PQ2: leader change while records are ACQUIRED
- **The new leader cannot miss a committed ack.** N's read is a write op. Either it is ordered after O's
  write W (then N sees W, deferred until commit, CR:884), or W is ordered after the read and fails with
  FENCED_LEADER_EPOCH.
- On a fenced write, O handles it as follows:
  - SP:2900 maps the error to `NotLeaderOrFollowerException`.
  - `rollbackOrProcessStateUpdates` rolls the in-memory state back (SP:2556-2575).
  - SPM's `fencedSharePartitionHandler` removes O (SPM:813-823).
  - The client sees an ack error, which is acceptable (at-least-once).
- **In-flight records come back as AVAILABLE or gap**, because ACQUIRED is never persisted. They are never
  non-AVAILABLE, except ARCHIVING (DLQ phase 1 was persisted), which N re-drives (SP:533-541).
- dc of the in-flight delivery is lost, so a record can be delivered more than `maxDeliveryCount` times across
  failovers. By design, not a contract bug.
- **The old instance can keep acting after replacement:**
  - acks that raced with removal (acknowledge has no state check)
  - acquisition-lock timer expiries
  - DLQ phase 2
  - completions of in-flight writes

  These writes are fenced **only if the coordinator's leaderEpoch/stateEpoch has moved past O's**. Fencing
  starts only at N's first `readState`, and N is lazily created on the first share fetch. Before that, O's
  writes are accepted, which is safe because N reads afterwards.
- **Same-epoch replacement is not fenced.** See finding F1.

### Q2 / PQ3: write failure or delay
- **Ack/release path**:
  - The new state is visible in `state` immediately, but every consumer that matters filters on
    `hasOngoingStateTransition`.
  - The start offset and cachedState removal happen only after success.
  - Rollback on failure is complete for the state.
  - **However, failure is ambiguous** (F3). A timed-out or disconnected attempt may have been applied, and the
    final error then rolls back a transition that *is* durable.
- **Timeout path, DLQ phase 2, LSO movement, rollback-after-expiry** (F2): in-memory state is advanced before
  durability, or without ever writing.
  - A later successful write does not carry the failed transition's batches, because writes are deltas.
  - It does carry the **advanced startOffset**. That durably "archives" everything below by pruning
    (SCS:670, PSBC prune).
  - So divergence above startOffset persists until the next write moves startOffset past it. Records that
    were ARCHIVED in memory (max delivery reached) may come back AVAILABLE after failover.
  - Consequences are over-delivery and dc under-count. They are not lost or acked-then-redelivered records.

### Q3: write contents
- Each write is a **delta**: only the batches changed by this operation (SP:2548, 2992, 3402).
- `startOffset` is the in-memory value at send time. In-memory startOffset moves only past committed terminal
  records (ack path), past records terminal through the unpersisted timeout or DLQ paths, or to the LSO.
- It cannot skip a non-terminal in-memory record above the LSO. It can skip records whose terminal state was
  never made durable (F2).
- Because SCS takes `max`, a stale write cannot move the durable start offset backward.
- **Merging is order-independent** (PSBC priority). Within one SharePartition instance, the persisted
  `(dc, state)` sequence for an offset is monotone in that lattice:
  - acquire bumps dc and is not persisted
  - release or timeout gives AVAILABLE at the same dc
  - accept or reject gives a terminal state at the same dc
  - ARCHIVING goes to ARCHIVED, and terminal states never leave

  So reordered or retried deltas from **one instance** merge to the same result. This is the compensating
  mechanism for Q5.

### Q4: logStartOffset passes ACQUIRED records
See section 1.4.
- The records stay ACQUIRED with their timers.
- Acks are silently treated as success with no write.
- On lock expiry they go to ARCHIVED, persisted as part of the timeout write, then pruned by startOffset.
- Start offset recomputation stalls for at most one lock duration.
- The durable startOffset lags the LSO until some later write. If a failover happens first, N starts below the
  LSO, gets OFFSET_OUT_OF_RANGE and catches up.
- No correctness issue found. The tests `testLsoMovementThenAcquisitionLockTimeoutThenAcknowledge*`
  (SharePartitionTest:7553-7660) and `testLsoMovementWithPendingAcknowledgements*` (11669-11800) encode
  this intent.

### Q5: retry ordering
- **Yes, an older write can be applied after a newer one for the same key.**
  - Retry backoff is per handler (PSM:925).
  - The `inFlight` gate is per node, not per key.
  - A first attempt that the coordinator applied but whose response timed out can be followed by its own
    retry after a newer write.
  - Same-key entries in one coalesced request are applied in list order, which is insertion order into
    `nodeRPCMap`, not creation order.
- Within one instance this is harmless because of the lattice merge and `max(startOffset)` (Q3).
- It becomes harmful only when (a) two instances with **equal epochs** write the same key (F1), or (b) results
  get mixed by duplicate-key coalescing (F4).

---

## 3. Findings

### F1. A replaced SharePartition with the same leader epoch is not fenced; its late writes can override the new instance's committed acks
- **Classification:** Model-checkable. Confidence that the mechanism exists: high. Likelihood in production:
  low to medium.
- **Mechanism:**
  1. A removal that keeps the leader epoch: `onGroupEmpty` SPM:937-950, init failure SPM:794-806,
     GroupIdNotFound / UnknownTopicOrPartition in SPM:813-823, share-version toggle SPM:626-636.
  2. The new instance N is created with the same `leaderEpoch` (SPM:761).
  3. O's timers, DLQ phase 2 and racing acks still call `writeShareGroupState` with the same leaderEpoch and
     stateEpoch (SP:1589-1596 does not stop them; the timeout handler SP:2945 has no state check).
  4. The coordinator accepts equal epochs (SCS:788-795).
  5. The merge is by `(dc, state)` priority (PSBC:37-44). A late O write `AVAILABLE dc=k+1` overrides N's
     committed `ACKNOWLEDGED dc=k`.
- **This requires O's dc to be ahead of N's.** That happens if O made a delivery that was never persisted
  before N read, for example O's timeout write failed (ignored, SP:2997) and O re-acquired the record.
- **Trace (same leader epoch e):**
  1. Durable: offset 5 `AVAILABLE dc=1`.
  2. O acquires (dc=2). The lock expires. The timeout write fails, but in memory the record is `AVAILABLE dc=2`.
  3. O re-acquires (dc=3). The member leaves. `releaseSession` produces write `Wr = AVAILABLE dc=3`, which is
     delayed (COORDINATOR_LOAD_IN_PROGRESS, backoff).
  4. The group becomes empty and O is removed.
  5. A new member fetches. N is created with epoch e, reads `AVAILABLE dc=1`, acquires (dc=2), the consumer
     ACCEPTs, and `ACK dc=2` is committed. In N's memory the record is terminal and the start offset may pass it.
  6. Wr is applied (equal epoch accepted). The merge gives `AVAILABLE dc=3`.
  7. If N fails over or is re-created before a write carries startOffset past offset 5, the next instance
     redelivers offset 5, which was already acknowledged. **This violates "acked then redelivered".**
- A variant with a smaller effect: O's late `ARCHIVING dc=k` at the same dc beats N's `ACKNOWLEDGED dc=k`
  (3 > 2), so an acknowledged record is sent to the DLQ on resume (SP:533-541).
- **Also relevant:** two writes with equal epochs from O and N in the same coalesced request get each other's
  result (F4).
- **Check for compensating mechanisms:**
  - stateEpoch fences only after a reset.
  - leaderEpoch fences only on leadership change.
  - Nothing in SPM cancels O's timers.
  - `SharePartitionTest.testMaybeInitializeFencedSharePartition` (1170) covers only init on a fenced instance.
  - No test covers timers of a removed instance.
- **TLA+:** model `instances` as a set of `(id, leaderEpoch, stateEpoch)`. Allow `Remove(i)` without an epoch
  bump, `Create` with the current epoch, and keep timer and write actions enabled for removed instances.
  Invariant: `AckedNeverRedelivered`, meaning every offset whose ack response was success stays terminal in
  durable state, or is below the durable startOffset.

### F2. The timeout, DLQ phase-2 and LSO paths advance in-memory state with no durable acceptance (by design)
- **Classification:** Test-verifiable for the divergence. Code-review for severity. Confidence high.
- **Code:**
  - Timeout path: SP:2965-3000. `tryUpdateState` is applied immediately, write failure is logged, and
    `maybeUpdateCachedStateAndOffsets()` still runs (SP:2999).
  - DLQ phase 2: SP:3390-3410.
  - LSO path: SP:1247-1305, nothing written.
  - Rollback on expiry: IFS:228-235. AVAILABLE or ARCHIVED is never written.
- **Effect:**
  - In memory a record is ARCHIVED (max dc) or AVAILABLE, while durable is older. The start offset can pass
    the ARCHIVED records.
  - The next successful write carries that startOffset, so the coordinator prunes them (consistent in the end).
  - If a failover happens before the next write, the records are re-delivered with a lower dc than O had.
    This exceeds `maxDeliveryCount` and contradicts the guidance's "delivery counts lost" goal, not the
    "acked then redelivered" goal.
- **Design intent:** the comment at SP:2997 and tests `testAcquisitionLockTimeoutWithWriteStateRPCFailure`
  (10129), `testRecordArchivedWithWriteStateRPCFailure` (10200),
  `testAcquisitionLockTimeoutWithDlqEnabledWriteFailure*` (13333, 13398).
- **Expected severity:** low. This is an accepted divergence, and it is the enabling precondition for F1.

### F3. An ambiguous write outcome rolls back in memory a transition that may be durable
- **Classification:** Test-verifiable (PSM unit test plus a mock coordinator). Confidence high. Severity low.
- **Mechanism:**
  - PSM retries REQUEST_TIMED_OUT, NETWORK_EXCEPTION and NOT_COORDINATOR with the same payload (PSM:899-940).
  - An earlier attempt may have been appended, or a coordinator failover may have kept the record.
  - When retries are exhausted, `requestErrorResponse` (PSM:943-947) causes SP to roll back (SP:2556-2575).
- **Result:** durable `ACKNOWLEDGED/ARCHIVED dc=k`, while in memory the record is ACQUIRED (or AVAILABLE on
  lock expiry).
  - This leader may redeliver a durably acked record (the client saw an ack error, so at-least-once holds).
  - A later release writes `AVAILABLE dc=k+1`, which overrides it (dc wins).
  - Divergence clears once the record is re-acked, but in the window a failover makes the record vanish
    (durable terminal) while the client believes its ack failed. For ACCEPT that is benign. For RELEASE the
    durable state is already AVAILABLE.
- **Not a contract violation** under at-least-once. Worth one model action, `WriteAmbiguous`: the
  coordinator applies the write but the response is an error.

### F4. Several writes for one share partition coalesced into one request receive a single (last) result
- **Classification:** Test-verifiable. Confidence high for the mechanism, low for impact.
- **Code:** PSM:1712-1741 puts every queued handler's PartitionData in the request, including duplicates.
  SCSvc:520-548 runs each entry but `partitionFut.put(partition, future)` keeps only the last. PSM:226-229
  also keeps only the last. Each handler gets the last entry's outcome.
- **Impact:** low for one instance, because both entries share epochs and the append batch, so their outcomes
  normally agree. It is real for the O/N equal-epoch case (F1) or after a stateEpoch bump:
  - W_O (old stateEpoch) gets FENCED_STATE_EPOCH, W_N succeeds, and both are in one request.
  - O commits its ack, and the client is told success, although the write was rejected. The ack was durably
    lost but reported as success.
  - The reverse order rolls back a durable write.
- **Note:** the duplicate-key collapse also existed before KAFKA-20803 (SCSvc map). The old client used
  `findFirst` on the response, which held only one entry anyway.
- **TLA+:** model coalescing as a nondeterministic batching of the pending write set. Response = outcome of
  the last write per key.

### F5. deliveryCompleteCount bookkeeping is asymmetric (metric only)
- **Classification:** Code-review-only. Confidence medium. Not delivery-relevant.
- **Code:**
  - `acknowledgeCompleteBatch` adds the full batch size (SP:2487), while rollback subtracts only the part
    above startOffset (SP:2532, 2571, `numInFlightRecordsInBatch` SP:2620-2625).
  - Writes carry the eager (uncommitted) count (SP:2849).
  - The coordinator chooses a value with max/keep logic (SCS:672-681).
- **Impact:** lag reporting only. Out of scope for the TLA+ model except as a ghost counter.

### Non-findings, verified
- **Durable startOffset regression:** prevented by SCS:670.
- **Stale write from the old leader after the new leader's read:** fenced by leaderEpoch (SCS:788), and the
  read is committed before it responds (CR:884).
- **Two ongoing transitions on one record:** prevented (IFS:153). Timeout during a transition is a no-op
  (SP:3020, 3089).
- **Ack writes reordered within one instance:** commutative merge (PSBC:37-44). Batches ack disjoint
  offsets, because an ongoing transition blocks overlap.
- **Init batch below startOffset:** impossible, because the coordinator prunes it (PSBC prune). SP would fail
  init anyway (SP:503-509).

---

## 4. TLA+ modeling sketch

### Constants
`Offsets = 0..N-1`, `MaxDc`, `Instances` (ids), `Members`, `MaxRetries`.

### Durable (coordinator) variables
- `dStart`: durable start offset.
- `dBatches`: function `Offsets -> [dc, st]` or ⊥ (gap).
- `cLeaderEpoch`, `cStateEpoch`: values of `leaderEpochMap` and `stateEpochMap`.
- `committed`: optional. Model commit as atomic with apply, plus separate actions for the ambiguous response.

### Broker variables (per instance i)
- `inst[i].status` ∈ {EMPTY, INIT, ACTIVE, FENCED, FAILED}
- `inst[i].le`, `inst[i].se`
- `inst[i].start`, `inst[i].rec[o] = [st, dc, owner, pending(rollback | none), terminalFlag]`
- `inst[i].lockExpired[o]`
- `cache`: the instance id currently in `partitionCache` (SPM)
- `leaderEpoch`: the broker metadata epoch
- `lso`

### Network and persister variables
- `pendingW`: a bag of write messages
  `[id, inst, le, se, start, batches: Offsets -> [dc, st], attempts, kind ∈ {ACK, TIMEOUT, DLQ}]`
- `respQ`: response messages `[wid, result ∈ {OK, FENCED, RETRIABLE, AMBIGUOUS_OK_APPLIED}]`
- `readQ`: read requests and responses `[inst, le]`

### Actions (each a separate step, because each runs under a separate lock acquisition or callback)
1. `CreateInstance(i)`: SPM `computeIfAbsent`, `le := leaderEpoch`.
2. `SendRead(i)`: EMPTY -> INIT, enqueue read.
3. `CoordRead(m)`: atomically fence (`cLeaderEpoch := max`), then snapshot `dStart` and `dBatches` into the
   response.
4. `RecvRead(i)`: build `rec` from the snapshot. Gaps get dc=0 and AVAILABLE. Set ACTIVE.
5. `Acquire(i, m, o)`: ACTIVE, st=AVAILABLE and not pending. Set ACQUIRED, dc+1, owner=m. No write.
6. `AckStart(i, m, o, t)`: owner check. `st := t`, `pending := old`, emit ACK write with `start := inst.start`
   **at send time** (send can be a separate step, `SendWrite`, to capture SP:2548 running outside the lock).
7. `LockExpire(i, o)`:
   - if ACQUIRED and not pending: set AVAILABLE, or ARCHIVED at MaxDc or below start; emit TIMEOUT write;
     then the in-memory state is final.
   - if pending: set `lockExpired` only.
8. `CoordApply(w)`: fencing check `cLeaderEpoch > w.le ∨ cStateEpoch > w.se` gives FENCED. Otherwise
   `dStart := max(dStart, w.start)`, `dBatches := Join(dBatches, w.batches)` pruned below `dStart`.
   Nondeterministic response: OK, or AMBIGUOUS (applied but the response says timeout).
9. `CoordReject(w)`: RETRIABLE, not applied (NOT_COORDINATOR or load in progress).
10. `PersisterRetry(w)`: attempts < MaxRetries. Re-enqueue the **same** payload in any order relative to
    others. This models per-handler backoff and the absence of per-key ordering.
11. `RecvWriteResp(i, w)`:
    - ACK kind: commit (clear pending) or rollback (restore; AVAILABLE or ARCHIVED if `lockExpired`).
    - Then `AdvanceStart(i)`.
    - TIMEOUT or DLQ kind: ignore the error, then `AdvanceStart(i)`.
12. `AdvanceStart(i)`: move start past a terminal, non-pending prefix.
13. `LsoAdvance(x)`, `UpdateCacheAndOffsets(i)`: archive AVAILABLE records below x, `start := x`, no write.
14. `Remove(i, bumpEpoch ∈ BOOLEAN)`: FENCED, `cache := none`. **Timers and pending writes of i stay enabled.**
    With `bumpEpoch`, also `leaderEpoch' = leaderEpoch + 1`, which models leadership moving.
15. `ResetStateEpoch`: `initializeState` sets `cStateEpoch + 1`, models admin offset reset (optional).
16. `CoalesceResp`: optional, for F4. Pick two pending writes with the same key and deliver the outcome of the
    later one to both.

### Invariants and properties
- `AckedNeverRedelivered`: for every offset in `ackOk` (a ghost set of acks reported OK to a client) that is
  ≥ `dStart`, `dBatches[o].st ∈ {ACKNOWLEDGED, ARCHIVED}`. Also, no ACTIVE instance has the offset
  AVAILABLE or ACQUIRED unless a later `ackOk` was removed by a release.
- `NoDoubleOwner`: at most one *cached* ACTIVE instance has `o` ACQUIRED. Across instances this is expected to
  fail by design (old consumer, new leader), so restrict it to the cached instance.
- `DurableStartMonotone`: `dStart' ≥ dStart`.
- `InMemoryNotBehindDurable` (expected to fail, documents F2/F3): the in-memory terminal set above `start`
  matches the durable terminal set. Use it as a diagnostic, not as a safety property.
- `DcMonotonePerInstance` (lemma backing Q5): the sequence of `(dc, st)` written for o by a single instance is
  non-decreasing in the PSBC order.
- `EventuallyDrained` (liveness, weak fairness on LockExpire and RecvWriteResp): every ACQUIRED record
  eventually becomes non-ACQUIRED. Bounded stall under LSO, section 1.4.

### Suggested small configuration
- N=2 offsets, MaxDc=2, 2 instances, 2 members, MaxRetries=1.
- `Remove` with `bumpEpoch=FALSE` enabled. This is where F1 should show a counterexample in about 12 to 15
  steps.
- `Remove` with `bumpEpoch=TRUE` should keep `AckedNeverRedelivered`. This validates PQ2.

---

## 5. Recommended next steps
- **F1:** write a reproduction in `SharePartitionManagerTest` or `SharePartitionTest` with a mock persister
  that applies writes to a real `ShareCoordinatorShard` and combiner. The scenario: two `SharePartition`
  instances with the same leaderEpoch, a failed timeout write on O, `onGroupEmpty` removal, N's read, ack and
  commit, then O's delayed release write. Assert that the durable state of the acked offset is AVAILABLE.
- **F4:** add a PSM test with two `WriteStateHandler`s for the same key in one coalesced request and a mixed
  per-entry outcome.
- **Ask upstream whether the following are intentional:**
  - equal-epoch acceptance (SCS:788) together with timers of removed instances that are never cancelled
    (SP:1129, SP:2945)
  - `acknowledge` not checking `partitionState`
