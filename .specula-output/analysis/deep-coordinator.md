# Deep analysis: share coordinator (KIP-932) durable state

Source revision: kafka @ 8aa57006f. Paths below are relative to
`share-coordinator/src/main/java/org/apache/kafka/coordinator/share/` unless they say otherwise.
`CR` = `coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java`.

## 0. Runtime contract (what the coordinator can rely on)

| Property | Evidence | Holds? |
|---|---|---|
| All ops on one share-partition key go to one `__share_group_state` partition | `ShareCoordinatorService.partitionFor` 282-288 (hash of `asCoordinatorKey()` = group+topicId+partition), used by `topicPartitionFor` 1171; every op (write/read/summary/delete/init/prune/cold-snapshot) uses `runtime.scheduleWriteOperation` (523, 656, 765, 874, 967, 369) or `scheduleWriteAllOperation` (439, 1159) | Yes |
| Ops on one TP are serialized | `CoordinatorWriteEvent.key()` = tp (CR 1309); `EventAccumulator` keeps one in-flight event per key (`EventAccumulator.java` 36, 170, 189) | Yes |
| The in-memory effect of a write appears at once (before commit) | `append` calls `coordinator.replay(currentBatch.nextOffset, ...)` before the append (CR 1008-1016). Shard state = timeline maps | Yes |
| Responses are released only after commit | records go to `currentBatch.deferredEvents` / `deferredEventQueue.add(offset, …)`, completed at the HWM (CR 738, 1830-1831) | Yes |
| Read-only results (empty records) wait for pending writes | `records.isEmpty()` -> `waitForPendingWrites` (CR 919-922, 880-890) | Yes: a read or a rejection that depends on uncommitted state is not released before that state commits, and fails if it is reverted |
| A failed append reverts the in-memory state | `failCurrentBatch` -> `revertLastWrittenOffset(baseOffset)` + fail the deferred events (CR 786-791). Unload -> `failAll(NOT_COORDINATOR)` (CR 625-626) | Yes |
| Replay offset = real log offset | the writer's returned offset is checked against `nextOffset`; a mismatch -> FAILED+reload (CR 719-736) | Yes |
| Write timeout | `withWriteTimeout(shareCoordinatorWriteTimeoutMs)` (Service 240), `OperationTimeout` (CR 1335-1341). On timeout the future fails but **the record can still commit** (ambiguous outcome). `CoordinatorOperationExceptionHelper` 65 maps REQUEST_TIMED_OUT -> COORDINATOR_NOT_AVAILABLE, which the persister **retries** (`server-common/.../PersisterStateManager.java` 900-931, also on client REQUEST_TIMED_OUT/NETWORK_EXCEPTION) | Duplicates and delayed duplicates of write/read/init requests are expected |

Answer to Q4 (guidance Assumption): **verified**. Per-key serialization holds. No read path skips the write
path. `readState` and `readStateSummary` are scheduled as write ops (Service 651-656, 765), and their
empty-record results wait for pending writes to commit. The only data read outside an event is
`lastPrunedOffsets` (a best-effort cache, Service 384-404).

## 1. writeState (Q1)

Validation order in `maybeGetWriteStateError` (Shard 754-809):
1. topicId null, partition < 0, leaderEpoch < 0, stateEpoch < 0 -> INVALID_REQUEST (766-780)
2. key not in `shareStateMap` -> INVALID_REQUEST "uninitialized" (784-786)
3. `leaderEpochMap[key] > req.leaderEpoch` -> FENCED_LEADER_EPOCH (788-791)
4. `stateEpochMap[key] > req.stateEpoch` -> FENCED_STATE_EPOCH (792-795)
5. no metadata image, or topic/partition missing -> UNKNOWN_TOPIC_OR_PARTITION (796-806)

Then `generateShareStateRecord(partitionData, key, updateLeaderEpoch=false)` (336, 661-714):
- `newLeaderEpoch = currentState.leaderEpoch()` (669). **writeState never raises the leader epoch**, even
  when `req.leaderEpoch > map` (the comment at 81-82 is stale). Only `readStateAndMaybeUpdateLeaderEpoch` raises it.
- `newStartOffset = max(req.startOffset, cur.startOffset)` (670). `deliveryCompleteCount` (dcc) = max when the start offsets are equal, cur when
  req.start < cur.start, req when req.start > cur.start (674-682).
- stateEpoch in the record = `currentState.stateEpoch()` (695). A write with a *higher* stateEpoch is accepted but does
  not advance it (comment 652-653 "or stateEpoch is highest seen so far => snapshot" is not implemented).
- Snapshot if `snapshotUpdateCount >= limit` (684); else Update with only the request's batches
  (`mergeBatches(List.of(), …)` 711).

Answers:
- A stale write with a lower leaderEpoch or an older stateEpoch is rejected (strict `>`). `==` is accepted for both. Leader
  epochs are unique per partition in KRaft, so `==` from two *different* brokers cannot happen unless the
  coordinator has lost the fence value (see F1).
- Within the same epochs, reordered or duplicated writes are accepted. This is safe because the merge is a join (see §2): it is
  commutative, associative and idempotent over (count, state) per offset, with max startOffset and dcc chosen by startOffset.
- `readStateAndMaybeUpdateLeaderEpoch` (357-415): fences `map > req` (846-849). If `map == req` it returns with no
  records (394-396). Otherwise it writes a record (Snapshot or Update, picked by the counter) with
  `leaderEpoch=req` (405-413). So the bump is **durable** (a real record). The response waits for commit, and a failed append
  reverts the map and fails the read. The bump is not lost on failover *by itself*, but see F1: the fence value
  `leaderEpochMap` and the durable `shareStateMap.leaderEpoch` can diverge, and the optimisation at 394
  compares against the map and not against the durable value.

## 2. Snapshot / Update and replay (Q2)

Replay (Shard 227-299):
- Snapshot: `leaderEpochMap`, `stateEpochMap` = max(old, record) (257-258, 287-299); `shareStateMap := record`
  (262); counter reset to 0 **only if** the counter is `>= limit` (264-268); `offsetsManager.updateState(key, offset)` (271).
- Tombstone (null snapshot value): removes all four maps (249-255) and the key from the offsets manager.
- Update: `leaderEpochMap` max (276); `shareStateMap := merge(cur, upd)` (283, 1030-1050), or `fromRecord(upd)`
  if absent (stateEpoch 0, which cannot be reached because writeState requires the key); counter++ (284).

`PersisterStateBatchCombiner` (whole file): prune batches with `lastOffset < start`, clip those straddling `start`
(91-104), then sweep-line where each offset takes the **max priority** over all covering batches, ordered by
`(deliveryCount desc, deliveryState desc)` (44-50, 128-153). Adjacent equal ranges are coalesced. It is **not**
last-writer-wins. Per offset it is a join on the lexicographic lattice (count, state), with AVAILABLE=0 <
ACQUIRED=1 < ACKNOWLEDGED=2 < ARCHIVING=3 < ARCHIVED=4 (`server/.../fetch/RecordState.java` 27-31).

Equivalence of write-time and replay-time state: **holds**.
- There is only one code path. The runtime replays the generated record to build the in-memory state (CR 1010), and the same
  `replay` is used at load time. Responses to writeState carry no state. readState serves `shareStateMap`.
- Snapshot record = `combine(cur.batches ∪ req.batches, newStart)` (696). Update record = `combine(req.batches, newStart)`
  (711), and its replay = `combine(cur.batches ∪ that, newStart)` (1043-1046). Pruning and the max-join commute, so both
  give the same batches. startOffset, dcc and leaderEpoch match as well. Only the timestamps, snapshotEpoch and stateEpoch source differ, and those are
  intended.
- `merge` uses `record.startOffset` (never -1 in practice, since it is computed with max at write time) and `record.dcc`
  verbatim. A replay after failover sees the same log sequence, so the same base.

`snapshotUpdateCount`:
- It is not persisted. It is rebuilt by replay from the retained log (after pruning, only records past the prune
  point). It resets only when a snapshot is replayed while `count >= limit`. Cold snapshots (612-620), init snapshots and
  below-limit snapshots do not reset it. So a snapshot can come "early" or "late", and after failover the
  counter differs from the pre-failover one.
- **This affects efficiency only.** An Update record holds a delta that is merged onto *whatever* is the current state
  in log order. Its correctness does not depend on the counter, and pruning keeps each key's latest snapshot
  (see §3), so the base is the same before and after failover. No update can be applied relative to a
  different base. **Priority Q5b (compaction drops an update): not possible at the coordinator level.**

## 3. initializeState / deleteState / pruning (Q3)

initializeState (553-584, 935-975): fences only on `stateEpochMap > req.stateEpoch` (956-959). No-op if
`stateEpoch ==` and `cur.startOffset == req.startOffset` (576-580). Otherwise it writes a **fresh snapshot**
(`fromRequest`: empty batches, **leaderEpoch = 0** (`ShareGroupOffset.UNINITIALIZED_EPOCH`), dcc -1/0,
snapshotEpoch+1) (724-740, `ShareGroupOffset.java` fromRequest(Initialize…)).

deleteState (505-539, 902-933): **no epoch check at all** (the request has no epochs). Missing key -> success, no
record. Otherwise a tombstone, whose replay removes `stateEpochMap` and `leaderEpochMap` too. After that, writeState and readState get
INVALID_REQUEST (uninitialized), which the broker maps to `UnknownServerException` (`core/.../SharePartition.java` 2890-2904).
That is **not** treated as fencing by `SharePartitionManager.fencedSharePartitionHandler` (813-824).

Pruning (`ShareCoordinatorOffsetsManager`; Service 340-405): `updateState` is called **only for snapshot and tombstone
records** (Shard 271), so `offsets[key]` = the offset of the key's latest snapshot. `lastRedundantOffset` = min over live keys
(`findRedundantOffset` 87-118). Tombstoned keys are dropped (79-81). Prune = `deleteRecords(tp, off)` deletes `< off`.
Each key's latest snapshot is at `>= off` and its later updates are above it, so the replay base is kept.
The value is timeline-backed (reverted with the batch), and the prune op waits for pending writes (empty records). **No
case was found where pruning deletes a record needed to rebuild `shareStateMap`.** However, pruning *does* delete
records needed to rebuild `leaderEpochMap` (F1), because the fence state is a max over *all* history while the
offsets manager only reasons about `shareStateMap`.

## 4. Findings

### F1. Leader-epoch fence lost after initializeState with a higher stateEpoch + pruning + coordinator failover
**Model-checkable. Confidence: medium-high (code path verified; consequence needs a cross-component check).**

1. A key has `leaderEpochMap = shareStateMap.leaderEpoch = 7` (leader B2 at epoch 7 read, which bumped it).
2. Alter offsets (`GroupMetadataManager` 9575-9642: groupEpoch+1) -> initializeState(stateEpoch+1). The replayed snapshot has
   `leaderEpoch=0`. `maybeUpdateLeaderEpochMap` keeps the map at 7 (max, Shard 287-292), but `shareStateMap.leaderEpoch = 0`.
3. B2's SharePartition is fenced (FENCED_STATE_EPOCH) and re-initializes: readState(leaderEpoch 7). `map == 7` -> **no record** (394-396).
4. All later writeState records carry `leaderEpoch = currentState.leaderEpoch() = 0` (669). Regular and cold snapshots also carry 0
   (694, 616). **No durable record after the init snapshot holds 7.**
5. Later snapshots of all keys move `lastRedundantOffset` past the old records that held epoch 7. The prune job (every 5 min) deletes
   them.
6. The share coordinator fails over and replays the log: `leaderEpochMap[key] = 0`.
7. A zombie, ex-leader B1 (epoch 6, stale metadata) creates a SharePartition. Its readState(6) passes (`0 > 6` false), and a record with epoch 6 is
   written. The live leader B2 (7) is also accepted (`6 > 7` false). **Two leaders are accepted by the coordinator.** That can cause concurrent
   delivery of the same offsets, and writes from both are joined. This contradicts "epoch fencing rejects stale writes" (PQ5).
Also visible without failover: `readStateSummary` reports `shareStateMap.leaderEpoch` = 0 (472) while the fence is at 7.
Root cause: there are two sources of truth for the leader epoch (`leaderEpochMap` = max over history, which is not durable as such,
and `shareStateMap.leaderEpoch`, which is durable), and the `==` short-circuit is checked against the non-durable one.
Tests: none cover init-over-existing-key followed by read and failover (`ShareCoordinatorShardTest` 1417-1510, 1719-1800).
Fix idea: compare against `shareStateMap.get(key).leaderEpoch()` at 394, or carry `max(leaderEpochMap, …)` into init
snapshots and into the records that writeState generates.

### F2. Delete, then re-create a group with a lower stateEpoch: a late writeState from the old incarnation is accepted
**Model-checkable (cross-component). Confidence: medium.**
The tombstone clears `stateEpochMap` and `leaderEpochMap` (252-255). A new group with the same id restarts groupEpoch (initialize
uses `groupEpoch`, `GroupMetadataManager` 3272-3290), so `S_new` can be `<= S_old`. The init snapshot puts the leaderEpoch at 0.
A writeState from the old incarnation then passes both fences (788-795) and is merged: startOffset = max(old, new) (670),
plus the old batches. Possible sources of such a write:
(a) a persister retry or delayed duplicate (retries on timeout, `PersisterStateManager` 900-931);
(b) an old SharePartition still cached on a non-GC broker. A write rejected as "uninitialized" during the gap
maps to UnknownServerException, which does not evict the partition (SharePartitionManager 813-824). Its later lock-timeout or ack writes
carry `S_old`, and the new group's fetches on that broker may reuse the cached SharePartition.
Effect: the new group inherits the old start offset, acknowledged states and counts. This conflicts with the new group's init offset and with `share.auto.offset.reset`.
Check needed: whether deleting a share group evicts SharePartitions on all brokers (out of scope here).

### F3. initializeState is not idempotent once the partition has progressed; a delayed init wipes the batches
**Model-checkable (with the GC init-retry). Confidence: low-medium.**
The duplicate check compares `req.startOffset` with the *current* startOffset (576-577), which SharePartition writes have advanced.
A delayed or retried init (persister retry; GC re-sends "initializing" partitions after `shareGroupInitializeRetryIntervalMs`
= 30 s with startOffset 0 for already-known topics, `GroupMetadataManager` 3189-3290) arriving after progress writes a
fresh snapshot with empty batches:
- If the stateEpoch is equal, the SharePartition is **not fenced**. The coordinator loses acknowledged/archived batches that are not in later writes, so after a leader change
  acknowledged records are re-delivered and delivery counts reset (PQ2 "acknowledged, then redelivered").
- If the stateEpoch is higher, the SharePartition is fenced and re-reads a startOffset of 0 or -1, which resets the position.
The GC gates assignment on `initializedTopics` (3141-3146), which narrows the window.

### F4. The max-join combiner diverges from SharePartition after an ambiguous write plus rollback
**Model-checkable. Confidence: medium as a divergence, low as a contract violation.**
SharePartition rolls back on a write error (`SharePartition.java` 2560-2575), but a timed-out coordinator write can still commit
(§0). Durable state = ACKNOWLEDGED(c). SharePartition goes back to ACQUIRED, then on lock timeout to AVAILABLE(c), which it writes. The combiner keeps
ACKNOWLEDGED(c) because state 2 > 0 at an equal count. The two diverge until a delivery at c+1 is persisted. After a leader change at that point,
the record is never redelivered, even though the consumer was told the acknowledgement failed. This is relevant to PQ3. The combiner's "max wins" is only
equivalent to "latest wins" because SharePartition's persisted (count, state) per offset is monotone. `DeliveryCountOps.DECREASE` exists
(`InFlightState.java` 249) but is unused today. If it were used, the coordinator would silently ignore the decrease. RENEW is not persisted (2352-2358).

### F5. Behaviour of snapshotUpdateCount (efficiency only)
**Code-review-only. Confidence: high.** The counter is not reset by cold, init or below-limit snapshots (264-268). It is rebuilt only from
the retained log after failover. Correctness is unaffected (§2).

### F6. Sibling-path inconsistencies (Q5)
**Code-review-only (F1-F3 are the model-checkable consequences).**
- writeState: leader epoch + state epoch fences. readState: leader epoch only (and it can *raise* the epoch). readStateSummary: no init
  check (it returns UNINITIALIZED for a missing key, 447-455) and no fences. deleteState: no fences, and a missing key is a success.
  initializeState: state epoch only (`>`). Equal epochs with a different start offset overwrite.
- The fence uses `leaderEpochMap`, while summary and records use `shareStateMap.leaderEpoch` (F1).
- writeState checks "uninitialized" before the fences. initializeState checks the state epoch before metadata. Every path checks metadata last,
  so a stale request on a deleted topic gets FENCED_* rather than UNKNOWN_TOPIC (harmless: both are treated as fenced by SharePartitionManager).
- FENCED_LEADER_EPOCH and FENCED_STATE_EPOCH both map to NotLeaderOrFollowerException at the broker (SharePartition 2899).
- Stale docs: Shard 81-82 (a write can update leaderEpochMap: false), 315-316, 652-653 (snapshot on a state-epoch increase: not
  implemented).

## 5. TLA+ sketch

```
CONSTANTS Keys, Leaders, MaxEpoch, MaxOff, Limit
VARIABLES
  log,          \* Seq of records <<type ∈ {"snap","upd","tomb"}, key, le, se, start, dcc, batches>>; prefix = committed
  hwm,          \* committed length
  pruneFrom,    \* first retained index (deleteRecords)
  \* in-memory shard state = Replay(SubSeq(log, pruneFrom, Len(log)))  -- define, don't store
  coordUp,      \* coordinator loaded?
  net           \* in-flight requests (bag), allow dup / reorder / delayed delivery

Join(b1, b2)  == per-offset max on (count, state); Prune(bs, start)
ApplyRec(s, r) ==
  CASE r.type="snap" -> [s EXCEPT !.sm[r.key] = r, !.lem[r.key] = Max(s.lem[r.key], r.le),
                                  !.sem[r.key] = Max(...), !.cnt[r.key] = IF s.cnt[r.key] >= Limit THEN 0 ELSE @]
    [] r.type="upd"  -> [sm[k].batches = Prune(Join(sm[k].batches, r.batches), r.start), start = r.start,
                         dcc = r.dcc, le = r.le; lem = Max; cnt+1]
    [] r.type="tomb" -> remove k from sm, lem, sem, cnt
Replay(seq) == FoldLeft(ApplyRec, Init, seq)

\* Actions (each atomically appends at most one record; the response is held until hwm passes it)
Write(req)  == /\ k ∈ DOMAIN st.sm  /\ ~(st.lem[k] > req.le) /\ ~(st.sem[k] > req.se)
               /\ append(IF st.cnt[k] >= Limit THEN snap(Join(st.sm[k].batches, req.batches), le |-> st.sm[k].le)
                                              ELSE upd(req.batches, le |-> st.sm[k].le))
Read(req)   == /\ k ∈ DOMAIN st.sm /\ ~(st.lem[k] > req.le)
               /\ IF st.lem[k] = req.le THEN respond ELSE append(record with le |-> req.le)
Init(req)   == /\ ~(st.sem[k] > req.se) /\ ~(same se /\ same start) /\ append(snap(le |-> 0, batches |-> {}))
Delete(k)   == append(tomb)
Commit / FailAppend (truncate uncommitted suffix = timeline revert) / Timeout (respond error, record stays)
Prune       == pruneFrom' = MinOverLiveKeys(index of latest snap)    \* mirrors OffsetsManager
Failover    == coordUp' = TRUE ; state rebuilt = Replay(retained committed log)
```
Invariants:
- `LeaderFence`: for each k, once a Read with epoch e is acknowledged, no later Read or Write with epoch < e is accepted
  (F1 should violate it with Init + Prune + Failover).
- `AcceptedReflected`: every acknowledged Write's batches are ≤ (join order) the state in `Replay(retained log)`, *within the same
  stateEpoch incarnation* (expected to hold; F3 breaks it across Init with an equal stateEpoch).
- `ReplayEq`: the in-memory state after each action = Replay(retained committed log) once quiescent (expected to hold, a check of §2).
- `IncarnationIsolation`: after Delete+Init, no write issued before the Delete is accepted (F2).
Keep the state space small: 1 key, 2 leaders, epochs ≤ 3, offsets 0..2, Limit = 1 (to force snapshot/update alternation),
1 extra key only when exercising Prune.
