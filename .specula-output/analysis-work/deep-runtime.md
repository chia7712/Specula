# Deep analysis: CoordinatorRuntime (coordinator-common), as used by the KIP-848 group coordinator

Source: `kafka/` trunk @ 1224f3d4b. Read-only analysis. Abbreviations:
CR = `coordinator-common/.../runtime/CoordinatorRuntime.java`, LI = `CoordinatorLoaderImpl.java`,
SC = `SnapshottableCoordinator.java`, TI = `CoordinatorTimerImpl.java`, EI = `CoordinatorExecutorImpl.java`,
EA = `EventAccumulator.java`, MT = `MultiThreadedEventProcessor.java`, DQ = `server-common/.../deferred/DeferredEventQueue.java`,
GMM = `group-coordinator/.../GroupMetadataManager.java`, GCS = `GroupCoordinatorShard.java`,
CPW = `core/.../coordinator/group/CoordinatorPartitionWriter.scala`.

## 0. Findings at a glance

| # | Finding | Sev. | Status | Class |
|---|---------|------|--------|-------|
| F1 | A stale `CompleteLoad` callback is applied to a **re-created** context (unload+reload during a load): the new context goes ACTIVE before its own load finishes, or gets stuck in FAILED | High | candidate, no compensating mechanism found | Model-checkable + Test-verifiable |
| F2 | A stale ACTIVE context (unload still queued) can **append in a later leadership tenure**. `append` has no leader-epoch fence, the out-of-sync check runs only after the write, and one path skips it entirely | Medium | candidate, timing-dependent | Model-checkable |
| F3 | HWM advances between the loader's last HWM read and listener registration are never delivered, so empty-record writes (steady heartbeats) hang until the next write or the write timeout | Low (liveness) | candidate | Test-verifiable |
| F4 | Non-timeline side effects of `generate` (timer schedule/cancel, executor tasks) are not rolled back when a batch is reverted without an unload | Low | design hazard | Model-checkable (with a FlushFail-without-unload action) |
| F5 | Non-atomic ops (`cleanupGroupMetadata`, `onTopicsDeleted`) may leave a partial tail across a failover | Info | by design | Test-verifiable |
| F6 | A stale HWM event (old listener) is applied via `coordinators.get(tp)` to a new context | Info | benign: the value is committed, and a backwards value throws and is logged | Code-review-only |
| F7 | Old-incarnation timer and executor callbacks are bound to `tp`, not to the context (CR:496-509) | Info | protected by `cancelAll` plus identity checks; keep as an invariant | Model-checkable |
| F8 | The regex executor runnable reads the non-volatile `GMM.metadataImage` field on an executor thread (GMM:3855) | Info | benign race on an immutable image | Code-review-only |

## 1. Per-shard serialization (Q1): verified, with listed bypasses

- Every `CoordinatorEvent` has `key() = tp` (CR:1309, 1476, 1621, 1742). `EventAccumulator.poll` takes a key from
  `availableKeys`, marks it in-flight (EA:170) and removes it from `availableKeys` (`randomKey`). `add` re-publishes
  a key only if it is not in flight (EA:240). `done` clears the in-flight mark and re-publishes if the queue is non-empty (EA:185-189).
  MT calls `event.run()` (MT:147) and always `done` in `finally` (MT:153). **Conclusion: at most one event per tp runs at a time.**
  The FIFO order per tp is broken only by `enqueueFirst` (HWM update, CR:1819, and linger flush, CR:834).
- Events that use the queue (and so are serialized): writes, reads, txn completion, internal events
  (Load, Unload, CompleteLoad, UpdateImage, FlushBatch, OperationTimeout, HighWatermarkUpdate).
  **Timer expiry and executor completion are ordinary write events.** The `CoordinatorTimerImpl` scheduler lambda is
  `scheduleWriteOperation(name, tp, c -> op.generate())` (CR:493-501), and the executor uses the same lambda (CR:502-510).
  `TimerTask.run` runs on the system timer thread and only enqueues (TI:~86).
- Work that bypasses the queue:
  1. **The loader thread** (`KafkaScheduler(1)`, LI:77) replays records straight into the `SnapshottableCoordinator`
     (LI:241-285, 304-312) **without the context lock**. It is safe only while `state==LOADING`, because every queued
     path checks ACTIVE (`withActiveContextOrThrow` CR:2094, `withActiveContext` CR:2123, HWM CR:1826, UpdateImage CR:2473).
     SC methods are `synchronized`, but the event thread calls the shard (`coordinator.coordinator()`) directly. **F1 breaks this assumption.**
  2. The executor runnable (EI:67-77, the `TaskRunnable`) runs on the `executorService` thread. It must not touch shard state
     (the regex resolver touches only `metadataImage`, see F8). Its result goes through a write event (EI:79).
  3. The HWM listener stores into an `AtomicLong` on the replica thread (CR:1815) and then `enqueueFirst`.
  4. `close()` transitions contexts after the processor threads have been joined (CR:2510-2521).
  5. Future callbacks (`.exceptionally` in TI:103-129 and EI:80-97) run on whichever thread completes the future,
     which is the event thread (HWM, unload, batch fail) or `OperationTimeout` (also an event).

## 2. Write path (Q2): exact sequencing (`CoordinatorWriteEvent.run`, CR:1317-1347)

1. `withActiveContextOrThrow` takes the context lock and checks that the state is ACTIVE (CR:2086-2104). LOADING fails with
   COORDINATOR_LOAD_IN_PROGRESS, anything else with NOT_COORDINATOR.
2. `result = op.generateRecordsAndResult(shard)` (CR:1322) reads the **uncommitted** timeline state (`Long.MAX_VALUE`).
   **Side effects happen here:** `timer.schedule/cancel` (e.g. GMM:5344 `cancelTimers`, GMM:5502 session, GMM:5557 rebalance),
   `executor.schedule` (GMM:3853), and direct mutation when `replayRecords=false` (GMM:5060 downgrade path; KAFKA-20845).
3. `context.append(...)` (CR:906-1045):
   - Empty records: `waitForPendingWrites` (CR:880-890). The event joins the current batch's deferred list if that batch has records.
     Otherwise it goes to DQ at `lastWrittenOffset` if `lastCommitted < lastWritten`, and otherwise completes at once.
   - Transactional writes: `flushCurrentBatch()` first (CR:930-935).
   - `maybeAllocateNewBatch` (CR:797-872) sets `baseOffset = lastWrittenOffset` and `batchEpoch++`. It also sets up the flush:
     with linger>0, a timer that runs `enqueueFirst(FlushBatch)`. With adaptive linger (the **default**, `append.linger.ms=-1`,
     GroupCoordinatorConfig:99), `enqueueLast(FlushBatch(batchEpoch))` (CR:668-687, 857).
   - Atomic writes: if the records do not fit in a non-empty batch, flush it and start a new one (CR:955-977).
     Non-atomic writes: for each record that does not fit, flush and start a new one (CR:984-1006). **A non-atomic op can span batches.**
   - For each record: `coordinator.replay(nextOffset, ...)` (timeline mutation at offset nextOffset), `builder.append`, `nextOffset++`
     (CR:1008-1019). If a replay throws, the event is added to the batch and the **whole batch fails** (CR:1020-1033).
   - Add the event to `currentBatch.deferredEvents` (CR:1037), then `maybeFlushCurrentBatch` (CR:770-780).
     That flushes if the batch is transactional, the linger time has elapsed, or the batch is full.
4. If the future is not done, arm `OperationTimeout(writeTimeout)` (CR:1336-1342). When it fires, an internal event completes the
   event with `TimeoutException` (CR:1136-1140). **Nothing is reverted, and the records may still commit.**

`flushCurrentBatch` (CR:694-765):
- `offset = partitionWriter.append(...)` returns LEO after the batch (CPW: acks=1, local append only, **no expected leader epoch**).
- `coordinator.updateLastWrittenOffset(offset)` (CR:719) creates a snapshot at `offset`. It throws if `offset <= lastWritten` (SC:139-146).
- If `offset != nextOffset`, the context goes FAILED then LOADING (in-place reload) and throws NOT_COORDINATOR (CR:721-735).
  This is detected **after** the records are durable in the local log.
- Otherwise `deferredEventQueue.add(offset, batch.deferredEvents)` (CR:738) and the batch is freed.
- Any throwable runs `failCurrentBatch(t)` and rethrows (CR:742-763).

`failCurrentBatch` (CR:786-792) calls `revertLastWrittenOffset(batch.baseOffset)`. That is `snapshotRegistry.revertToSnapshot(base)`,
which reverts **all timeline state of every op in the batch** (SC:85-95). It then completes **every** deferred event of the batch
with `t` and frees the batch. A snapshot at `base` always exists: snapshots are created at every lastWritten (SC:143) and at 0 (SC:75),
and `deleteSnapshotsUpTo(hwm)` deletes only epochs `< hwm` (SnapshotRegistry:297-305), with `hwm <= lastWritten` (SC:165).

What is and is not rolled back on a batch failure:
- Rolled back: all timeline structures (groups, members, assignments, epochs), including direct mutations in the same batch.
- Failed: every op in the same batch (its response never reaches the client as success).
- **Not affected:** batches already flushed and still waiting in DQ (only an unload fails them).
- **Not rolled back:** timer `tasks` map entries (TI:138 put, 160 cancel), executor tasks (EI:118), `topicHashCache`,
  `lastMetadataImageWithNewTopics` (GMM:544-547). Any op whose `generate` ran but whose append threw before its records
  were replayed (e.g. a flush in CR:969 or CR:998 fails) keeps its side effects with zero records.
- **A timer scheduled by a reverted op can still fire.** It acts on the reverted state, but all consumer timers re-validate:
  the session timeout looks the member up and returns no records if it is unknown (GMM:5406-5427), and the rebalance timeout also checks
  `memberEpoch == scheduled epoch` (GMM:5572). That is safe. **The reverse case is unsafe (F4):** an op that *cancels* timers (leave or fence,
  GMM:5106) and is then reverted leaves the member in state with **no session timer**, a zombie member until reload.
  A revert without unload needs a non-leadership flush error (replay exception in another op of the same batch, storage error,
  RecordTooLarge from a compression estimate) or a failed flush inside a later op's append. Leadership-loss reverts
  are compensated, because the unload runs `cancelAll` and the reload's `onLoaded` reschedules all timers (GMM:6906-6960).

## 3. Reads and uncommitted state (Q3)

- `CoordinatorReadEvent` passes `lastCommittedOffset` (CR:1489-1492), and the group reads use it
  (e.g. `consumerGroupDescribe(groupList, lastCommittedOffset)`, GroupCoordinatorService:1360-1363).
- **Every consumer-group heartbeat is a write** (GCS→GMM:5725). Its response is computed from uncommitted state and **released only when
  HWM ≥ the end offset of its batch**, or, with no records, ≥ the `lastWrittenOffset` seen at the time (CR:880-890). So the response never
  depends on state above its release offset. The heartbeat's own state changes and its in-memory session-timer reset take effect
  **before** commit.
- Results completed without the commit gate: `EMPTY_RESULT` (GMM:559) and classic-protocol `appendFuture` users (out of scope).
  `fetchOffsets(requireStable)` reads `Long.MAX_VALUE` but is a write, so it is deferred (GroupCoordinatorService:1930-1936).
- Responses can be lost or ambiguous: `OperationTimeout` gives TimeoutException while the records may commit, and the unload gives NOT_COORDINATOR
  for records that may already be durable (F2). The protocol relies on the client retrying with its old epoch and on GMM accepting `previousMemberEpoch`.
  **Model this as "response may be lost or failed while the state commits".**

## 4. High watermark (Q4)

- `HighWatermarkListener.onHighWatermarkUpdated` (CR:1810-1846) does `getAndSet` into an AtomicLong and calls `enqueueFirst` only if no
  event is pending, so updates coalesce to the latest value. The event runs `updateLastCommittedOffset(hwm)`, which throws if
  `hwm < lastCommitted` or `hwm > lastWritten` (SC:158-172). It then runs `deferredEventQueue.completeUpTo(hwm)`, which completes all entries with key ≤ hwm in
  offset order (DQ). If `updateLastCommittedOffset` throws, `completeUpTo` is skipped and the error is logged (CR:1764-1766).
- Priority (KAFKA-16374 era): HWM and linger-flush go to the **front** of the tp queue, so they can overtake queued writes and
  even a queued Unload.
- Leader change: `ListenerAdapter` gates delivery on `onBecomingFollower`, `onDeleted` and `onFailed` (CPW:47-66, KAFKA-20634, d559af58d, 2026-06).
  After that, a stale context gets no HWM, its deferred events stay parked, and the unload fails them with NOT_COORDINATOR (CR:625).
  **HWM going backwards cannot reach a context within one tenure.** Across tenures the event uses `coordinators.get(tp)`, not the
  listener's own context (CR:1822), see F6. This is benign because a new context's loader set `lastCommitted=log.highWatermark` (LI:302-308).
- **F3:** the listener is registered only at the ACTIVE transition (CR:553-554). `Partition.maybeAddListener` does not replay the current HWM
  (Partition.scala:271-283). If the HWM advances after the loader's last read (LI:302) and before registration, the new context keeps
  `lastCommitted < lastWritten` until the next HWM change, and that needs a new write with records. Meanwhile every empty-record write
  (steady-state heartbeats, which produce no records) is parked (CR:884-885) and times out with REQUEST_TIMED_OUT after `writeTimeout`.
  Any offset commit or member change heals it. Possible test: in CoordinatorRuntimeTest, a MockPartitionWriter whose HWM is advanced before `registerListener`.

## 5. Load and unload (Q5)

States: INITIAL→LOADING, FAILED→LOADING, {LOADING,ACTIVE}→ACTIVE, {LOADING,ACTIVE}→FAILED, *→CLOSED (CR:265-317).
- `scheduleLoadOperation(tp, e)` (CR:2355-2399): `maybeCreateContext` runs on the caller thread. The Load event then applies only if
  `context.epoch < e`, sets the epoch, and moves INITIAL/FAILED to LOADING. If the context is LOADING or ACTIVE, only the epoch is bumped.
- LOADING (CR:530-548) creates a fresh SnapshotRegistry and shard, then calls `loader.load(tp, coordinator)` (CR:578-613).
  The callback enqueues `CompleteLoad`, which does `coordinators.get(tp)` and, **only if `state==LOADING`**, moves it to ACTIVE
  (or to FAILED on an exception). **There is no epoch or context-identity check (F1).**
- ACTIVE (CR:551-556) registers the HWM listener, then runs `coordinator.onLoaded(image)`, which schedules session timers for all members and
  rebalance timers for UNREVOKED_PARTITIONS members (GMM:6906-6960), plus the expiration timers (GCS:1238-1245).
- `scheduleUnloadOperation(tp, eOpt)` (CR:2409-2448) applies only if `eOpt` is empty or `context.epoch < e`. It moves the context to CLOSED
  and removes it from `coordinators`. `unload()` (CR:618-635) deregisters the listener, runs `timer.cancelAll`, `executor.cancelAll`,
  `deferredEventQueue.failAll(NOT_COORDINATOR)` and `failCurrentBatch(NOT_COORDINATOR)` (which reverts and fails the open batch),
  then `onUnloaded` and sets `coordinator = null`. **Pending deferred events → NOT_COORDINATOR; timers → cancelled (identity
  checks TI:93, EI:67/112 make in-flight timer and executor write events no-ops).**
- Loader (LI:119-171) reads with `FetchIsolation.LOG_END`, so it loads the **uncommitted tail** too. At each batch end
  (LI:301-314): if `nextOffset ≥ HWM`, it sets lastWritten=nextOffset and lastCommitted=HWM. Otherwise, every 16384 offsets, it sets both
  to nextOffset (KAFKA-19716, which bounds snapshot growth). It stops when the LEO is -1 (not leader), giving NotLeaderOrFollower (LI:160-162).
  A "partial batch" cannot exist in the log (batches append atomically). The real "partial tail" is a **non-atomic op split across batches** (F5).
- Uncommitted tail after load: KAFKA-21349 (b1093f64c, 2026-10-27) made the *transaction* coordinator wait for HWM=LEO, because acting on a
  tail that may be truncated leaks effects. The **group** coordinator does not wait. It stays safe because lastWritten=LEO makes every response
  wait for HWM ≥ LEO (`testWriteOpIsNotReleasedWhenStateMachineIsNotCaughtUpAfterLoad`), and its timers and executors have no external effects.
  If the tail is truncated, this broker loses leadership and unloads.
- KAFKA-19862 (83dc0d7ea): the loader can race with log compaction and miss intermediate records (e.g. a partition-unassign record).
  Replay now warns instead of throwing (ConsumerGroup partition-epoch bookkeeping). For the spec, model loading as replay of
  `Compact(log)`, and check `Replay(Compact(log)) ≡ Replay(log)` on the rebalance-relevant projection.
- "Coordinator moves mid-write": the batch may be durable locally but uncommitted. The new leader may lack it, so it is truncated here.
  The client gets NOT_COORDINATOR via the unload (or a timeout), never success: HWM delivery is gated (KAFKA-20634), and DQ releases only at HWM ≥ offset.
  The new leader rebuilds from its own log, which is a prefix-consistent history (Q2 safe *if* every record ever appended was generated from
  the state of the log it was appended to; F1 and F2 break that).

### F1: stale CompleteLoad on a re-created context (HIGH, candidate)
Trace (all events on one tp, loader single-threaded):
1. Load(e1): context C1 is LOADING, and load L1 (into shard S1) runs on the loader thread.
2. The broker resigns at e2: Unload(e2) moves C1 to CLOSED and **removes** it (CR:2434).
3. The broker is re-elected at e3: Load(e3) runs `maybeCreateContext`, creating **C2**, which goes LOADING. L2 (into S2) is queued behind L1 (LI:77).
4. L1 finishes. The partition is led again at e3, so the LEO is not -1 and L1 **succeeds** (LI:163-164). CompleteLoad(L1) runs `coordinators.get(tp)`,
   gets **C2**, sees `state==LOADING`, and moves C2 to ACTIVE (CR:585-594).
5. C2 is ACTIVE with an empty or partial S2, its listener is registered, and `onLoaded` schedules timers on partial state. L2 is replaying into S2 **concurrently**
   with event-thread writes (no lock, see §1). A heartbeat for an existing group sees no group or member. A rejoin (epoch 0) creates a new group
   at epoch 1, and its records are **appended to the real log** after the true group state. The append returns an offset ≠ nextOffset, so
   CR:721 reloads, **but the records are already durable**. After that reload: group-epoch regression and duplicate partition ownership
   (Q1, Q2 violations). The in-place reload (FAILED→LOADING) queues L3. CompleteLoad(L2) can then prematurely activate the L3 context again, a cascade.
Variant b: L1 sees the LEO = -1 during e2, its CompleteLoad carries an exception, and it is processed after Load(e3). C2 goes LOADING→FAILED and is unloaded.
CompleteLoad(L2) is ignored (state FAILED). C2 is **stuck FAILED at epoch e3** until a higher leader epoch arrives. That is a liveness loss for every group on the partition.
Compensation checked: no epoch or identity check (the lambda could capture `CoordinatorContext.this` or `epoch`), no loader cancellation,
and the git history of CR:583-612 (03910980c, 1f23bb132, 638844f83) shows no such guard. No test covers it (CoordinatorRuntimeTest:109-660).
Triggerability: a leadership flap (e1→e2→e3 on the same broker) during a long load, e.g. a fence/unfence or preferred-leader election in a rolling restart.
**Test:** a loader returning a manually completed future: load(e1), unload(e2), load(e3), complete the first future, then assert the context is not ACTIVE.

### F2: stale ACTIVE context appends in a later tenure (MEDIUM, candidate)
Events queued for tp **before** `Unload(e2)` (a backlog, an adaptive FlushBatch, or `enqueueFirst` linger flushes and HWM events that can even jump ahead of the
Unload) run against C1 (ACTIVE, epoch e1). If the broker is **leader again (e3)** by then, `partitionWriter.append` succeeds: CPW `appendRecordsToLeader`
checks leadership but takes no expected leader epoch. C1's state can differ from the log: its uncommitted records were truncated in e2, and it lacks the e2 leader's records.
- If `offset != nextOffset`, the context reloads (CR:721), but the records generated from divergent state are durable and will commit at e3.
- If `offset <= lastWrittenOffset` (the log is now shorter), `updateLastWrittenOffset` throws IllegalStateException (SC:140) **before** the out-of-sync check.
  `failCurrentBatch` reverts while the records stay written, and **C1 stays ACTIVE** (the KAFKA-20845 class, "written but reverted").
- If the offsets match by coincidence, nothing is detected at all.
Listener L1 is retired (KAFKA-20634), so these ops never succeed toward clients, but the **log** now holds records computed from a state that never
existed on that log, which breaks Q2 on the next load. The window is small: it needs the backlog to span two leadership changes plus truncation.

## 6. Transactional writes (Q6), only where they affect group records
Transactional offset commits flush the open batch first and are always atomic (CR:930-935). The verification guard comes from `maybeStartTransactionVerification`
(CR:2211). `completeTransaction` (CR:1058-1118) flushes, `replayEndTransactionMarker`s, appends the marker, and on failure runs `revertLastWrittenOffset(prev)`.
GCS:1489-1494 sends markers only to `offsetMetadataManager`, and **no consumer-group (rebalance) records are transactional**.
Transactions affect the rebalance only through batch boundaries (a forced flush) and through InvalidProducerEpoch failing the open batch, which
reverts any non-transactional heartbeat ops that shared it (a source of F4-style reverts without unload).

## 7. Abstract per-shard state machine (for TLA+)

Constants: `Brokers`, `Epochs`, `MaxLog`, `Ops` (abstract group ops). The shard tp is fixed.
Log layer (per broker b): `log[b]` (sequence of entries `[op, epoch, gen]`), `hwm` (the leader's), `leader`, `leaderEpoch`.
Runtime (per broker b; the context may be absent):
```
ctx[b] ∈ [state: {INITIAL,LOADING,ACTIVE,FAILED,CLOSED}, epoch: Nat, id: Nat (incarnation),
          uncommitted: GroupState     \* timeline "latest" (SC + shard)
          committedOff, writtenOff,   \* SC.lastCommittedOffset / lastWrittenOffset (SC:56-59)
          batch: Seq(op) ∪ {none}, batchBase,   \* CR:323-393, base=writtenOff at alloc (CR:805)
          snapshots: [off -> GroupState],       \* SC:143 create, SC:165 delete <hwm, SC:94 revert
          dq: [off -> SUBSET Resp],             \* DeferredEventQueue (CR:419)
          timers: SUBSET [key, member, epoch, incarnation],  \* TI tasks map (non-timeline!)
          listener: BOOLEAN, loadPending: Nat]  \* loader job id
queue[b]: Seq(Event)   \* per-tp FIFO with enqueueFirst for HWM/FlushLinger
loaderJobs[b]: Seq([job, targetCtxId])  \* single-threaded loader, LI:77
responses: set of [member, resp, status ∈ {pending, ok, failed}]
```
Actions (each is one serialized event, matching §1):
- `Generate(op)` + `Append` (one step): requires ctx ACTIVE. It computes records from `uncommitted`, applies them, adds them to the batch,
  and attaches the response to the batch. Side effects on `timers` happen here (not reverted).
- `Flush`: `appendLocal(log[b])` is allowed iff b is the current leader (**no epoch check**, to expose F2). Then `writtenOff' = Len(log)`,
  and if the offset differs from expected, FAILED+LOADING. Otherwise move the batch responses to `dq[off]` and snapshot at off.
- `FlushFail(err)`: revert `uncommitted` to `snapshots[batchBase]`, fail the batch responses, keep `timers` (F4).
- `Replicate` / `AdvanceHWM` (log layer, nondeterministic up to a quorum LEO). `HWMEvent` (front of queue): only if `listener` is active.
  It sets `committedOff`, releases `dq` up to the HWM, and sets responses to `ok` (clients act on these).
- `LeaderChange(e)`: new leader, followers truncate to the new leader's log, the old leader's listener is retired (KAFKA-20634), and Unload(e)
  is enqueued on the old leader and Load(e) on the new one, **at the back** of the queue.
- `LoadStart` (Load event, epoch check CR:2371): creates a ctx if absent (new `id`) and enqueues a loader job `[job, ctx.id]`.
- `LoaderRun` (loader thread): replays `log[b]` (optionally `Compact(log)`) into the job's shard. The result is a success, or a failure if b is not leader.
  It enqueues `CompleteLoad(job)`. **`CompleteLoad` checks only `ctx[b].state = LOADING` (not `id`)**, which exposes F1.
- `Unload`: epoch check (CR:2421), `failAll` dq, revert batch, clear timers, delete ctx.
- `TimerFire(k)`: enqueues a write. On execution it applies only if the task identity still matches (TI:93). Re-validation is done by the op (GMM).
- `OpTimeout(r)`: sets the response to `failed` without a revert (CR:1136).
Invariants to check:
- `CommittedPrefix`: the `ok` responses are justified by the committed log prefix (Q3).
- `NoActiveWhileLoading`: ACTIVE ⇒ no loader job targets this ctx incarnation (F1).
- `AppendFromLogState`: every appended entry was generated from `Replay(log[b] prefix it is appended to)` (F1, F2, Q2).
- `ReloadEquivalence`: after a reload, `Replay(log)` restricted to committed equals what clients were told (`ok`) (Q2).
- `TimersCoverMembers`: in each ACTIVE ctx, every member in `uncommitted` has a session timer (F4, liveness).
- `TimerIncarnation`: a timer write from incarnation i never executes against i' ≠ i (F7).
Liveness: `◇` an ACTIVE ctx eventually exists on the leader (F1b); a parked empty write eventually completes (F3).

## 8. Granularity recommendations
1. Keep **Append(local) / Replicate / AdvanceHWM / HWMEvent(delivery)** as four separate actions. F3 and KAFKA-20634 live in the gap between HWM advance and delivery.
2. Model **batching only as "one open batch of ≤ 2 ops"**, with FlushFail reverting all of them. Linger is just a nondeterministic Flush event. Model non-atomic ops
   only if cleanup is in scope (F5).
3. Model **leader failover with truncation** explicitly: followers' logs = the new leader's log. Use 2 brokers and 3 leader epochs, the minimum for F1/F2 (e1→e2→e3 back on B).
4. Model the **event queue per broker as a sequence** with `enqueueFirst` for HWM and linger events. Load, Unload and CompleteLoad must be queue events, and the loader
   a separate process with a FIFO of jobs (F1 needs the loader to be decoupled from the queue).
5. Group state: abstract it to {groupEpoch, assignmentEpoch, per-member (epoch, prevEpoch, owned, state)} with 2 members and 2 partitions. Timers are a set of
   (kind, member, epoch). Treat timers as non-timeline (not reverted). Abstract the regex executor away.
6. Compaction: optional action `Compact` on the committed prefix before `LoaderRun` (KAFKA-19862).
7. Suggested order: (a) a runtime-only spec with opaque ops and the F1/F2/F3 invariants, which is cheap and closes fast. (b) Plug in the KIP-848 group abstraction
   for Q1/Q2/Q4.
