# Modeling Brief: Apache Kafka Transaction Coordinator

## 1. System Overview

- **System:** Apache Kafka transaction coordinator, trunk @ `8aa57006ff`. Scala (`core/.../coordinator/transaction`) plus Java (`transaction-coordinator/`), about 3.7K LOC of core logic.
- **Category: A (Distributed / Message-Passing).** The coordinator implements 2PC. It persists the decision to the replicated `__transaction_state` partition, sends WriteTxnMarkers by RPC to the data-partition leaders, then appends the completion record. Ownership follows partition leadership, with coordinator epoch = leader epoch.
- **Protocol:** KIP-98 exactly-once transactions, KIP-360 epoch bump, KIP-890 TV2 (epoch bump on every EndTxn, implicit AddPartitions from produce, verification guards).
- **Deviations from textbook 2PC:**
  - The decision record is followed by in-memory "pending" transitions.
  - Markers are fire-and-retry and are tracked in a per-transactional-id `transactionsWithPendingMarkers` map.
  - Coordinator-initiated aborts (timeout, InitProducerId fence) use a two-step PrepareEpochFence handshake that is never persisted.
  - Loads wait for the high watermark asynchronously (KAFKA-21349).
- **Concurrency model:** each broker runs several threads:
  - request handlers;
  - DelayedProduce completion callbacks;
  - one TxnMarkerSenderThread;
  - one `transaction-log-manager-` scheduler for loads, timeout aborts and expiration;
  - the metadata publisher for `onElection` / `onResignation`.
- **Locks:** `stateLock` (read/write) and a per-transactional-id `txnMetadata.inLock`. Most handlers are split into separate atomic steps: lookup, prepare, append, callback re-lookup, apply, enqueue. The table is in `analysis-report.md` §1.

## 2. Scenarios

### Scenario 1: Stale-epoch asynchronous continuations overwrite newer-epoch state

**Mechanism:** callbacks validate the coordinator epoch in one atomic step and act on shared, per-id structures in a later, unguarded step. If a same-broker reload happens in between, the newer epoch's state gets overwritten or deleted.

**Evidence:**
- Historical: #15524 (stuck PrepareX), #8389 / #8782 (a stale operation removes a newer completion entry), #5036, #4140 (duplicate CompleteX), KAFKA-5416. 10 M2 fixes, 3 of them Critical (`notes/archaeology-git.md`).
- F1: `sendTxnMarkersCallback` checks the epoch at `TC:960`, sets pending CompleteX at `TC:976`, then calls `addTxnMarkersToSend` with no lock. Its `put` at `TMCM:281` is unconditional, so it can replace the entry that the epoch-(e+1) reload installed (`TSM:604-608`). Stale responses then `remove` it (`TMRCH:127-133`), and the e+1 completion finds no entry (`TMCM:299-306`). The transaction stays in PrepareCommit with pending CompleteCommit.
- C1: the expiration tombstone callback (`TSM:264-291`) re-reads the current cache entry with no null or epoch check. It can NPE, or clear the pending state of a newer object.

**Affected code paths:** `TransactionCoordinator.endTransaction.sendTxnMarkersCallback`, `TransactionMarkerChannelManager.addTxnMarkersToSend / maybeWriteTxnCompletion / removeMarkersForTxn`, `TransactionMarkerRequestCompletionHandler.onComplete`, `TransactionStateManager.removeExpiredTransactionalIds`, `onElection`.

**Suggested modeling approach:**
- Variables:
  - `cache[b]`: per-broker `{epoch, txn record, pending}`;
  - `pendingMarkers[b][tid]`: `PendingCompleteTxn` holding an epoch and a reference to a metadata *object version*;
  - `inflightCallbacks`: a bag of `{kind, epoch, step}`.
- Actions: split EndTxn into `PrepareAppend`, `AppendAck`, `CallbackCheckEpoch`, `CallbackPrepareComplete` and `CallbackEnqueue`. Split marker completion into `CheckEpoch`, `RemovePartition` and `MaybeComplete`. Add `ElectSameBroker(e+1)`, which bumps the leader epoch with no resignation, as BMP:301 allows.
- Granularity: follow the lock table exactly. The bug only exists if the `Enqueue` step is separate from the epoch check.

**Priority:** High.
**Rationale:**
- This is the densest historical area, with 18 M2+M5 fixes.
- F1 is a fresh, reachable-looking interleaving.
- The answer requires interleavings that code review cannot settle.

### Scenario 2: Load ordering across coordinator epochs and the HWM wait

**Mechanism:** `loadingPartitions` can hold several epochs for one partition. Each load task checks only its own key before installing its cache unconditionally, and resignation removes at most one loading entry. A stale load can therefore install an older epoch, or run without its HWM wait, after a newer election.

**Evidence:**
- Historical: KAFKA-21349 / #23943 (Critical, merged 2026-10-27): loading records above the HWM caused a split commit/abort. Also #23630, #23831, #15139 (only part 1 merged), #7840, #7963, #18011.
- Code analysis:
  - F2/C2: `TSM:577` checks only `loadingPartitions.contains(own key)`, then `TSM:542` does an unconditional `put`. Load(e) waits for the HWM (`TSM:634-655`), while Load(e+1) may not.
  - C3: `removeLoadingPartitionWithEpoch` uses `find` (`TSM:731-737`).
  - `getTransactionState` serves from the cache unless `isCoordinatorFor` fails (`TSM:403-423`).
  - KAFKA-21181 empty commit on a stale leader (`TC:859-905`) relies on that write failing to replicate.

**Affected code paths:** `onElection`, `onResignation`, `loadTransactionsForTxnTopicPartition` (`HighWatermarkWaitListener`, `scheduleLoad`, `scheduleAbandonLoad`), `addLoadedTransactionsToCache`, `removeTransactionsForTxnTopicPartition`, `getAndMaybeAddTransactionState`.

**Suggested modeling approach:**
- Variables:
  - `txnLog`: a sequence of records `{tid, state, pid, epoch, partitions, leaderEpoch}`;
  - `hw`;
  - `leader` and `leaderEpoch` per broker view;
  - `loading[b]`: a set of epochs;
  - `loadQueue[b]`: the scheduler FIFO.
- Actions:
  - `BecomeLeader(b, e)`, `BecomeFollower(b)`, `ReplicateTxnLog`, `TruncateToHW`;
  - `HwReached(b, e)`, which enqueues a load;
  - `RunLoad(b, e)`, which reads the log up to the LEO (not just the HW) as of that moment;
  - `AbandonLoad`.
- Granularity:
  - The log read and the install are separate steps.
  - Install is atomic, because it runs under the stateLock write lock.
  - The leader epoch on a broker can bump without an intervening resignation.

**Priority:** High.
**Rationale:**
- This scenario is the only path found to a cross-partition **atomicity violation**, which is guidance Priority Question 1.
- The area was patched last week and the fix has not been audited for multi-epoch load queues.

### Scenario 3: The epoch-fence handshake is split across two lock regions

**Mechanism:** coordinator-initiated aborts (InitProducerId on an Ongoing txn, timeout abort) set pending PREPARE_EPOCH_FENCE under one lock and consume it in `endTransaction` under a later lock. Any request that runs in between sees a half-done fence. The timeout abort also acts on a stale snapshot.

**Evidence:**
- Historical: M3 and M6 fixes — KAFKA-20785 (TV2 commit racing the timeout abort), #18467, #20534, #23204, #3066, #4137, #8164, #13499 (pending state left after a failed fence append), #19910/#19976 (double epoch bump on a failed fence).
- Code analysis:
  - The fence is prepared at `TC:180-183 / 288` and `TC:1086-1099`, then consumed at `TC:804, 847-851`; the TV1 path mutates the epoch in memory at `TC:578-585`.
  - C4: the timeout abort re-checks only producerId (`TC:1087`), and the snapshot epoch is never compared.
  - C5: a client EndTxn(ABORT) can consume the fence.
  - C6: a client EndTxn(COMMIT) during the fence throws at `TM:319`.

**Affected code paths:** `handleInitProducerId`/`prepareInitProducerIdTransit`, `abortTimedOutTransactions`, `timedOutTransactions`, `endTransaction`/`endTransactionWithTV1`, `prepareFenceProducerEpoch`, `hasFailedEpochFence`.

**Suggested modeling approach:**
- Variables: `pending[b][tid]`, `producerEpoch` and `lastProducerEpoch`, `hasFailedEpochFence`, and a ghost `txnInstance` counter that increments on each Empty/Complete->Ongoing.
- Actions:
  - `TimeoutSnapshot`, then `TimeoutFence`, then `FenceEndTxn`;
  - `InitPidFence`, then `FenceEndTxn`;
  - client `EndTxn(commit|abort, epoch)` from an environment with stale and retried epochs.
- Granularity: the snapshot, the fence prepare and the consume must be three separate actions.

**Priority:** High.
**Rationale:** this answers guidance Priority Question 2 (total fencing). The window is small but real, and fencing a live producer that has not timed out is client-fatal under TV1.

### Scenario 4: Second-phase stalls with no driver

**Mechanism:** after PrepareX is durable, only the marker channel moves the transaction forward. Several error branches drop that work while leaving PrepareX in place, at the same coordinator epoch. Nothing periodically re-drives PrepareX: the timeout abort skips it and clients get CONCURRENT_TRANSACTIONS. Recovery waits for the next leadership change.

**Evidence:**
- Historical: KAFKA-20734 / #22680 (Critical, 2026-10-27): an offline partition was treated as deleted, which left a hanging txn. Also #15524 and #21176 (TV2 marker-retry livelock); open PR #23993 (busy loop).
- Code analysis:
  - F3: on INVALID_PRODUCER_EPOCH or TRANSACTION_COORDINATOR_FENCED, `TMRCH:160-167` drops the marker work and leaves pending CompleteX.
  - F5: when the CompleteX append fails with NOT_COORDINATOR at an unchanged epoch, pending is cleared (`TSM:836-847`, `TMCM:314-332`) and PrepareX is left behind.
  - Partition-leader contract: TV2 markers need a strictly greater epoch, except for the duplicate case (`PAI:137-144`).
  - Coordinator epochs are tracked per producerId and reset when the producer expires (`PSM:329-339`).

**Affected code paths:** `TransactionMarkerRequestCompletionHandler.onComplete`, `TransactionMarkerChannelManager.tryAppendToLog / addTxnMarkersToBrokerQueue / unknown-broker queue`, `TransactionStateManager.appendTransactionToLog` error mapping.

**Suggested modeling approach:**
- Variables: `dataPart[p]`, an abstract partition-leader producer state `{epoch, coordEpoch, ongoing, markers}`, plus `markerInFlight`.
- Actions: `LeaderReceiveMarker` (accept, or reject with a nondeterministic error from the documented set), `MarkerResponse`, `CompleteAppend` (with nondeterministic error outcomes), `DataLeaderChange`, `ProducerStateExpire`.
- Granularity: one marker per partition per step. Error outcomes are drawn nondeterministically from the code's own error tables.

**Priority:** Medium-High.
**Rationale:** this answers guidance Priority Question 4 (stuck pending) as a liveness question. Reaching F3 needs specific leader-side states that the model can search for.

### Scenario 5: TV2 epoch-bump retry classification

**Mechanism:** under TV2, a request at epoch e can be a retry, a stale request, or the next transaction. The coordinator classifies it from `(state, epoch, retryOnEpochBump, retryOnOverflow)`, and each newly found misclassification has been patched one cell at a time.

**Evidence:**
- Historical:
  - M4: 8 fixes, including #21506 (stuck ONGOING at the epoch boundary), #21828 and #22096.
  - KAFKA-20785: only the CompleteAbort+Commit+Retry cell was patched.
- Code analysis:
  - W1: in the PrepareAbort-without-pending window between `TC:950` and `TC:976`, a client COMMIT at the pre-abort epoch gets INVALID_TXN_STATE (`TC:917-918`).
  - CompleteCommit + Abort + Retry returns INVALID_TXN_STATE (`TC:889-891`).
  - C7: an InitProducerId retry at MAX-1 throws (`TM:158`).

**Affected code paths:** `endTransaction` (TV2) state table, `prepareAbortOrCommit`, `prepareComplete` overflow rotation, `validProducerEpoch`, `prepareInitProducerIdTransit`.

**Suggested modeling approach:**
- Use a small epoch domain with MAX = 3, so that overflow and rotation are reachable.
- Model the client as an environment that may replay any of its earlier requests.
- Record a ghost `clientOutcome` per request, so the model can check that no fatal error is returned while the decision is still benign.

**Priority:** Medium.
**Rationale:** this area is active (11 fixes in 2026). The violations it can produce are client-fatal errors, not data inconsistency.

### Scenario 6: TV2 implicit AddPartitions and verification vs EndTxn and failover

**Mechanism:** data reaches a partition through `AddPartitions(TV2)` or verification, and a guard is checked later at append time. Markers go to the coordinator's partition set. These two sets must match, even across EndTxn and failover.

**Evidence:**
- Historical: M10 — d662b09c9f (Critical), #13579, #14774, #18720, #18810, #19902, #23234.
- Contracts:
  - Verification guards live in memory only and survive a leadership change (`PSM:87, 150-153`).
  - The guard check is skipped when `transaction.partition.verification.enable=false` (`UL:1479`).
  - Under TV2, the Short.MAX abort does not bump the epoch (`TM:242-245`), so late data at that epoch is not rejected by the epoch check.

**Affected code paths:** `handleAddPartitionsToTransaction`, `handleVerifyPartitionsInTransaction`, and the leader-side guard (abstracted).

**Suggested modeling approach:** add `Produce(p, epoch)` split into `Verify` and `Append`, with an abstract guard per partition. Reuse the data-partition model from Scenario 4.

**Priority:** Medium-Low.
**Rationale:** this answers guidance Priority Question 3. The contracts suggest the property holds, except at the epoch-MAX and verification-disabled corners. Keep it in the model for coverage, at a lower search budget.

## 3. Modeling Recommendations

### 3.1 Model
- **Replicated txn log with HW and leader epoch** (S1, S2): `__transaction_state` with 2 brokers, records above the HW, truncation, and leader epoch bumps on the same broker.
- **Coordinator per broker with split handler steps** (S1, S3, S5): use the lock table in `analysis-report.md` §1. The pending state is per-broker in-memory state.
- **Marker channel** (S1, S4): the `pendingMarkers` map with `put` and `remove(k,v)` semantics, a per-partition queue, and the completion handler.
- **Abstract data-partition leader** (S4, S6): producer epoch, coordinatorEpoch with `>=` acceptance, the TV1/TV2 marker epoch rules, an ongoing flag, the guard, and producer-state expiry.
- **Scheduler tasks** (S2, S3): load, abandon-load, timeout abort, expiration, serialized FIFO on one thread.
- **Crash / restart of a broker** (S2): in-memory state is lost and the log is kept.

### 3.2 Do Not Model
- **Producer client state machine:** an environment that sends arbitrary, possibly stale or retried requests is enough. The client is out of scope per the guidance.
- **ProducerIdManager block allocation:** `generateProducerId` is a nondeterministic success or failure. M9 races are implementation-level.
- **KIP-939 2PC / `keepPreparedTxn`:** stubbed in trunk (`TC:143-146`), so there is nothing to verify.
- **Serde, compaction, metrics, the TxnMarkerQueue data structure, the busy loop F6:** not protocol logic.
- **Unclean leader election, and `min.insync.replicas=1` zombie leaders:** these are replication-layer assumptions (#22937 treats them as inherent). Assume the HW is never truncated.
- **FatalExitError on unexpected error codes (F4) and ISE-to-UNKNOWN_SERVER_ERROR (C6, C7):** deterministic, so better confirmed by unit tests.
- **Group coordinator TxnOffsetCommit and ProducerStateManager internals beyond the abstraction above:** out of scope per the guidance.

## 4. Proposed Extensions

| Extension | Variables | Purpose | Scenario |
|-----------|-----------|---------|----------|
| SameBrokerReElection | `leaderEpoch[b]`, `loading[b]` (set) | epoch bump without resignation; multiple loading epochs | S1, S2 |
| AsyncLoad | `loadQueue[b]`, `hwWait[b]` | the HWM wait and the stale queued load | S2 |
| SplitEndTxnCallback | `cb[tid]` with phase and captured epoch | the epoch-check-then-act window | S1, S5 |
| PendingMarkersMap | `pendingMarkers[b][tid]` = `{epoch, objVersion}` | overwrite and removal by a stale owner | S1 |
| FenceHandshake | `pending`, `hasFailedEpochFence`, `timeoutSnap` | the three-step fence | S3 |
| DataPartitionLeader | `dp[p]` = `{epoch, coordEpoch, ongoing, guard, markers}` | the marker acceptance contract | S4, S6 |
| ErrorInjection | nondeterministic append and marker error codes from the code's tables | stall paths | S4 |
| GhostTxnInstance | `txnInst[tid]`, `decision[tid][inst]` | outcome and fencing invariants | all |

## 5. Proposed Invariants

| Invariant | Type | Description | Targets |
|-----------|------|-------------|---------|
| MarkerAtomicity | Safety | for each (pid, txn instance), no partition has a COMMIT marker while another has an ABORT marker | S2, guidance Q1 |
| DurableDecisionFinal | Safety | once PrepareCommit (PrepareAbort) is at or below the HW, every marker written for that instance is COMMIT (ABORT) | S2, guidance Q1 |
| MarkersOnlyToTxnPartitions | Safety | a marker for an instance is written only to partitions in that instance's durable partition set | S6, guidance Q3 |
| NoDataWithoutMarker | Safety | every partition holding data for a completed instance is in the instance's partition set, so it receives a marker | S6, guidance Q3 |
| FencingTotal | Safety | after InitProducerId returns epoch E, no EndTxn/AddPartitions/append carrying an epoch < E succeeds | S3, guidance Q2 |
| NoSpuriousCoordinatorAbort | Safety | a coordinator-initiated abort only hits an instance that is timed out, or that is being fenced by an InitProducerId | S3 (C4, C5) |
| PendingOwnerConsistent | Safety | if `pendingMarkers[b][tid]` exists, its epoch equals `cache[b].epoch`, or the entry belongs to an unloaded epoch | S1 (F1) |
| MemoryMatchesDurable | Safety | when there is no pending state, cached state equals the latest record for that tid in the log at or below the LEO | S1 (C1), guidance Q4 |
| CacheEpochIsLeaderEpoch | Safety | when a broker is leader and is not loading, `cache[b].epoch = leaderEpoch[b]` | S2 (F2) |
| NoFatalOnBenignRace | Safety | the client never receives INVALID_TXN_STATE or UNKNOWN_SERVER_ERROR for a request that a correct client could send | S3, S5 (W1, C6) |
| PrepareEventuallyCompletes | Liveness | under stable leadership with weak fairness on the sender and the append, PrepareX ~> CompleteX | S1, S4, guidance Q1/Q4 |
| PendingEventuallyClears | Liveness | `pending[b][tid] ≠ None ~> pending = None`, or b loses leadership | S1, S3, S4, guidance Q4 |
| ExpirationOnlyIdle | Safety | a tombstone is durable only for a tid whose last durable state is Empty or Complete*, and no transaction is open | guidance Q5 |

## 6. Findings Pending Verification

### 6.1 Model-Checkable

| ID | Description | Expected invariant violation | Scenario |
|----|-------------|------------------------------|----------|
| MC1 | Can a delayed epoch-e `sendTxnMarkersCallback` overwrite the epoch-(e+1) pending entry after a same-broker re-election, leaving PrepareX unfinished until the next election? (F1) | PendingOwnerConsistent, PrepareEventuallyCompletes | S1 |
| MC2 | Can a stale queued load (multiple loading epochs; the `find`-based removal) install an older-epoch cache, or load records above the HW, after a newer election? Can that send markers that disagree with the eventual durable decision? (F2, C3) | CacheEpochIsLeaderEpoch, MarkerAtomicity, DurableDecisionFinal | S2 |
| MC3 | Can the expiration tombstone callback, or any other epoch-checked callback, act on a newer-epoch object after a reload, so that memory and log diverge? (C1) | MemoryMatchesDurable, PendingEventuallyClears | S1 |
| MC4 | Can a timeout abort or fence act on a newer transaction instance than the one it snapshotted? Can a client EndTxn that consumes the fence leave an old epoch accepted after InitProducerId? (C4, C5) | NoSpuriousCoordinatorAbort, FencingTotal | S3 |
| MC5 | Under stable leadership, is there a reachable data-leader state in which markers are rejected with INVALID_PRODUCER_EPOCH or COORDINATOR_FENCED, or the CompleteX append fails with NOT_COORDINATOR, leaving PrepareX with no driver? (F3, F5) | PrepareEventuallyCompletes | S4 |
| MC6 | With TV2 implicit add plus failover, can a partition have data for an instance that is missing from its durable partition set, or receive a marker for an instance it has no data of? Focus on the epoch-MAX abort and a guard surviving a leader change. | NoDataWithoutMarker, MarkersOnlyToTxnPartitions | S6 |
| MC7 | Is there any interleaving of retries, timeout aborts and overflow at a small epoch MAX in which a correct client gets a fatal error, beyond the known W1 window? | NoFatalOnBenignRace | S5 |

### 6.2 Test-Verifiable

| ID | Description | Suggested test approach |
|----|-------------|-------------------------|
| T1 | C7: InitProducerId retry with `(pid, MAX-2)` while the current epoch is MAX-1 gets UNKNOWN_SERVER_ERROR (`TM:158` before `TM:174`) | `TransactionCoordinatorTest`: set the metadata epoch to MAX-1 and lastEpoch to MAX-2, then call `handleInitProducerId` |
| T2 | C6: EndTxn(COMMIT) while PREPARE_EPOCH_FENCE is pending throws an ISE | set the pending state manually, then call `handleEndTransaction(COMMIT)` for TV1 and TV2 |
| T3 | C4a: the timeout abort hits a txn in PrepareX/CompleteX without pending, which throws an ISE and skips the rest of the round | stub `timedOutTransactions` to return a stale snapshot |
| T4 | W1: a TV2 COMMIT at the pre-abort epoch, arriving between the PrepareAbort append and the pending CompleteAbort, gets INVALID_TXN_STATE | `TransactionCoordinatorTest` with a captured append callback |
| T5 | F4: a WriteTxnMarkers response with UNKNOWN_TOPIC_ID (topic recreated) leads to FatalExitError | `TransactionMarkerRequestCompletionHandlerTest` |

### 6.3 Code-Review-Only

| ID | Description | Suggested action |
|----|-------------|------------------|
| R1 | CompleteCommit + Abort + Retry returns INVALID_TXN_STATE (`TC:889-891`); this is the sibling of the KAFKA-20785 cell | confirm intended client behavior |
| R2 | `addTxnMarkersToSend` `put` is unconditional (`TMCM:281`); keeping the higher epoch would close F1 | review fix |
| R3 | `removeLoadingPartitionWithEpoch` uses `find`, not `filter` (`TSM:731-737`) | review fix |
| R4 | The load install calls `txnMetadata.inLock` and appends while holding the stateLock write lock, against the documented lock order (`TSM:61-69, 576-611`) | review |
| R5 | `TransitionData.topicPartitions` aliases the live set (`TM:665`); `TxnTransitMetadata` is documented as mutable | check for aliasing with `removePartition` |
| R6 | Deleted partitions are matched by topic name, not topic id (`TMCM:388-396`) | review |
| R7 | The partition leader accepts an equal coordinatorEpoch, and followers overwrite it without checking (`PAI:259-262, 300`) | review the contract |
| R8 | The sender busy-loops on offline partitions (`TMCM:192-207, 411`; open PR #23993) | track the PR |
| R9 | CompleteX retries on COORDINATOR_NOT_AVAILABLE have no backoff and no `wakeup()` (`TMCM:323`) | review |

## 7. Reference Pointers

- **Full analysis:** `analysis-report.md`. Notes: `notes/archaeology-git.md` (77-fix table), `notes/archaeology-prs.md`, `notes/contracts-partition-leader.md`, `notes/deep-statemanager.md` (§0 has a suggested action list), `notes/deep-markers.md` (lock/thread table, error tables).
- **Key source:**
  - `TransactionCoordinator.scala` 118-297 (InitProducerId), 362-470 (Verify/Add), 478-506 (election), 773-1037 (EndTxn TV2), 1077-1113 (timeout abort);
  - `TransactionStateManager.scala` 129-291 (timeouts/expiration), 400-430 (lookup), 530-740 (load/unload), 760-912 (append);
  - `TransactionMarkerChannelManager.scala` 270-340, 380-415;
  - `TransactionMarkerRequestCompletionHandler.scala` 55-205;
  - `TransactionMetadata.java` 140-493.
- **Partition-leader contract:** `storage/.../ProducerAppendInfo.java` 122-159, 258-300; `UnifiedLog.java` 890-937, 1442-1482, 2694-2702; `KafkaApis.scala` 1755-1889.
- **Historical context (reference only):** KAFKA-21349 / #23943, KAFKA-20734 / #22680, KAFKA-20785, KAFKA-21181, #15524, #8389, #13499, #19910, #21506.
- **Specs:** KIP-98, KIP-360, KIP-890. No prior TLA+ spec of the Kafka transaction coordinator was found in this repository.
