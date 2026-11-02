# Analysis Report: Apache Kafka Transaction Coordinator

Target: Apache Kafka trunk @ `8aa57006ff` (2026-11-02), guidance `guidance/kafka-transaction-coordinator.md`.
Audit trail for `modeling-brief.md`. The detailed evidence lives in `notes/`:

| File | Content |
|------|---------|
| `notes/archaeology-git.md` | 236 commits on coordinator paths, table of all 77 bug fixes, mechanism groups M1-M11, hotspots |
| `notes/archaeology-prs.md` | 863 PRs collected, 64 deep-read, open PR list, exclusions |
| `notes/contracts-partition-leader.md` | WriteTxnMarkers / verification / coordinatorEpoch contracts on the data-partition leader |
| `notes/deep-statemanager.md` | TransactionStateManager: append, load/unload, expiration, timeout abort (candidates C1-C7) |
| `notes/deep-markers.md` | Marker channel + completion handler (candidates F1-F6), lock/thread table |

## 1. Reconnaissance

| Component | File | LOC |
|-----------|------|-----|
| Request handlers, fence, timeout abort | `core/.../transaction/TransactionCoordinator.scala` | 1145 |
| Txn log append, load/unload, expiration, cache | `core/.../transaction/TransactionStateManager.scala` | 929 |
| Marker queues, sender thread, CompleteX append | `core/.../transaction/TransactionMarkerChannelManager.scala` | 442 |
| WriteTxnMarkers response handling | `core/.../transaction/TransactionMarkerRequestCompletionHandler.scala` | 205 |
| State machine (prepare/complete transitions) | `transaction-coordinator/.../TransactionMetadata.java` | 673 |
| States and valid previous states | `transaction-coordinator/.../TransactionState.java` | 171 |
| Record serde | `transaction-coordinator/.../TransactionLog.java` | 165 |

Core logic is about 3.7K LOC. **Category A (Distributed / Message-Passing)**. The protocol is a two-phase
commit: the decision is persisted to the replicated `__transaction_state` partition, then markers are sent by RPC to
data partition leaders, then a completion record is appended. Coordinator ownership follows partition leadership
(coordinator epoch = leader epoch). There are thread-level races inside one broker too, but every hazard found involves
either the replicated log, leadership change, or RPC to other brokers.

### Threads and atomicity boundaries (from `notes/deep-markers.md`, `notes/deep-statemanager.md`)

| Thread | Work |
|--------|------|
| request handler | InitProducerId, AddPartitions, Verify, EndTxn: `getTransactionState` (stateLock read) then `txnMetadata.inLock` prepare, then `appendTransactionToLog` |
| purgatory / DelayedProduce completion | append callback: re-lookup (stateLock read), then completeTransition under txn lock, then response callback (e.g. `sendTxnMarkersCallback`, which re-locks) |
| TxnMarkerSenderThread (1) | drains broker queues, completion handler, CompleteX append retries |
| `transaction-log-manager-` scheduler (1) | load tasks, abandon-load tasks, timeout abort, transactional id expiration |
| metadata publisher | `onElection` / `onResignation` (clears marker queues without stateLock) |

Key non-atomic sequences:
- `prepareFenceProducerEpoch` (txn lock #1) -> lock released -> `endTransaction` (txn lock #2). `TC:180-208`, `TC:1086-1110`.
- PrepareX append success -> `completeTransitionTo` (lock) -> `sendTxnMarkersCallback` epoch check (stateLock read) -> `prepareComplete` (txn lock) -> unlocked `addTxnMarkersToSend` `put` (`TMCM:281`). `TC:950-1007`.
- Load: HWM wait -> scheduled load task reads log -> stateLock write install (`TSM:576-611`), where only its own `(partition, epoch)` key in `loadingPartitions` is checked.

## 2. Bug archaeology summary

- Git: 236 commits touch the coordinator directories. 77 are bug fixes: 7 Critical, 33 High, 21 Medium, 16 Low. 26 of them landed 2024-2026, 11 in 2026 alone.
- PRs: 863 collected via search. 64 were deep-read from the PR body, the squash commit message and the trunk code. 49 were confirmed, 10 are open, 15 were excluded (reasons in `notes/archaeology-prs.md`).
- Limitation: JIRA (issues.apache.org) is blocked by the network policy, and GitHub REST access to `apache/kafka` comment threads returned 403. Issue discussions were therefore not read. Maintainer acknowledgment was taken from the `Reviewers:` lines of merged commits.

Mechanism groups (fix counts by primary mechanism):

| Group | Fixes | Representative | Live today? |
|-------|-------|----------------|-------------|
| M2 stale coordinator epoch / load-unload | 10 (3 Critical) | KAFKA-21349 / #23943 (load above HWM -> split commit/abort, merged 2026-10-27), #23630, #23831, #15139 | partially (S1, S2) |
| M5 marker delivery / completion map | 8 (1 Critical) | #15524 (stuck PrepareX), #8389, #8782, KAFKA-20734 / #22680 (offline partition treated as deleted -> hanging txn, 2026-10-27) | yes (S1, S4) |
| M1 pendingState after failed append | 6 | #3184, #5820, #8239, #13499, #19910/#19976 | through the fence window (S3) |
| M3 epoch bump / fencing ongoing txn | 5 (2 Critical) | KAFKA-20785 (TV2 commit vs timeout abort), #18467, #20534, #23204 | yes (S3, S5) |
| M4 TV2 overflow / PID rotation / persisted fields | 8 | #21506, #21828, #22096 | edge cases (S5) |
| M6 expiration / timeout races | 5 | #3101, #7687, #13499 | yes (S3, S1/C1) |
| M10 partition add / TV2 verification | 4 (1 Critical) | d662b09c9f, #18720, #18810, #19902, #23234 | believed closed (S6) |
| M7 locking, M8 serde, M9 PID blocks, M11 misc | 31 | see notes | out of modeling scope |

Open PRs still reflected in trunk: #23993 (sender busy loop on offline partitions, `TMCM:192-207, 411`), #22704 / #21735
(KIP-939 `keepPreparedTxn` is a stub, `TC:143-146`), #22937 (unclean election loses a marker, documented as inherent).

## 3. Deep analysis: verified candidates

Each candidate below was re-read against trunk. Where noted, I re-checked it myself in addition to the subagent's reading.

| ID | Finding | Evidence | Class |
|----|---------|----------|-------|
| F1 | A delayed epoch-e `sendTxnMarkersCallback` passes its epoch check, then unconditionally `put`s its `PendingCompleteTxn` over the entry installed by an epoch-(e+1) reload. Stale-epoch responses then `remove` it, and the e+1 completion finds no entry. The transaction is stuck in PrepareX with pending CompleteX until the next election. | `TC:960-1007`, `TMCM:281`, `TMCM:299-306`, `TMRCH:127-133`; re-verified | plausible race (MC) |
| F2/C2 | `onElection(e+1)` without a resignation leaves `(p,e)` in `loadingPartitions`. Load(e) waits for the HWM, so it can run after Load(e+1) and still pass the check at `TSM:577`, then `put` (`TSM:542`) its stale-epoch cache. The partition then answers NOT_COORDINATOR, and markers carry the old epoch. | `TSM:542, 577, 614-655`; re-verified | plausible race (MC) |
| C3 | `removeLoadingPartitionWithEpoch` uses `find`, so it removes at most one loading entry (`TSM:731-737`). A stale queued load can run after re-election, skipping the HWM wait. If it reads records above the HWM, this is the KAFKA-21349 class again. | `TSM:731-737` | plausible, possible safety (MC) |
| C1 | Expiration tombstone callback re-reads the current cache entry with no null or epoch check. After a same-broker reload, it can NPE or clear the pending state of the new object. | `TSM:264-291` | plausible race (MC) |
| C4 | Timeout abort re-checks only producerId (`TC:1087`). It can throw IllegalStateException when the txn is in PrepareX/CompleteX without a pending state, which skips the rest of the round. It can also fence a *new* ONGOING txn that has not timed out; the snapshot epoch is never compared. | `TSM:129-150`, `TC:1086-1099` | ISE: confirmed by reading; spurious fence: plausible (MC) |
| C5 | The fence window: a client EndTxn(ABORT) arriving between `prepareFenceProducerEpoch` and `endTransaction` consumes the fence (`TC:804, 847`; TV1 `TC:569-585`). The client gets epoch e+1. The fencer's later `endTransaction(e+1)` passes `TC:819/823` and may abort the client's next transaction. | `TC:288, 804-851, 1097` | plausible (MC) |
| C6 | Client EndTxn(COMMIT) while the fence is pending: `prepareTransitionTo` throws (`TM:319`), and the client gets UNKNOWN_SERVER_ERROR. | `TC:865, 874-881`, `TM:319-321` | confirmed by reading (test) |
| C7 | InitProducerId retry with expected epoch MAX-2 while the current epoch is MAX-1 throws at `TM:158` before reaching the retry branch `TM:174`, so the client gets UNKNOWN_SERVER_ERROR. | `TC:266-275`, `TM:158-181`; re-verified | confirmed by reading (test) |
| W1 | TV2 window between the PrepareAbort transition completing and `sendTxnMarkersCallback` setting pending CompleteAbort. A client COMMIT at e (the pre-abort epoch) gets `retryOnEpochBump=true` and then INVALID_TXN_STATE (`TC:917-918`), which is fatal for the client. KAFKA-20785 fixed only the CompleteAbort case. | `TC:809, 914-918, 950-976` | plausible (test / MC) |
| F3 | INVALID_PRODUCER_EPOCH or TRANSACTION_COORDINATOR_FENCED from a leader drops all remaining markers but leaves pending CompleteX at the same coordinator epoch, so the txn is wedged. The TV2 strict marker-epoch check (`PAI:137-144`) makes this reachable in principle. No concrete trigger found. | `TMRCH:160-167` | plausible (MC) |
| F5 | CompleteX append that fails with NOT_COORDINATOR (e.g. KAFKA_STORAGE_ERROR mapped) at an unchanged epoch: pending is cleared but nothing re-drives PrepareX. | `TMCM:314-332`, `TSM:779-847` | plausible (MC) |
| F4 | Any WriteTxnMarkers error outside the handled list throws at `TMRCH:178` -> FatalExitError on the sender thread. Concrete case: UNKNOWN_TOPIC_ID after a topic is deleted and recreated. | `TMRCH:178`, `KafkaApis:1859` | confirmed by reading (test) |
| F6 | Offline partition re-park and `wakeup()` busy loop (open PR #23993). | `TMCM:192-207, 411` | known, open (review) |

False positives / checked and fine (details in notes):
- A durable PrepareCommit can never become aborted through the coordinator's own paths. The outcome is always taken from the durable state (`TSM:583-591`, `TC:972-981`), and EndTxn(ABORT) in PrepareCommit is rejected (`TC:914-918`).
- Loaded PrepareX transactions keep their full partition set, because partitions are serialized for every non-Empty state (`TL:59-68`).
- PrepareEpochFence is never persisted (`TM:413-419`).
- Expiration only qualifies Empty/Complete* with no pending state. Pending DEAD blocks every request, and the id leaves the cache only after the tombstone is acked.
- TV2: late data at epoch e after an epoch-(e+1) marker is rejected by the partition leader. TV1: the marker clears the verification guard (`UL:2694-2702`).
- TxnMarkerQueue dead-queue race and `remove(k,v)` value equality: only old-epoch entries are affected.

Developer signals: there are no TODO/FIXME comments in the coordinator directories. The invariants are encoded as "should not happen" ISE checks instead (`TM:141, 242`, `TC:611, 931`).
