# Bug archaeology via GitHub PRs: Kafka transaction coordinator

Date: 2026-11-02. Trunk checked: `kafka/` @ 8aa57006ff (2026-11-02).

## Method and limitations

- Collection: `gh api search/issues` over `repo:apache/kafka is:pr` with 48 query terms
  (TransactionCoordinator, "transaction coordinator", TransactionStateManager, TransactionMarker,
  WriteTxnMarkers, PrepareEpochFence, "hanging transaction", CONCURRENT_TRANSACTIONS,
  "transactional id" expiration, KIP-890, "transaction version 2", TV2, "epoch overflow",
  "coordinator epoch", InitProducerId, EndTxn, keepPreparedTxn, pendingState, "txn marker",
  TransactionMetadata, "transaction marker", KIP-939, AddPartitionsToTxn, TransactionLog,
  PrepareCommit, PrepareAbort, TxnMarker, __transaction_state, INVALID_TXN_STATE, "epoch bump",
  "verification guard", AddPartitionsToTxnManager, 2PC, "two-phase commit", unload, plus
  `is:open` sweeps for transaction/txn/producer epoch/coordinator/marker), up to 300 results each.
  The local clone's `git log` on the coordinator paths was used as a cross-check and found 8 more
  relevant PRs (#22680, #19976, #18810, #23234, #14033, #20882, #19193, #8960).
- **Total unique PRs collected: 863** (80 open). About 190 touch the transaction path; the rest
  are search noise (Streams, share groups, consumer-group, KRaft).
- **Limitation:** `gh api repos/apache/kafka/...` (pulls, issue comments, review comments) and
  GraphQL return HTTP 403 in this session ("GitHub access to this repository is not enabled";
  `add_repo` was refused by the permission classifier). So "deep read" means: full PR body
  (from the search API `.body` field) plus the full squash-commit message in the local clone
  (it includes the `Reviewers:` line, which shows maintainer sign-off) plus a check of current
  trunk code. **Comment threads and review comments could not be read.** The comment counts
  that search reports are given where they matter.
- Deeply read: **64 PRs** (bodies in scratchpad `arch/b/*.txt`). Of these, **49 are confirmed
  correctness bugs** (merged with reviewers, or maintainers confirmed in the body), **10 are open**,
  and **15 were excluded** as out of scope, client-only, performance or not a bug (some are both
  open and excluded).
- Old PRs (2017–2018) show `merged=null` because committers then merged with the
  `Closes #N` squash script. Merge status was checked in `git log`.

## Per-PR table (deep read)

Columns: PR | KAFKA | state | summary / root-cause mechanism | confirmed | severity | fix in trunk

### A. Failed log append / pending state left set (CONCURRENT_TRANSACTIONS forever)

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #3184 | 5351 | merged (2017) | `appendTransactionToLog` returned an error without clearing `pendingState`, so every later request got CONCURRENT_TRANSACTIONS until restart. | Gustafson, Wang | High (liveness) | yes |
| #5820 | 7519 | merged 2018 | Expiration task (`transactionalId-expiration`) set pendingState=Dead. If the tombstone append failed, pendingState was never cleared and the txnId became "untouchable" (CONCURRENT_TRANSACTIONS). | yes | High (liveness) | yes |
| #8239 | 9666 | merged 2020 | InitProducerId fence bumped the in-memory epoch before the append. When the append failed (e.g. NOT_ENOUGH_REPLICAS) the epoch stayed bumped, and repeated retries exhausted the epoch. Fix: `hasFailedEpochFence` flag. | Gustafson | Med-High | yes |
| #13499 | 14880 | merged 2023 | First InitProducerId append failed, so the metadata kept epoch -1 forever. Expiration then threw `IllegalArgumentException: Illegal new producer epoch -1` in prepareTransitionTo, which crashed the scheduled task. | yes | Med | yes |
| #19910 | 19367 | merged 2025 | TV2: InitProducerId fence bumped the epoch, then the abort bumped it again (double increment). At epoch 32766 this overflowed to negative and threw IllegalArgumentException. | Olshan, Livshits | High (TV2) | yes |
| #19976 | 19367 | merged 2025 | Follow-up: removed `hasFailedEpochFence=true` on the TV2 path. Expected sequence: append fails, retry does a single bump and COMPLETE_ABORT, CONCURRENT_TRANSACTIONS, then the client retries into EMPTY. | yes | Med | yes |

### B. Marker completion races (duplicate completion, stale completion removal, stuck PrepareX)

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #3192 | 5357 | merged 2017 | Recursive `appendToLogCallback` retries caused StackOverflow. Replaced with a retry queue drained in the send thread. | yes | Med | yes |
| #4140 | 6131 | merged 2017 | Non-atomic creation of marker queues lost markers. Fix: `putIfAbsent`. | Gustafson, Wang, Juma | High (lost markers, hanging txn) | yes |
| #5036 | 6917 | merged 2018 | Deadlock between txn completion (delayed op) and state lock. Completion was made asynchronous. | Wang, Gustafson | High | yes |
| #8389 (supersedes #8377) | 9777 | merged 2020 | DelayedTxnMarker purgatory plus the shared read lock meant `checkAndComplete` might never complete, so the txn hung in PrepareX. Purgatory replaced with the `transactionsWithPendingMarkers` map. | yes | High | yes |
| #8782 | 10080 | merged 2020 | `maybeWriteTxnCompletion` was not safe for concurrent calls. A duplicate CompleteX append could happen, causing IllegalState and a correctness violation if a new txn had already started. Fix: only the thread that removes the map entry writes. | Gustafson | **High (correctness)** | yes |
| #15524 | 16352 | merged 2024 | `transactionsWithPendingMarkers.remove(txnId)` was not value-checked, so a stale operation from an older epoch removed the newer entry and the txn was stuck in PrepareCommit/PrepareAbort. Fix: `remove(key, value)`. | yes (6 comments) | High (hanging txn) | yes |
| #8336 | 9749 | merged 2020 | `TransactionMarkerRequestCompletionHandler` threw IllegalStateException on KAFKA_STORAGE_ERROR and did not retry, so the txn was stuck in PendingCommit/PendingAbort. | yes | High | yes |
| #21176 | 19999 | merged 2025 | TV2 strict marker epoch check (`markerEpoch > currentEpoch`) rejected an idempotent marker retry (after coordinator reload of PrepareX, or a lost response) with InvalidProducerEpoch, so the txn hung in PREPARE forever (livelock). Fix: accept same-epoch marker when TV≥2 and no ongoing txn. | Olshan, Tsai | **High (hanging txn)** | yes |
| #20882 | 19446 | merged 2025 | KIP-1228: propagate the transaction version in WriteTxnMarkers so the partition leader picks strict (TV2) or `>=` (TV1) epoch validation. Before this, the leader guessed. | yes | Med | yes |
| #23993 | 20734 | **OPEN** | See open list: busy loop on the unknown-broker marker queue. | — | Low-Med (CPU) | no |

### C. Load / unload / coordinator epoch / stale cache

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #2926 | 5131 | merged 2017 | Initial: on immigration, resend markers for PrepareX loaded from log. | yes | design | yes |
| #3082 | 5280 | merged 2017 | The check of isCoordinator/loading and the metadata read were not atomic, so the epoch could change between them (emigration then immigration) during an append. Read-write lock held across append. | yes (27 comments) | High | yes |
| #7840 | 9307 | merged 2019 | A failed become-follower left stale metadata, and a later reload refused to overwrite an older epoch entry. Loading now overwrites. | yes | High | yes |
| #7963 | 9235 | merged 2020 | Leader demoted and removed from replicas in the same reassignment: StopReplica did not clear the txn coordinator cache, so a stale coordinator kept serving. | Gustafson | High | yes |
| #15139 (part of #14489) | 15468 | merged 2024 | Partition-epoch bumps (ISR change) triggered a full coordinator reload. Now only leader-epoch bumps do. **Part 2 of #14489 (skip reload when re-elected as the same leader) was dropped.** Trunk still unloads and reloads on every leader-epoch bump (`TransactionCoordinator.scala:478-488`). | yes | Med | partial |
| #18011 | 18137 | merged 2024 | `removeTransactionsForTxnTopicPartition(p, epoch)` removed a loadingPartitions entry of a *newer* epoch, so the newer load was then discarded. Fix: remove only if epoch < new. | yes | High (load lost; NOT_COORDINATOR loop) | yes |
| #17688 / #20529 | 17941 | merged 2024 | A segment ending with an empty batch did not advance currOffset, so `loadTransactionMetadata` looped forever (load hangs). | yes | High | yes |
| #23630 (+4.0–4.3 backports) | 21181 | merged 2026-10 | After losing leadership of a `__transaction_state` partition, the cache stayed until `onResignation`. Decisions made from the cache alone (TV2 EndTxn COMMIT) returned fatal INVALID_TXN_STATE instead of NOT_COORDINATOR. Fix: use a cache entry only if the local replica still leads at the entry's coordinator epoch (`TransactionStateManager.scala:405`). | Olshan, Livshits | **High** | yes |
| #23831 (+backports) | 21181 | merged 2026-10 | Same race with metadata still behind: TV2 EndTxn(COMMIT) at current epoch in Empty/CompleteX returned INVALID_TXN_STATE. Now commits an empty txn (epoch bump, PrepareCommit with no partitions). Relies on acks=-1 so a stale coordinator cannot get the write replicated. | Olshan, Livshits | High | yes |
| #23943 | 21349 | merged 2026-10-27 | **Load read past the HWM:** the new leader loaded records above HW that were later truncated. The coordinator sent COMMIT markers for a PREPARE_COMMIT that the next leader lacked, and that leader timed out and ABORTed: **atomicity violation (committed on some partitions, aborted on others).** Fix: wait for HW ≥ LEO-at-load-start. Also skip expiration of loading partitions. | Olshan | **Critical (correctness)** | yes (very recent) |
| #6110 | 7771 | closed, unmerged | ZK-era: coordinators should recompute the `__transaction_state` partition count. Superseded/obsolete in KRaft. | — | — | n/a |

### D. Epoch fencing races with InitProducerId / EndTxn / timeout abort

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #3066 | 5231 | merged 2017 | InitPid aborting an ongoing txn must bump the epoch on the abort markers, so the zombie is fenced. | Gustafson, Rao | High | yes |
| #4137 | 6119 | merged 2017 | Timeout abort did not bump the epoch, so a zombie producer could keep writing into the aborted txn. Fix: bump the epoch on expiration abort (PrepareEpochFence origin). | Gustafson, Wang | **High (correctness)** | yes |
| #8164 | 9600 | merged 2020 | EndTxn accepted `epoch+1` from clients (meant only for coordinator retry), which is too permissive. Strict check when from client. | Gustafson | Med | yes |
| #18467 | 18464 | merged 2025 | TV2: commit/abort followed by an empty abort. The epoch check ran before the pending-state check, so the producer was self-fenced. Fix: return CONCURRENT_TRANSACTIONS when pendingState is set, before the epoch check. | yes | High (TV2) | yes |
| #20534 (+#20577) | 19690 | merged 2025 | After a timeout abort, a stalled producer's produce returned InvalidTxnState (fatal) instead of InvalidProducerEpoch, because the verification guard was checked before the epoch. | Olshan | Med (fatal client error) | yes |
| #23204 | 20785 | merged 2026-08 | TV2: EndTxn(COMMIT) racing with a coordinator-side timeout abort arrives with the pre-abort epoch, which matches the TV2 retry condition (stored == req+1). It then hits COMPLETE_ABORT and gets INVALID_TXN_STATE (fatal). Fix: PRODUCER_FENCED. | Olshan, Huang, Magrans | Med-High | yes |

### E. TV2 (KIP-890) epoch bump / overflow / producerId rotation / persistence

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #3183 | 5283 | merged 2017 | Epoch overflow: generate a new producerId when the epoch would overflow. | yes | High | yes |
| #17849 | 17898 | merged 2024 | Refined `canEpochBump` across TV0/TV1/TV2 combinations. | yes | Med | yes |
| #18176 | 18227 | merged 2024 | Upgrade to TV2 mid-transaction: v2 partitions were added to the last v1 txn. Fix: epoch bump on upgrade. | yes (8 comments) | High (upgrade) | yes |
| #18448 | 18401 | merged 2025 | TV2 commit with no records was an error. Client fix. | yes | Low | yes |
| #18730 / #18758 | 18660 | merged 2025 | TV2 epoch overflow returned the wrong producerId/epoch (typo). The concurrent-txn handling was then rearranged to avoid self-fencing with the new PID. | Livshits, Kim | **High** | yes |
| #21469 | 20090 | merged 2026-03 | Epoch reaching Short.MaxValue threw. Now logs and allows markers at MAX epoch so the txn can finish, then rotates PID. | Olshan, Tsai | High | yes |
| #21506 | 20090 | merged 2026-03 | TV2: timeout at epoch MaxValue-1, then fence bump to MaxValue. A late client EndTxn(ABORT) was treated as a retry, and `!isEpochFence` skipped `nextProducerId` allocation, so the txn was **stuck in ONGOING**. | Olshan, Livshits | High | yes |
| #21828 | 20310 | merged 2026-03 | `valueToBytes` never wrote PreviousProducerId/NextProducerId, so they were lost on failover. Two effects: stuck at an exhausted epoch (no rotation) and a spurious PRODUCER_FENCED on retry. | Olshan | **High (failover)** | yes |
| #22096 | 20357 | merged 2026-09 | `lastProducerEpoch` was not persisted, so an InitProducerId retry after failover during PID rotation was rejected with PRODUCER_FENCED. **NextProducerEpoch (tag 3) is still not read back** (`readTxnRecord` hardcodes NO_PRODUCER_EPOCH per #21828 body). #21735 (open) adds it. | Olshan (13 comments) | Med | yes (partial) |

### F. Verification / AddPartitionsToTxn races (KIP-890 part 1 and 2, partition-leader side)

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #13579 | 14904 | merged 2023 | Verification returned CONCURRENT_TRANSACTIONS while the coordinator had a pendingState for another partition, which led to OutOfOrderSequence loops. Verification is now allowed in pending Ongoing. The PR also restored locking in addPartitions/verify that had been removed. | yes | Med | yes |
| #13787 (redo of #13608) | 14884 | merged 2023 | Verification said "in txn", but an abort marker was written before the append, so data was appended to an already-aborted txn (**hanging/ghost txn**). Fix: VerificationGuard object checked under the log lock. | yes | **High (correctness)** | yes |
| #14033 | 14920 | merged 2023 | Timeouts during verification caused out-of-order sequence loops. Track the earliest seq and latest epoch in the verification state. | yes | Med | yes |
| #14774 | 15784 | merged 2023 | TxnOffsetCommit verification: in-memory update and write were not atomic. Callback after verification. | Livshits, Gustafson, Jacot, Rao | High | yes |
| #18604 | 18575 | merged 2025 (reverted by #18720) | TV2 race: the txn was still completing when the leader checked whether to add the partition, and had completed by the time the coordinator saw it, so the add went to the wrong txn. Fix by early CONCURRENT_TRANSACTIONS. | yes | **Critical (TV2 correctness)** | replaced |
| #18720 + #18810 | 18654 | merged 2025 | Reverted the early return for performance and fixed the verification-guard handling instead. Server-side retry of add-partitions on CONCURRENT_TRANSACTIONS. | yes | High | yes |
| #18733 | 18662 | merged 2025 | Return CONCURRENT_TRANSACTIONS on produce in TV2 (was NOT_ENOUGH_REPLICAS). | Olshan | Low | yes |
| #19844 | 18042 | merged 2025 | Reject produce with a lower epoch early (TV2). | yes | Low | yes |
| #19902 | 18202 | merged 2025 | TV2: reject a non-zero first sequence with empty producer state. | yes | Med | yes |
| #23234 | 15591 | merged 2026-09 | Same for all producers on never-written partitions (silent data loss with idempotence). Partition-leader side. | Rao | High (adjacent) | yes |

### G. Expiration races

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #3101 | 5279 | merged 2017 | Introduced transactionalId expiration (Dead tombstone). | yes | design | yes |
| #5820 | 7519 | (see A) | Expiration append failure left pendingState. | | | |
| #13499 | 14880 | (see A) | epoch -1 expiry crash. | | | |
| #7687 / #8960 | 9144 | merged 2020 | Producer-state expiry on the partition leader ignored marker timestamps, so the coordinatorEpoch could go backwards and log recovery validation failed. | Gustafson | High | yes |
| #23943 | 21349 | (see C) | Expiration of a partition being reloaded wrote tombstones that moved LEO during the HWM wait. Now skipped. | | | |

### H. Marker retries to removed / offline partitions, leader changes

| PR | KAFKA | State | Mechanism | Conf. | Sev. | Fixed |
|---|---|---|---|---|---|---|
| #3130 | 5202 | merged 2017 | Topic deleted while markers were pending: if no leader is known, drop the partition from metadata and complete. If leader known but node unavailable, park in the unknown-broker queue. Caveat: delete-and-recreate. | yes (14 comments) | High | yes |
| #22680 | 20734 | merged 2026-10-27 | **KRaft metadata cache regression:** `getPartitionLeaderEndpoint` returned empty (not `noNode`) for an existing partition whose leader is offline. `addTxnMarkersToBrokerQueue` treated it as deleted and **dropped the markers, then wrote CompleteCommit**, leaving the offline partition with an un-terminated txn (hanging txn / LSO stuck, atomicity loss). Fix: return noNode. | Wu, Olshan | **Critical** | yes (not released) |
| #23993 | 20734 | OPEN | Follow-up busy loop (see open list). | | | |
| #22937 | 20716 | OPEN (docs+test) | Unclean leader election can drop the COMMIT/ABORT marker, leaving read_committed consumers stuck at LSO. Maintainers: inherent, document it. | Chen (per body) | High but by design | no (docs) |

## Open PRs with bug-fix intent (known but unfixed)

| PR | KAFKA | Bug | Still in trunk? |
|---|---|---|---|
| **#22704** | 20739 | The coordinator rejects every `InitProducerId(keepPreparedTxn=true)` with UNSUPPORTED_VERSION (stub), so a 2PC (KIP-939) producer cannot recover a prepared txn after a crash. | **Yes**: `core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala:143-146` (`else if (keepPreparedTxn) { ... UNSUPPORTED_VERSION`). |
| **#21735** | 15370 | Same feature, larger design (dual identity: frozen ongoing PID/epoch for markers and client-facing next PID/epoch). Eagerly returns CONCURRENT_TRANSACTIONS in PrepareX. Persists NextProducerEpoch. | Yes: stub as above. NextProducerEpoch not restored on load (per #21828 body). |
| **#23993** | 20734 | After #22680, markers for an offline partition stay in `markersQueueForUnknownBroker`. Each `generateRequests()` drains them and calls `addTxnMarkersToBrokerQueue`, which re-parks them and always calls `wakeup()`, so the sender thread busy-spins while the partition is offline. | **Yes**: `TransactionMarkerChannelManager.scala:192-207` drains and re-adds, and `:411` calls `wakeup()` unconditionally. |
| #22937 | 20716 | Unclean election can lose the txn marker, leaving read_committed stuck at LSO. Docs and test only. | Yes, inherent (no reconciliation). |
| #21672 | 20077 | Client: AddPartitionsToTxn re-enqueues forever on retriable errors (e.g. topic deleted), so `flush()` hangs. | Yes: `clients/.../TransactionManager.java:1633-1643` (`reenqueue()` with no deadline). |
| #21715 / #23547 | 20237 | Client: an idempotent producer's InitProducerId is dequeued and then an SSL auth failure occurs. `authenticationFailed` iterates an empty queue, so the producer is stuck in INITIALIZING. | Yes: `TransactionManager.java:696` (skips if INITIALIZING) and `:967-970`. |
| #23857 | 21132 | Client: `close()` with an open txn retries EndTxn(abort) forever if the coordinator is unreachable. | Yes: `clients/.../Sender.java:267-277` (no deadline). |
| #23853 | 14359 | Client: the next batch is sent before the first batch of the epoch is acked, which leads to OOSN and data loss. Partially mitigated broker-side by #23234 (never-written partitions) and #19902 (TV2). | Partly. |
| #23301 | 20036 | Log cleaner overflow re-filtering corrupts txn metadata (aborted batches kept, markers dropped early). | Storage, out of scope here. Relevant to `kafka-log-cleaner.md`. |

Closed-unmerged items that point to residual issues:
- #14489 part 2 (KAFKA-15468): a full reload happens even when re-elected as the same leader. Not a correctness bug; reload churn widens load/unload windows.
- #8765 (KAFKA-10073/9878): "endTxn wait until markers complete" design, never merged. EndTxn still returns after PrepareX is written, which is the root of the TV2 "completing while new txn starts" races (#18604, #18720, #18810).
- #15078 (KAFKA-16047): fenceProducers timeout. Feature, abandoned.

## Mechanism groups (for modeling)

1. **Failed log append leaves in-memory state inconsistent** (pendingState, bumped epoch, epoch -1, double bump): #3184, #5820, #8239, #13499, #19910, #19976. Invariant: after any append failure, `pendingState = None` and the in-memory epoch is the persisted epoch, or the `hasFailedEpochFence` path. Liveness: no permanent CONCURRENT_TRANSACTIONS.
2. **Marker completion concurrency** (duplicate CompleteX, stale removal, lost queues, deadlock): #4140, #5036, #8389, #8782, #15524, #3192. Invariant: exactly one CompleteX per PrepareX and epoch. Map removal is value-keyed.
3. **Load/unload/coordinator epoch/stale cache:** #3082, #7840, #7963, #18011, #17688, #15139, #23630, #23831, **#23943 (load above HWM, so commit/abort split)**. Invariant: a coordinator acts only on committed (≤HW) log state and only while leader at its coordinator epoch.
4. **Epoch fencing races** (InitProducerId vs ongoing, timeout abort vs EndTxn/produce): #3066, #4137, #8164, #18467, #20534, #23204. Invariant: zombie writes after fence are rejected. Errors are recoverable (not INVALID_TXN_STATE) for coordinator-initiated aborts.
5. **TV2 epoch bump/overflow/PID rotation and persistence across failover:** #3183, #18176, #18730, #19910, #21469, #21506, #21828, #22096. Invariant: the epoch never exceeds Short.MaxValue. After failover the prev/next PID and lastEpoch are recovered, so retries are idempotent.
6. **Verification / TV2 implicit add-partition races** (partition leader and coordinator): #13579, #13787, #14033, #14774, #18604, #18720, #18810, #19844, #19902, #23234. Invariant: no data appended to a txn that is already decided (VerificationGuard).
7. **Marker delivery to removed/offline partitions and marker retry idempotence:** #3130, **#22680 (offline treated as deleted, so markers dropped)**, #8336, #21176, #20882, #23993, #22937. Invariant: CompleteX is written only after every partition that still exists has its marker. A marker retry at the same epoch is accepted.
8. **Expiration races:** #3101, #5820, #13499, #7687, #23943 (expire while loading).

Highest-value modeling targets, given how recent and severe the fixes are: (3) HWM-gated load and stale-cache answers, (7) offline vs deleted partition marker skipping, (5) persistence of rotation fields across failover, (2) completion map semantics. 2PC keepPreparedTxn is still unimplemented in trunk, so it cannot be validated.

## Excluded (not coordinator correctness bugs) and why

- #22716 KAFKA-20753: Streams transactional state store status marker. Unrelated to the txn coordinator.
- #21161 KAFKA-19925: a system-test workaround (new producer/txnId). Closed. Not a product fix; the root cause was not identified in the PR.
- #10880 KAFKA-12870: client RecordAccumulator flush counter. Client performance.
- #9632 KAFKA-10702: transaction index bookkeeping. Performance.
- #18720 (perf half) / #18733 / #19844: error-code and performance refinements (listed for context only).
- #5514 KAFKA-7296: client TxnOffsetCommit retry of COORDINATOR_LOAD_IN_PROGRESS. Client.
- #15182 KAFKA-16118: new *group* coordinator unload with empty epoch. Not the txn coordinator.
- #23301 KAFKA-20036: log cleaner. Belongs to the log-cleaner target.
- #22703 KAFKA-20738: client version negotiation for 2PC.
- #8765, #15078, #6110, #4121 (KAFKA-6042 fix landed via #4103 in the group coordinator), #2923: closed unmerged or superseded. #2923 could not be matched to a commit.
- #18448 KAFKA-18401: client-only empty-commit handling.
- #21672 / #21715 / #23547 / #23857 / #23853: real but client-side. Listed in the open table as adjacent, and excluded from coordinator-model scope.
