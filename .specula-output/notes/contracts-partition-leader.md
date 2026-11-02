# Contracts: Transaction coordinator <-> partition leader (Kafka trunk)

All paths relative to `/home/user/Specula/kafka`. Abbreviations:
KA = core/src/main/scala/kafka/server/KafkaApis.scala,
RM = core/src/main/scala/kafka/server/ReplicaManager.scala,
P = core/src/main/scala/kafka/cluster/Partition.scala,
UL = storage/src/main/java/org/apache/kafka/storage/internals/log/UnifiedLog.java,
PAI = storage/.../log/ProducerAppendInfo.java, PSE = storage/.../log/ProducerStateEntry.java,
PSM = storage/.../log/ProducerStateManager.java, VSE = storage/.../log/VerificationStateEntry.java,
TC = core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala,
TMCM = core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerChannelManager.scala,
TMRCH = core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerRequestCompletionHandler.scala,
TM = transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TransactionMetadata.java,
APTM = server/src/main/java/org/apache/kafka/server/transaction/AddPartitionsToTxnManager.java.

## 1. WriteTxnMarkers on the partition leader

Path: `KA.handleWriteTxnMarkersRequest` (KA:1755) -> per marker, per partition:
- Hosting check: `replicaManager.onlinePartition(p)`; if not Online -> `UNKNOWN_TOPIC_OR_PARTITION` (KA:1805-1810). No leader-epoch field in the request; the marker is appended to whatever leader is local.
- `__consumer_offsets` partitions go to `groupCoordinator.completeTransaction(...)` (KA:1840-1862), coordinator-load errors are mapped to `NOT_LEADER_OR_FOLLOWER` (KA:1853-1856). (Out of scope here.)
- Other partitions: build `MemoryRecords.withEndTransactionMarker(pid, marker.producerEpoch, EndTransactionMarker(type, marker.coordinatorEpoch))` (KA:1867-1871) and call `replicaManager.appendRecords(origin=COORDINATOR, requiredAcks=EnforcedRequiredAcks, transactionVersion=marker.transactionVersion)` (KA:1876-1889).
- RM.appendRecords -> appendRecordsToLeader -> appendToLocalLog (RM:638-662, 1376-1416) -> `getPartitionOrException` (RM:1414): not hosted but topic in metadata -> NOT_LEADER_OR_FOLLOWER (RM:528-533), else UNKNOWN_TOPIC_OR_PARTITION (RM:535-536), offline -> KAFKA_STORAGE_ERROR (RM:525-526). `Partition.appendRecordsToLeader` throws NotLeaderOrFollower if not leader (P:1248-1250) and NotEnoughReplicas if ISR < minISR with acks=-1 (P:1235-1240).
- UL.appendAsLeader -> append -> `analyzeAndValidateProducerState` (UL:1257-1259, 1402). Control batches skip verification-guard checks (UL:1480) and go to `UnifiedLog.updateProducers` (UL:1463-1469, 2685) -> `ProducerAppendInfo.append` -> `appendEndTxnMarker` (PAI:217-222, 271).

### Validation in `appendEndTxnMarker` (PAI:271-302)
- `transactionVersion == TV_UNKNOWN` is illegal for non-REPLICATION origins (IllegalArgumentException, PAI:283-286). Replication/recovery uses TV0 semantics (PAI:289; follower path UL:1093-1102; recovery `loadProducersFromRecords` passes `(short)0`, UL:2671-2677).
- Producer epoch, `checkProducerEpoch` (PAI:122-159), `current = updatedEntry.producerEpoch()`:
  - TV0/TV1: reject iff `markerEpoch < current` (accept `>=`) (PAI:124).
  - TV2 (`transactionVersion >= 2`): reject iff `markerEpoch <= current` (strictly greater required) (PAI:124), EXCEPT idempotent retry: `markerEpoch == current && (no ongoing txn || markerEpoch == Short.MAX_VALUE)` is accepted and the duplicate marker is WRITTEN (PAI:137-144).
  - Error: `InvalidProducerEpochException` -> INVALID_PRODUCER_EPOCH (PAI:156); for REPLICATION only a warn (PAI:150-151).
- Coordinator epoch, `checkCoordinatorEpoch` (PAI:258-269): reject iff `lastSeenCoordinatorEpoch > marker.coordinatorEpoch` (equal accepted) -> `TransactionCoordinatorFencedException` -> TRANSACTION_COORDINATOR_FENCED (PAI:264-266). REPLICATION: only logs (PAI:260-262).
  - Keyed per producerId (state is the `ProducerStateEntry` for that pid, PSE:40), NOT per transactionalId; per partition (one PSM per log).
- State update: `updatedEntry.update(markerEpoch, marker.coordinatorEpoch, ts)` (PAI:300 -> PSE:110-121) unconditionally overwrites coordinatorEpoch, clears `currentTxnFirstOffset`, updates epoch (clearing batch metadata if epoch changed, PSE:94-99).
- No ongoing txn for that pid on the partition: marker is still written to the log; `CompletedTxn` is empty so no txn-index / LSO effect (PAI:293-299). Producer epoch/coordinatorEpoch are still updated (PAI:300).
- Ongoing txn at a different (lower) epoch: TV1 marker with higher epoch completes it (CompletedTxn built from `currentTxnFirstOffset` regardless of epoch, PAI:296-298).
- Persistence: coordinatorEpoch and currentTxnFirstOffset are written to producer snapshots (PSM:676-683) and read back (PSM:653-657). After leader change the new leader has state built by replication (follower appends use updateProducers with REPLICATION, which never rejects, PAI:150-151/260-262, so a follower may even store a LOWER coordinatorEpoch, PAI:300).
- Producer expiration: `removeExpiredProducers` drops the whole entry (incl. coordinatorEpoch, epoch) when no ongoing txn and idle >= producerIdExpirationMs (PSM:329-339); also on snapshot load (PSM:303).
- TV field on the wire: WriteTxnMarkers v2 adds `TransactionVersion` (ignorable, default 0) (clients/src/main/resources/common/message/WriteTxnMarkersRequest.json:24-25,49-50). Coordinator sets it from `txnMetadata.clientTransactionVersion` (TMCM:350-358, 364).

### Coordinator reaction to leader errors (TMRCH:127-197)
- NONE -> remove partition from pending set (TMRCH:138-139).
- UNKNOWN_TOPIC_OR_PARTITION, NOT_LEADER_OR_FOLLOWER, NOT_ENOUGH_REPLICAS(_AFTER_APPEND), REQUEST_TIMED_OUT, KAFKA_STORAGE_ERROR -> retry with re-resolved leader (TMRCH:148-158, 186-196).
- INVALID_PRODUCER_EPOCH, TRANSACTION_COORDINATOR_FENCED -> `removeMarkersForTxn` and abort sending ALL remaining markers for this txn (TMRCH:160-167); completion is never written by this coordinator.
- UNSUPPORTED_FOR_MESSAGE_FORMAT/UNSUPPORTED_VERSION -> partition removed as if done (TMRCH:169-176).
- Partition with no leader endpoint at enqueue time -> treated as deleted, removed from pending set (TMCM:375-397).
- Coordinator epoch changed since marker enqueued -> cancel (TMRCH:127-133).

## 2. TV2 / KIP-890 verification and implicit add

- `RM.handleProduceAppend` collects (pid, epoch) of transactional batches; >1 pid -> InvalidPidMapping (RM:707-718). Produce v>11 => `ADD_PARTITION` (supportsEpochBump=true), v11 => GENERIC_ERROR_SUPPORTED, else DEFAULT_ERROR (APTM:98-105, 70-90).
- Skip verification if transactionalId null, no APTM, or (!supportsEpochBump && !transaction.partition.verification.enable) (RM:1011-1018). So TV2 clients always go through it; default config is true (TransactionLogConfig.java:52-53).
- Per partition: `Partition.maybeStartTransactionVerification` (P:483-487; NotLeaderOrFollower if not leader) -> `UL.maybeStartTransactionVerification` (UL:887-903):
  - `epoch < entry.producerEpoch` -> InvalidProducerEpochException (UL:890-894).
  - `hasOngoingTransaction(pid, epoch)` (entry exists, currentTxnFirstOffset present, epoch EQUAL, UL:933-937) -> SENTINEL (no verification needed).
  - else `PSM.maybeCreateVerificationStateEntry` (PSM:159-165): one VerificationStateEntry per pid with a fresh unique `VerificationGuard` (VerificationGuard.java: AtomicLong id; `verify` requires non-SENTINEL and equal). Entry tracks lowest seq and max epoch (VSE `maybeUpdateLowestSequenceAndEpoch`).
- APTM.addOrVerifyTransaction sends AddPartitionsToTxn with `verifyOnly = !supportsEpochBump` (APTM:233-262). Coalescing per txnId: same epoch -> old request gets NETWORK_EXCEPTION, higher incoming -> old gets INVALID_PRODUCER_EPOCH, lower incoming -> INVALID_PRODUCER_EPOCH (APTM:282-297).
- KA:1992-2006 dispatch: verifyOnly=false -> `TC.handleAddPartitionsToTransaction` (TV from request version >3 => TV2, TransactionVersion.java:67-70); verifyOnly=true -> `TC.handleVerifyPartitionsInTransaction`.
  - Verify (TC:362-413): pid mismatch -> INVALID_PRODUCER_ID_MAPPING; `producerEpoch != txn.producerEpoch` -> PRODUCER_FENCED (TC:386-387); state PREPARE_COMMIT/ABORT -> CONCURRENT_TRANSACTIONS (TC:388-389); partition not in `topicPartitions` -> TRANSACTION_ABORTABLE (TC:392-396). Pending transitions intentionally NOT checked (TC:380-382).
  - Add (TC:417-470): pendingTransitionInProgress -> CONCURRENT_TRANSACTIONS (TC:439-444); pid mismatch; epoch != -> PRODUCER_FENCED (TC:447-448); PREPARE_* -> CONCURRENT_TRANSACTIONS (TC:449-450); ONGOING & already contained -> NONE (TC:451-453); else prepareAddPartitions + append to txn log (TC:455, 462-466). COMPLETE_* / EMPTY with matching (bumped) epoch is allowed to start the next txn.
- TV2: CONCURRENT_TRANSACTIONS is retried inside the broker with backoff up to min(addPartitionsToTxnRetryBackoffMaxMs, requestTimeout) (RM:796-832). Error translation for older clients (RM:722-746).
- Append: `appendRecords(..., verificationGuards)` (RM:767-777) — note no transactionVersion passed for data (TV_UNKNOWN), data epoch check is TV-independent.
- In `analyzeAndValidateProducerState` for CLIENT/COORDINATOR origin (UL:1424-1455): if transactional && !hasOngoingTransaction(pid, batchEpoch):
  - `batchEpoch < entry.producerEpoch` -> InvalidProducerEpochException (UL:1442-1447).
  - `batchMissingRequiredVerification` (only when transactionVerificationEnabled; control batches exempt; guard must equal the stored one) -> InvalidTxnStateException (UL:1451-1453, 1478-1482).
- Data batch epoch check: reject iff `batchEpoch < current` (PAI:96-98, 124 with TV_UNKNOWN); first batch of a new txn sets currentTxnFirstOffset (PAI:251-255); non-transactional during txn -> InvalidTxnState (PAI:247-250).
- Guard clearing (UL:2694-2702, runs for every transactional batch incl. markers, on leader and follower): cleared after any transactional append EXCEPT when `entry.supportsEpochBump && batch.isControlBatch && batch.producerEpoch == entry.epoch` (TV2 "next transaction already started" case: client already verified with the bumped epoch E+1 and the marker for the previous txn carries E+1). Also expired by time in `removeExpiredProducers` (PSM:341-343). Not persisted; not cleared on leadership change (only `producers` cleared in `clearProducerIds`, PSM:150-153).

### What prevents data after the marker
- Coordinator: no verify/add succeeds while PREPARE_* (TC:388-389, 449-450); COMPLETE_* is written only after all markers are acked (TMCM:299-306).
- Leader, TV2: marker carries epoch E+1 (TM:241-249; marker epoch = `txnMetadata.producerEpoch` after the Prepare transition, TMCM:286-287). Late data at epoch E: rejected at verification start (UL:890-894) and at append (UL:1442-1447, PAI:124). Its guard (epoch E) is cleared by the marker (UL:2699-2701).
- Leader, TV1: marker epoch == data epoch, so the epoch check does not help; protection is only the guard: marker clears it (UL:2699-2701), so a late append with a pre-marker guard hits `verificationGuard(pid)=SENTINEL` -> InvalidTxnState (UL:1451-1453). Only if transaction.partition.verification.enable=true (UL:1479).
- A late produce with a valid guard CAN be appended if it reaches the log before the marker (that is "in the txn"). It cannot be appended after the marker unless the TV2 keep-guard exception applies (same epoch E+1 = next txn, which is intended).

## 3. coordinatorEpoch in markers
- Origin: leader epoch of the `__transaction_state` partition at election (BrokerMetadataPublisher.scala:301-302 -> TC.onElection TC:478-487 -> TransactionStateManager.loadTransactionsForTxnTopicPartition:555-557).
- EndTxn path: `addTxnMarkersToSend(coordinatorEpoch, ...)` after Prepare write, only if cached coordinatorEpoch still equals it (TC:960-993, 1007). Load path also calls it (TC:486-487).
- Stored in `PendingCompleteTxn` (TMCM:276-280) and put in each `TxnMarkerEntry` (TMCM:349, 364).
- Leader stores it per pid in `ProducerStateEntry.coordinatorEpoch` (PSE:40, set PSE:119 via PAI:300; initial -1, PSE:44-45), snapshotted (PSM:682).

## 4. Gaps / observations (contract-relevant)
1. Stale coordinator accepted after producer-state expiry: entry removed (PSM:329-339) -> coordinatorEpoch back to -1 and epoch NO_PRODUCER_EPOCH; a zombie coordinator's marker (any coordinatorEpoch, any epoch) passes PAI:124/259 and is written (no-op w.r.t. LSO). Same for a fresh replica of a log whose snapshot lacks the pid.
2. Equal coordinatorEpoch is accepted (PAI:259 uses `>`); fencing relies on strictly increasing __transaction_state leader epochs.
3. Followers never enforce epochs (PAI:150-151, 260-262) and overwrite coordinatorEpoch with whatever is in the log (PAI:300) -> a lower coordinatorEpoch can be installed on a follower and survive into its leadership.
4. Markers for txns the partition has no data for are written (PAI:293-300) and bump the stored producer epoch / coordinatorEpoch. With TV2, the idempotent-retry exemption (PAI:137-144) lets a duplicate marker at `epoch == current` be written whenever no txn is ongoing; `epoch == Short.MAX_VALUE` is accepted even with an ongoing txn.
5. TV mismatch/downgrade: TransactionVersion field is ignorable, default 0 (WriteTxnMarkersRequest.json:49-50); a TV2 marker delivered via v1 is validated with `>=` (PAI:124).
6. Data after marker in TV1 depends on `transaction.partition.verification.enable` (UL:1479; RM:1014). With it off, a late same-epoch transactional produce after the marker opens an orphan txn (hanging txn). For TV2 the guard check is also skipped when the config is off, but the epoch bump still rejects late data (except the Short.MAX_VALUE abort without bump, TM:242-245).
7. Coordinator abandons the whole txn on a single INVALID_PRODUCER_EPOCH / TRANSACTION_COORDINATOR_FENCED (TMRCH:160-167): other partitions may never get markers from this coordinator; txn stays PREPARE_* until coordinator reload.
8. Verification state is in-memory only and not cleared on leadership change (PSM:87, 150-153); a guard created on an old leadership stint survives until a transactional batch for the pid is applied or it expires (PSM:341-343).
