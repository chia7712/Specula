# Deep analysis: transaction marker pipeline (TxnMarkerChannelManager & co.)

Scope: `kafka/` trunk clone (read-only). Abbreviations used for file:line citations:

- MCM = `core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerChannelManager.scala`
- RCH = `core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerRequestCompletionHandler.scala`
- TSM = `core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala`
- TC  = `core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala`
- TMQ = `transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TxnMarkerQueue.java`
- TM  = `transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TransactionMetadata.java`
- PAI = `storage/src/main/java/org/apache/kafka/storage/internals/log/ProducerAppendInfo.java`
- BMP = `core/src/main/scala/kafka/server/metadata/BrokerMetadataPublisher.scala`

## 1. Pipeline overview (what the model needs)

1. EndTxn (TC:773-1036). Under the `txnMetadata` lock it decides and calls `prepareAbortOrCommit`.
   This sets `pendingState = PrepareX` (TM:318-345). It then calls `appendTransactionToLog` (TC:1033).
2. First append completes. `updateCacheCallback` (TSM:765-867) runs `completeTransitionTo(PrepareX)`
   under the txn lock, after a fresh `getTransactionState` plus an epoch check. Then
   `sendTxnMarkersCallback` (TC:950-1030) runs:
   - A: `getTransactionState` and an epoch check (TC:952-960).
   - B: `inLock { prepareComplete }`, which sets `pendingState = CompleteX` (TC:962-981, TM:265-288).
   - C: respond NONE to the client (TC:1005).
   - D: `addTxnMarkersToSend` (TC:1007). TV1 has the same path at TC:625-681.
3. `addTxnMarkersToSend` (MCM:271-289):
   - D1 does `transactionsWithPendingMarkers.put(txnId, pending)`. The put is unconditional and only logs "Replaced".
   - D2 does `addTxnMarkersToBrokerQueue` (MCM:339-412). It groups partitions by leader:
     - A known node goes to the per-broker `TxnMarkerQueue[txnTopicPartition]`.
     - `Node.noNode` goes to the unknown-broker queue.
     - `None` (topic or partition absent from this broker's metadata image) removes the partitions
       from the txn and calls `maybeWriteTxnCompletion` (MCM:375-407).
   - D3 runs `maybeWriteTxnCompletion`.
4. Sender thread `generateRequests` (MCM:192-228):
   - `retryLogAppends`.
   - Drain the unknown-broker queue and re-route it.
   - Drain every per-broker queue into one WriteTxnMarkers request per broker.
5. Completion handler (RCH:37-204), per entry:
   - `getTransactionState`, then an epoch check.
   - `inLock { removePartition / classify errors }`.
   - Then either re-enqueue the retry partitions or call `maybeWriteTxnCompletion`.
6. `maybeWriteTxnCompletion` (MCM:299-306). If `txnMetadata.topicPartitions` is empty and
   `CHM.remove(txnId, pending)` succeeds, it calls `writeTxnCompletion` (MCM:230-269). That checks
   the epoch again and calls `tryAppendToLog` (MCM:308-337), the second append of CompleteX.
7. Load (TSM:555-659):
   - `loadingPartitions += (p, e)`.
   - Wait for HWM >= LEO (listener), then run the scheduler task.
   - The scheduler task holds the stateLock WRITE lock (TSM:576-611) and:
     - installs the cache,
     - for every PrepareX: `prepareComplete` (pendingState = CompleteX),
     - removes `(p, e)` from loading,
     - calls `addTxnMarkersToSend` inside the write lock (TSM:603-606).
8. Unload (TSM:698-738) and `TC.onElection` / `onResignation` (TC:478-506):
   - `removeMarkersForTxnTopicPartition` (MCM:414-430) runs from the metadata publisher thread.
     It runs before loading on election and after cache removal on resignation.
   - `BMP:301-305`: a leader-epoch bump while this broker stays leader calls `onElection(p, e+1)`
     with NO resignation.

There is NO periodic re-drive of PrepareCommit/PrepareAbort:
- `timedOutTransactions` only considers ONGOING with no pending state (TSM:129-149).
- EndTxn in PrepareX returns CONCURRENT_TRANSACTIONS or INVALID_TXN_STATE (TC:914-923).
- InitProducerId in PrepareX, or with any pendingState, returns CONCURRENT_TRANSACTIONS (TC:249-260).

So a txn is only finished in two ways:
- (i) the in-memory pipeline above reaches `writeTxnCompletion`, or
- (ii) a re-load of the txn partition at a higher epoch.

Any path that drops the pipeline while this broker stays coordinator at the same epoch leaves the
txn stuck until the next leadership change of that txn partition.

## 2. Atomicity boundaries (for TLA+ action granularity)

Threads:
- request-handler and purgatory threads: EndTxn and append callbacks.
- `TxnMarkerSenderThread`, a single thread: `generateRequests`, all RCH callbacks, `retryLogAppends`,
  and append callbacks that complete inline.
- the `transaction-log-manager-` scheduler, a single thread (`BrokerServer.scala:423`): load tasks,
  abandon-load, timeout aborts and expiration.
- the metadata publisher thread: `onElection` / `onResignation`.
- the replica/HWM thread: HWM listener callbacks, which schedule loads.

| Step | Locks held | Atomic w.r.t. |
|---|---|---|
| `getTransactionState` (TSM:414-445) | stateLock.read | load install / unload (write). Its result is a snapshot that may be stale right after return. `isCoordinatorFor` reads ReplicaManager leadership (TSM:403-405). |
| Load install + send markers (TSM:576-611) | stateLock.write, plus txn locks inside (TSM:582) | everything that reads the cache. `sendTxnMarkers` runs inside the write lock: it enqueues, can do `maybeWriteTxnCompletion`, and can even call `appendRecords` (empty-partition txn, or a `None` leader). This takes the txn lock and calls `appendRecords` while holding stateLock.write, against the documented order (TSM:61-69). It does not deadlock today because no other thread can reach the freshly installed objects. |
| Unload (TSM:698-738) | stateLock.write | cache readers. The markers are removed afterwards and separately (TC:505), with no lock. |
| `appendTransactionToLog` pre-check + local append (TSM:871-912) | stateLock.read across the epoch check and `replicaManager.appendRecords` | load/unload: no unload between the check and the local append. |
| `updateCacheCallback` (TSM:765-867) | separate `getTransactionState` (read) then `txnMetadata.inLock` without stateLock | the cache snapshot can be stale, but it then mutates a detached object, which is harmless. |
| EndTxn prepare (TC:798-940) | txn lock | each txn's own fields. |
| `sendTxnMarkersCallback` A / B / C / D | A: read lock. B: txn lock. C and D: no lock | **not atomic as a whole**. D1, D2 per partition group, and D3 are each separate steps. |
| `TxnMarkerQueue.addMarkers` vs `removeMarkersForTxnTopicPartition` (TMQ:57-80 / 53-55) | CHM + LinkedBlockingQueue only | a lost marker in a "dead queue" is possible (acknowledged in TMQ:64-79). |
| `transactionsWithPendingMarkers` (MCM:140) | CHM | `put` is unconditional. `remove(k, v)` is identity-or-equals and conditional. |
| RCH per entry (RCH:94-201) | read (snapshot), then txn lock for `removePartition`, then no lock for re-enqueue or `maybeWriteTxnCompletion` | three separate steps. All RCH callbacks are serialized on the sender thread. |
| `maybeWriteTxnCompletion` (MCM:299-306) | txn lock for the emptiness check, then CHM.remove | at most one `writeTxnCompletion` per pending object, guaranteed by the CHM conditional remove. |

Suggested TLA+ actions:
- `EndTxnPrepare`
- `AppendPrepareReplicated`
- `SendMarkersCheck` (A+B)
- `RespondClient`
- `PutPending` (D1)
- `EnqueueMarkers` (D2)
- `MaybeComplete` (D3)
- `SenderDrain`
- `BrokerWriteMarker(p)` (partition-leader side, epoch checks per PAI:122-160, 258-268)
- `MarkerResponse(entry)`, with sub-steps for the epoch check, then `removePartition`, then enqueue/complete
- `WriteCompletionAppend`
- `CompletionAppendReplicated(err)`
- `RetryLogAppend`
- `Elect(p, e)` (remove queues, then `loading += (p, e)`)
- `HwmReached(p, e)` (schedule load)
- `LoadInstall(p, e)` (the atomic write-lock block)
- `Resign(p, e)`
- `EpochBumpNoResign`

## 3. Error-code handling (WriteTxnMarkers, per partition, RCH:136-180)

| Error | Action | Comment |
|---|---|---|
| NONE | `removePartition` | |
| UNKNOWN_TOPIC_OR_PARTITION, NOT_LEADER_OR_FOLLOWER, NOT_ENOUGH_REPLICAS(_AFTER_APPEND), REQUEST_TIMED_OUT, KAFKA_STORAGE_ERROR | retry: re-enqueue with re-resolved leader (RCH:186-196) | Retries forever. If the topic is deleted, the next leader resolution returns `None`, which drops the partition (MCM:375-399). |
| INVALID_PRODUCER_EPOCH, TRANSACTION_COORDINATOR_FENCED | `removeMarkersForTxn` + abortSending (RCH:160-167) | Drops the pipeline and leaves `pendingState=CompleteX` with state PrepareX, so the txn is stuck (see F3). |
| UNSUPPORTED_FOR_MESSAGE_FORMAT, UNSUPPORTED_VERSION | `removePartition` (treated as done) | Marks the partition complete without a marker. Not reachable on 4.x brokers. |
| CORRUPT_MESSAGE, MESSAGE_TOO_LARGE, RECORD_LIST_TOO_LARGE, INVALID_REQUIRED_ACKS | throw ISE | On the sender thread this becomes FatalExitError and `Exit.exit` (InterBrokerSendThread pollOnce, ShutdownableThread:137-141). |
| anything else (e.g. UNKNOWN_TOPIC_ID, UNKNOWN_SERVER_ERROR, INVALID_TXN_STATE, OUT_OF_ORDER_SEQUENCE...) | throw ISE, so the coordinator broker exits | See F4. |
| disconnect / request timeout (RCH:40-87) | re-enqueue all partitions if the epoch is unchanged, else drop | |
| version mismatch (NetworkClient:658-664 creates a ClientResponse with null body, not disconnected) | NPE at RCH:91-93, so FatalExitError | Only reachable if the destination lacks WriteTxnMarkers v1+. Not reachable in 4.x clusters, which require IBP >= 3.3. Robustness note only. |

Second append (CompleteX) results, from `tryAppendToLog` (MCM:308-337) and `updateCacheCallback` (TSM:765-867):
- NONE: `completeTransitionTo`. If the epoch changed in between, it maps to NOT_COORDINATOR and only logs.
- COORDINATOR_NOT_AVAILABLE (UNKNOWN_TOPIC_OR_PARTITION, NOT_ENOUGH_REPLICAS*, REQUEST_TIMED_OUT):
  `retryOnError = true`, so the pendingState is kept and the append goes to `txnLogAppendRetryQueue`.
  It is retried on each `generateRequests`, forever, with no backoff. There is no `wakeup()` at
  MCM:323, so when the thread is idle a retry waits up to `request.timeout.ms`
  (NetworkClient.poll caps at defaultRequestTimeoutMs). That is latency, not a hang.
- NOT_COORDINATOR (NOT_LEADER_OR_FOLLOWER, KAFKA_STORAGE_ERROR, or an epoch mismatch): if the local
  cache still has the same epoch, the pendingState is CLEARED (TSM:846) and the append only logs.
  The txn stays in PrepareX with no pendingState and no pipeline. It relies on the subsequent
  resignation and reload (F5).
- COORDINATOR_LOAD_IN_PROGRESS: only from the pre-check, when a re-load at a higher epoch is
  running. That load re-sends the markers. This is correct.
- other (UNKNOWN_SERVER_ERROR from MESSAGE_TOO_LARGE, INVALID_*, ...): the pendingState is cleared
  (TSM:846), then MCM:329-332 throws ISE inside the append callback. On the sender thread this means
  JVM exit. On a request-handler or purgatory thread the exception is logged and the txn stays in
  PrepareX until a reload (F5).

## 4. Findings

### F1 (b, HIGH VALUE for model checking): a stale `put` after a same-broker re-election leaves the new-epoch txn stuck in PrepareX with pendingState=CompleteX

Path:
1. A thread T is in `sendTxnMarkersCallback` for txn X at coordinator epoch e. It passed the epoch
   check (TC:952-960) and `prepareComplete` (TC:976) on the old object O_e, and is now between
   TC:960 and the `put` at MCM:282 (it holds no lock).
2. A leader-epoch bump of the txn partition happens on the same broker. `BMP:301-302` calls
   `onElection(p, e+1)` without resignation, which runs `removeMarkersForTxnTopicPartition` (still
   empty for X) and the load at e+1. Leadership away and back has the same effect.
3. Load(e+1) reads the committed PrepareCommit and creates the new object O_{e+1}. It sets pendingState
   to CompleteCommit and calls `addTxnMarkersToSend`. That puts `pending_{e+1}` (MCM:282) and enqueues
   markers with epoch e+1.
4. T resumes and runs `put(X, pending_e)` (MCM:282). It **replaces** `pending_{e+1}` and only logs
   "Replaced" (MCM:283-285). It enqueues markers with epoch e.
5. Next, two things happen in either order:
   - (a) the e-markers' response arrives. RCH:127-133 sees the epoch mismatch and calls
     `removeMarkersForTxn(pending_e)`, which removes the map entry. The map now has nothing for X.
   - (b) the e+1-markers' responses remove all partitions from O_{e+1} and call
     `maybeWriteTxnCompletion(X)` (RCH:198). That finds either nothing or `pending_e`, whose O_e still
     has all its partitions, so nothing is written.
6. No later event calls `maybeWriteTxnCompletion` for X.

Result:
- O_{e+1} stays in PrepareCommit with `pendingState=CompleteCommit` forever, until the next
  leadership change of txn partition p.
- EndTxn, InitProducerId and AddPartitions return CONCURRENT_TRANSACTIONS forever.
- The timeout abort skips the txn, because of its pending state (TSM:134).
- The data partitions did receive the markers, so the LSO is not blocked. The producer and
  transactional.id are wedged.

Compensating mechanisms checked:
- `writeTxnCompletion` and `tryAppendToLog` re-check the epoch. They are irrelevant here because they are never reached.
- The TMQ comment (TMQ:64-79) assumes "sending new markers will fix the state". That holds only when the new put comes last.
- No periodic re-drive exists.

The window is short in practice: lines TC:960-1007, which include `responseCallback`. Making the
`put` conditional on the epoch (or `compute` keeping the higher coordinatorEpoch) would fix it.
TLA+ invariant to check: `[](state=PrepareX /\ pending=CompleteX /\ leaderStable => <>(state=CompleteX))`.
A safety proxy: `pendingMap[X].epoch = cacheEpoch(X)` whenever markers for the cache epoch are in flight.

### F2 (b): a stale earlier-epoch load installs over a newer one after an epoch bump without resignation

`onElection(e+1)` adds `(p, e+1)` to `loadingPartitions` but never removes `(p, e)`.
`removeLoadingPartitionWithEpoch` only runs on resignation (TSM:713-738, TC:497-506).

If Load(e+1) runs before Load(e), Load(e) still finds `(p, e)` (TSM:577) and:
- `addLoadedTransactionsToCache(p, e)` overwrites the e+1 cache (TSM:542),
- re-sends markers with epoch e.

Afterwards every `getTransactionState` returns NOT_COORDINATOR, because `isCoordinatorFor(p, e)` is
false (TSM:422-423). The partition is unavailable until the next leadership change. Its PrepareX txns
are only completed by luck.

Ordering requirement: Load(e) is delayed by the HWM wait (TSM:634-650), while Load(e+1) is scheduled
directly. That needs the HWM listener for e to fire after onElection(e+1) has read
`highWatermark >= LEO` (the UnifiedLog listener runs after the HWM is published; Partition.scala:206-213).
The scheduler is single-threaded, so the order is decided by enqueue order. This is narrow but
modelable. It is a loading-layer issue rather than a marker issue, but it also produces stale-epoch
markers and an F1-like "Replaced" put of `pending_e` over `pending_{e+1}`.

### F3 (b-low / c): INVALID_PRODUCER_EPOCH or TRANSACTION_COORDINATOR_FENCED at an unchanged local epoch wedges the txn

RCH:160-167 removes the pending entry and stops. Nothing clears `pendingState=CompleteX` (set at
TC:976/981 or TSM:587-591), so the txn is stuck like F1, at the same epoch, until a reload.

Can the error occur while the local cache epoch still matches?

- **TV2.** The marker epoch is the bumped epoch, and PAI requires `>` (PAI:122-160).
  - Duplicate or retried markers are accepted only when no txn is ongoing (KAFKA-19999 relaxation,
    PAI:137-143).
  - A new txn cannot start on the data partition while the coordinator is in PrepareX:
    AddPartitions/verify returns CONCURRENT_TRANSACTIONS (TC:388, 449).
  - So within a single coordinator, a duplicate marker should never meet an ongoing txn.
- **TV1.** The epoch is unchanged, so this error is unreachable without an InitProducerId, and
  InitProducerId is blocked in PrepareX.
- **COORDINATOR_FENCED.** Requires a higher coordinator epoch elsewhere. In that case this broker is
  stale and will resign, so this is compensated.

Classification: no concrete trigger found. It is a liveness assumption that model checking should
verify, including F1-style stale markers and the TV2 duplicate rule.

### F4 (a, crash): unlisted WriteTxnMarkers error codes kill the coordinator broker

RCH:178-179 throws ISE on the sender thread. InterBrokerSendThread turns this into FatalExitError,
which calls `Exit.exit`.

The concrete trigger is UNKNOWN_TOPIC_ID:
- The marker handler builds `replicaManager.topicIdPartition(tp)` from the leader's metadata cache
  (KafkaApis:1859).
- `getPartitionOrException` throws `UnknownTopicIdException` when the local Partition's id differs
  (ReplicaManager:507-516).
- `appendToLocalLog` passes that through as an error (ReplicaManager:1434-1442).
- Window: the topic is deleted and recreated with the same name, and the leader has published the
  new image to MetadataCache before ReplicaManager.applyDelta replaces the Partition.

Also any Throwable becomes UNKNOWN_SERVER_ERROR, and the `__consumer_offsets` path maps arbitrary
group-coordinator exceptions (KafkaApis:1838-1851).

Severity: availability. It is not txn-safety: the new coordinator reloads and will usually succeed.
It is worth a code-level reproduction.

### F5 (b-low, liveness depends on resignation): the second append clears pendingState on NOT_COORDINATOR or an unexpected error while the epoch is unchanged

TSM:836-847 clears the pendingState and leaves PrepareX with no pipeline:
- NOT_COORDINATOR is reachable while `isCoordinatorFor` is still true for KAFKA_STORAGE_ERROR,
  before the log-dir failure marks the partition offline.
- Unexpected errors (MCM:329-332) are also reachable when the callback runs off the sender thread.

Recovery comes only from a reload after the leadership change. That is sound if the controller moves
leadership (log-dir failure), and stuck otherwise. Model it as "the CompleteX append fails at a stable
epoch, so the txn is never completed".

### F6 (c, efficiency): busy spin while a partition has no leader

A partition with leader = -1 resolves to `Node.noNode` (KRaftMetadataCache:425-431) and goes into the
unknown queue. Every `generateRequests` drains it and re-adds it via `addTxnMarkersToBrokerQueue`,
which calls `wakeup()` (MCM:411). The next `selector.poll` returns immediately, so the sender thread
spins until a leader appears. Correctness is fine.

### False positives / compensated

- **Unknown leader (`None`) treated as a deleted topic (MCM:375-399).** It could drop the markers of
  an existing partition if the coordinator's metadata image lagged. It is compensated:
  - AddPartitions requires the topic in the coordinator's own image (KafkaApis:1965-1966).
  - Images are monotonic. A failover coordinator's image is at or past the leadership-change offset,
    which is at or past the image offset at which the old coordinator added the partition.
  - So `None` really means deleted.
  - Delete then recreate with the same name sends a marker to the new topic (name based). That is
    harmless because the new topic has no producer state, but see F4 for the crash variant.
- **Dead-queue race (TMQ:57-80).** Only old-epoch entries are lost. A reload re-sends, and a pure
  resignation leaves an orphan map entry (a metric/leak only). The orphan is overwritten by the next
  put, which is benign in that order.
- **`removeMarkersForTxn` uses CHM.remove(k, v) with value equality.** `PendingCompleteTxn` is a
  record whose `TransactionMetadata.equals` compares mutable fields (TM:617-634). A different
  coordinatorEpoch always differs. Within one epoch, `newMetadata` timestamps differ between txns.
  So no spurious removal was found.
- **Unload while markers are in flight.**
  - Completions see NOT_COORDINATOR, LOAD_IN_PROGRESS or an epoch mismatch, and drop.
  - The reload re-drives from the log, since PrepareX is committed thanks to the HWM wait (TSM:627-653).
  - Stale markers carry the old coordinatorEpoch, and data partitions fence lower coordinator epochs
    (PAI:258-268).
  - The result is the same (COMMIT stays COMMIT), so there is no flip to abort.
- **COORDINATOR_LOAD_IN_PROGRESS on the second append (MCM:325-327, MCM:244-247).** The loading
  process re-drives the txn. This is correct.
- **`writeTxnCompletion` `Right(None)` (MCM:263-267).** Expiration only removes
  `isExpirationAllowed` states (TSM:224-230), never PrepareX. Not reachable.
- **Can a durable PrepareCommit become aborted?** No path was found. Every re-drive takes the result
  from the persisted state (TSM:583-591, TC:972-981). EndTxn(ABORT) in PrepareCommit returns
  INVALID_TXN_STATE (TC:914-918). The timeout abort only targets ONGOING (TSM:138-142).
  The failure modes are liveness (stuck) and availability (crash), not a commit/abort flip.

## 5. Answers to the key questions

- **Once PrepareCommit is durable, can the txn become aborted?** No, in all paths read.
- **Can some partitions never receive markers?**
  - Only partitions whose topic is absent from the coordinator's image (deleted).
  - Partitions failing with UNSUPPORTED_* are treated as done.
  - Otherwise markers are retried forever, or re-driven by a reload.
- **Can the txn be stuck in PrepareX forever with pendingState=CompleteX?**
  - Yes via F1, at a same-broker epoch change, which is a race.
  - Yes via F3, if its precondition is reachable.
  - With pendingState cleared, via F5.
  - F2 makes the whole partition NOT_COORDINATOR.
  - All of these last until the next leadership change of the txn partition. Nothing periodic re-drives them.
