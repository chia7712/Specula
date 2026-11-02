# Deep analysis: TransactionStateManager (+ TransactionMetadata, TransactionLog, coordinator hooks)

Source: `kafka/` trunk @ 8aa57006ff. Recent relevant commits already in tree:
KAFKA-21349 (wait for HWM before load, b1093f64c0), KAFKA-21181 (NOT_COORDINATOR on stale cache +
empty TV2 commit), KAFKA-20785 (PRODUCER_FENCED for commit racing coordinator abort), KAFKA-20357
(persist lastProducerEpoch).

Abbreviations: TSM = `core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala`,
TC = `.../TransactionCoordinator.scala`, TMCM = `.../TransactionMarkerChannelManager.scala`,
TMRCH = `.../TransactionMarkerRequestCompletionHandler.scala`,
TM = `transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TransactionMetadata.java`,
TL = `.../TransactionLog.java`, TS = `.../TransactionState.java`.

---------------------------------------------------------------------------------------------------
## 0. Atomicity boundaries (for TLA+ granularity)

| Region | Lock(s) | Notes |
|---|---|---|
| `getAndMaybeAddTransactionState` TSM:414-440 | stateLock.read | checks loadingPartitions (LOAD_IN_PROGRESS), cache entry, `isCoordinatorFor` (TSM:403-405, replica leader && leaderEpoch == cacheEntry.coordinatorEpoch), putIfAbsent. Returns object ref + coordinatorEpoch; lock released on return. |
| request "prepare" step (TC handlers) | txnMetadata.lock only | `getTransactionState` and `inLock` are **two separate atomic steps**; the cache can be replaced in between. Checks + `prepareXxx` set `pendingState` atomically. |
| `appendTransactionToLog` pre-check + local append TSM:871-912 | stateLock.read held across getTransactionState, inLock epoch check (888-898) and `replicaManager.appendRecords` (900) | the local log append is atomic w.r.t. load/unload (which need the write lock). Replication/ack is asynchronous. |
| `updateCacheCallback` TSM:765-869 | runs on whichever thread completes the DelayedProduce (request handler, follower-fetch handler that advanced HWM, purgatory reaper on timeout); takes stateLock.read inside getTransactionState (released), then txnMetadata.lock separately (811 / 838) | i.e. callback = 2 atomic steps: lookup, then apply. `responseCallback` (868) runs after the txn lock is released (except TSM:879/883/891 which run under stateLock.read, 891 also under txn lock). |
| load phase 1 `loadTransactionMetadata` TSM:458-533 | none, except stateLock.read per loop iteration to recheck `(pid,epoch) ∈ loadingPartitions` | runs on the single-threaded `transaction-log-manager-` scheduler (BrokerServer.scala:423). |
| load phase 2 TSM:576-608 | stateLock.write held for: install cache entry, prepareComplete for PrepareX txns (txn locks nested), remove from loadingPartitions, **and enqueue markers (sendTxnMarkers)** | the marker enqueue may even reach `appendTransactionToLog` (TMCM:299-306 → 308-336) while holding stateLock.write (reentrant read OK). |
| unload TSM:713-726 / 698-707 | stateLock.write | removes cache entry + (some) loadingPartitions entries. Marker queues purged separately afterwards (TC:505) — not atomic with cache removal. |
| expiration TSM:246-303 | stateLock.read for the whole scan + local append; txn lock per id during check + `prepareDead` | tombstone callback TSM:264-291 again stateLock.read + txn lock. |
| timeout abort TC:1077-1113 | snapshot under stateLock.read without txn locks (TSM:129-150); then per id: getTransactionState; inLock{prepareFenceProducerEpoch}; **release**; endTransaction (new getTransactionState + inLock) | 3+ separate atomic steps per txn. |

Lock-order rule in TSM:61-69 (stateLock.read before txn lock; no txn lock under stateLock.write) is
violated harmlessly in load phase 2 (TSM:582, and TMCM:293-297 `hasPendingMarkersToWrite` via
sendTxnMarkers under write lock) and at TSM:891 (responseCallback under txn lock may call
getTransactionState, TC:1014) — no deadlock because nobody acquires stateLock while holding a txn lock
without already holding stateLock.read (reentrant).

Suggested TLA+ actions: `Prepare(id)`, `LocalAppend(id)` (pre-check+append), `Replicate/HWMAdvance`,
`AppendComplete(id, ok|err)` split into `CbLookup` + `CbApply`, `Elect(p,e)`, `HwmListenerFire(p,e)`,
`LoadRead(p,e)`, `LoadInstall(p,e)`, `Resign(p,e)`, `ExpireScan`, `TombstoneCb`, `TimeoutSnapshot`,
`TimeoutFence(id)`, `TimeoutEndTxn(id)`, `ClientEndTxn`, `ClientInit`, `ClientAddPartitions`,
`MarkerSend/Ack`, `WriteCompletion`.

---------------------------------------------------------------------------------------------------
## 1. appendTransactionToLog (TSM:747-913)

### Error mapping (TSM:779-796)
| storage error | coordinator error |
|---|---|
| UNKNOWN_TOPIC_OR_PARTITION, NOT_ENOUGH_REPLICAS, NOT_ENOUGH_REPLICAS_AFTER_APPEND, REQUEST_TIMED_OUT | COORDINATOR_NOT_AVAILABLE |
| NOT_LEADER_OR_FOLLOWER, KAFKA_STORAGE_ERROR | NOT_COORDINATOR |
| MESSAGE_TOO_LARGE, RECORD_LIST_TOO_LARGE | UNKNOWN_SERVER_ERROR |
| other | passed through |

### Pending-state handling
* Success + lookup Right(Some) + epoch equal → `completeTransitionTo` (TSM:820, clears pending, TM:444).
* Success + lookup Left(err) (LOAD_IN_PROGRESS / NOT_COORDINATOR incl. `isCoordinatorFor` false) →
  respond err; **pending NOT cleared, metadata NOT updated** (TSM:804-807). Safe: that cache entry is
  about to be replaced (load) or removed (resign) — every path that makes the lookup fail ends in a reload
  or unload.
* Success + epoch differs → NOT_COORDINATOR, the new object is untouched (TSM:812-818).
* Success + Right(None) → NOT_COORDINATOR (TSM:825-831).
* Error + same epoch → pending cleared unless `retryOnError(err)` (TSM:839-848). Only the marker
  completion path passes a retry predicate (`_ == COORDINATOR_NOT_AVAILABLE`, TMCM:336); it then
  re-enqueues (TMCM:318-323).
* Error + different epoch / None / Left → nothing touched (TSM:849-864).
* Pre-check failure (TSM:878-892): responseCallback(err) without clearing pending. Same reasoning
  as above (Left ⇒ entry will be replaced; epoch mismatch ⇒ the object in hand is a *different* one).

So pendingState of a live (current-epoch) object is always cleared or completed, except deliberately
for COORDINATOR_NOT_AVAILABLE in the marker path.

### "Applied in memory but not durable"?
`completeTransitionTo` only runs after acks=-1 success (`EnforcedRequiredAcks`), i.e. offset <= HWM,
with unclean election forced off in topic config (TC:1046). Exceptions (all by design):
* TV1 epoch fence mutates memory **before** append: TC:582-584 (`setProducerEpoch`,
  `setLastProducerEpoch(-1)`); on failure `hasFailedEpochFence(true)` (TC:696) prevents a second bump
  (TM:147). Memory epoch e+1 vs. log e; only used to fence; documented safe (TM:144-146).
* `removePartition` as markers are acked (TMRCH:150, TMCM:405-407) is memory-only progress; after a
  reload all partitions get markers again (idempotent).
* Load path `prepareComplete` (TSM:585-590) only sets pending.
→ No unsafe in-memory-ahead-of-log path found. FALSE POSITIVE for the generic concern.

### "Durable but in-memory not updated"?
Yes, two families, both believed safe:
(a) error returned although record is in the local log (REQUEST_TIMED_OUT,
    NOT_ENOUGH_REPLICAS_AFTER_APPEND): pending cleared, memory stays at old state, log may later commit
    the new state. Client gets COORDINATOR_NOT_AVAILABLE (retriable) and retries; every subsequent
    transition is recomputed from memory and appended *after* the stale record, so log order supersedes
    it. Markers are only ever sent after a successful (durable) PrepareX append, and a newer coordinator
    (ISR member) that sees only the stale PrepareCommit commits it — consistent with the client's
    in-flight commit. E.g. log `PrepareCommit(e+1)` (timed out) then timeout `PrepareAbort(e+1)`: abort
    markers only after PrepareAbort is committed ⇒ any successor has it. Worth keeping in the model
    (`AppendComplete(err)` while the record stays in the log) but expected to hold.
(b) success but lookup fails / epoch changed: client gets NOT_COORDINATOR / LOAD_IN_PROGRESS, state
    becomes visible via the (re)load, which also re-drives markers for PrepareX.

---------------------------------------------------------------------------------------------------
## 2. Load / unload

* `loadTransactionsForTxnTopicPartition` (TSM:555-654): adds `(pid,epoch)` to loadingPartitions under
  write lock (559-561); if HWM < LEO waits via `HighWatermarkWaitListener` (633-648, 660-696) then
  schedules the load on the single scheduler thread; abandon on follower/failed/deleted (691-695).
* `loadTransactions` phase 2 (576-608): if still `contains(pid,epoch)`: install entry (put, overwriting
  any existing entry **without comparing epochs**, TSM:538-548), prepareComplete for PrepareAbort /
  PrepareCommit with **the new coordinatorEpoch**, remove from loading, enqueue markers.
* onElection (TC:478-488) first purges marker queues for the partition, then starts the load; the
  old cache entry (old epoch) remains until install; meanwhile lookups return LOAD_IN_PROGRESS.
* Resignation (TC:497-506 → TSM:713-726): `removeLoadingPartitionWithEpoch` + remove cache entry, then
  purge marker queues.
* Loaded objects start with empty pendingState (TM:106) except PrepareX → pending CompleteX.
* PrepareEpochFence is never written (only a transient pending state; completeTransitionTo throws for it,
  TM:413-419; the record appended is PrepareAbort). Not reachable on load. If it were, TC:290-294 /
  610-614 would throw fatal.
* PrepareCommit partitions: serialized for every non-EMPTY state (TL:59-68) and read back (TL:127-134),
  so a loaded PrepareCommit has its full partition set. TV2 empty commit has {} ⇒ completes immediately.
  Note `TransitionData.topicPartitions` aliases the live HashSet (TM:665) for prepareAbortOrCommit /
  prepareFence; benign because bytes are serialized synchronously (TSM:756) and `removePartition`
  refuses non-Prepare states (TM:126-128).
* In-flight appends from the old epoch completing after the new load: handled by epoch compare
  (TSM:812, 839) — except the tombstone callback (C1 below).
* In-flight markers from the old epoch: TMRCH compares marker coordinatorEpoch with the cache entry
  (TMRCH:66-72, 121-127); completion append from old epoch rejected at TSM:889; retry queue entries
  likewise. OK.

### C2 — stale-epoch load installs over a newer epoch (PLAUSIBLE RACE, liveness)
`loadingPartitions` is keyed by (pid, epoch); `onElection` never removes older epochs of the same
partition (TSM:559-561), and phase 2 installs if *its own* key is present (TSM:577) with an
unconditional `put` (TSM:542). Leader-epoch bumps while staying leader do call onElection again
(BrokerMetadataPublisher.scala:301-303, TopicDelta.java:196-197) without resignation.
Sequence: Elect(p,E1) waits HWM (listener L1); Elect(p,E2) — if the metadata thread reads
`log.highWatermark >= LEO` (TSM:634) after the HWM was written but before L1 is notified
(Partition.scala:210-213), load(E2) is queued before load(E1). load(E2) installs entry@E2;
load(E1) then still finds (p,E1) in loadingPartitions and **overwrites the cache with entry@E1**.
Now `isCoordinatorFor(p,E1)` is false forever (leaderEpoch=E2) ⇒ every request for the partition gets
NOT_COORDINATOR until the next leadership change. Also markers are sent for PrepareX with epoch E1.
Fix idea: in phase 2 skip if an existing entry has a higher epoch, or have onElection drop older
loading entries for the partition.

### C3 — resignation removes only one loading entry (PLAUSIBLE, potentially SAFETY)
`removeLoadingPartitionWithEpoch` uses `find` (TSM:731-737): if loadingPartitions holds (p,E1) and
(p,E2) (see C2), resign(p,E3) removes only one. Listener `abandon` compensates only if the listener has
not fired yet; if the HWM wait already finished and the load task is queued behind a long load on the
single scheduler thread, the stale entry survives. If the broker becomes leader again (Elect(p,E4),
HWM wait pending) before the stale task runs, `loadTransactionMetadata` reads up to the *current* LEO
(TSM:459, `getLogEndOffset` = leader LEO, ReplicaManager.scala:2121-2122) **without the HWM wait for
E4**, installs entry@E1 and **enqueues markers for PrepareCommit/PrepareAbort records that may be above
the HWM** (records replicated while follower under E3). Marker send (TMCM:192-228) does not recheck
epoch before sending. If the broker then loses leadership before those records commit and they get
truncated, the successor coordinator can resolve the txn differently (e.g. Ongoing → timeout abort) while
some partitions already got COMMIT markers ⇒ atomicity violation. Data-partition leaders accept the
marker only if E1 ≥ their stored coordinator epoch for that producer, so it needs a producer not touched
by E3. Highly contrived but mechanically reachable; good model-checking target (needs two outstanding
loads per partition + resign + re-elect + scheduler backlog).

---------------------------------------------------------------------------------------------------
## 3. Transactional-id expiration (TSM:152-312)

* Qualifies: `state.isExpirationAllowed` = EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT (TS:40-80) and
  `txnLastUpdateTimestamp <= now - transactionalIdExpirationMs` (TSM:224-230), and no pending (TSM:181),
  and partition not loading (TSM:251).
* Under txn lock: append tombstone (key, null value) to an in-memory batch and `prepareDead`
  (pending DEAD, TSM:192-198). Txn lock released; append happens later in `flushRecordsBuilder`, still
  inside the outer stateLock.read (TSM:156, 293-302) ⇒ cannot interleave with load/unload install, but
  can interleave with client requests (which see pending DEAD ⇒ CONCURRENT_TRANSACTIONS via
  TC:249-251, 439-444, 569-570, 865-869).
* Cache removal only after the tombstone is acked (acks=-1) and checks epoch, pending==DEAD, producer
  epoch, error NONE (TSM:272-276). On success pendingState is **not** cleared, so a request that
  obtained the old object reference before removal still sees pending DEAD ⇒ CONCURRENT, then retries
  and creates a fresh metadata (new producerId, TC:153-167). OK.
* On load a tombstone removes the id (TSM:518-519); a later record re-creates it. OK.
* Failed tombstone that is nonetheless durable: pending cleared, id stays in memory; any later
  transition appends after the tombstone; if nothing follows and failover happens the id simply
  disappears (it was expired anyway). OK.

### C1 — tombstone callback uses whatever cache entry is current (PLAUSIBLE RACE: NPE / pending clobber)
`removeFromCacheCallback` (TSM:264-291) re-reads `transactionMetadataCache.get(partition)` and
`metadataPerTransactionalId.get(id)` with no null check and no entry-identity check:
* NPE at TSM:271 when the current entry no longer contains the id — e.g. leader-epoch bump E→E+1 on
  the same broker, HWM reaches the tombstone offset, the load(E+1) (which includes the tombstone, so
  the id is absent) installs before the DelayedProduce completion runs. The HWM listener schedules the
  load synchronously inside the HWM update (Partition.scala:210-213) while `tryCompleteDelayedRequests`
  runs later on the fetch thread, so the scheduler thread can win. The exception escapes the delayed
  produce completion; remaining ids in the batch are skipped.
* Worse, if a client recreated the id after the load (InitProducerId → new object M2 with pending
  EMPTY in flight), the stale callback takes the `else` branch (epoch mismatch) and executes
  `txnMetadata.pendingState(Optional.empty())` on **M2** (TSM:284). M2's append then completes and
  `completeTransitionTo` throws "does not have a pending state" (TM:362-367) inside updateCacheCallback
  ⇒ client gets no response (times out), memory EMPTY/epoch -1 vs log epoch 0, and the cleared pending
  also lets a concurrent transition be prepared. Contrast: `updateCacheCallback`'s error branch only
  clears when epochs match (TSM:839). Fix: only clear when `coordinatorEpoch` matches and id present.

---------------------------------------------------------------------------------------------------
## 4. Timed-out transactions (TSM:129-150, TC:1077-1113)

* Snapshot: ONGOING, no pending, not 2PC, `txnStartTimestamp + txnTimeoutMs < now`, read without txn
  locks (TSM:129-150). Snapshot records producerEpoch but TC never uses it.
* Per id (TC:1086-1099) the only rechecks are producerId equality and no pending; then
  `prepareFenceProducerEpoch` (pending PREPARE_EPOCH_FENCE, transit epoch e+1 or e if
  `hasFailedEpochFence` / e==Short.MaxValue, TM:140-152). Lock released, then
  `endTransaction(..., producerEpoch = transit epoch, ABORT, isFromClient=false, serverTV)` (TC:1101-1110).
* Epoch exhaustion: TV2 rotates producerId when exhausted (TC:836-845, TM:277-280); TV1 fence can store
  Short.MaxValue (TC:583), next InitProducerId rotates (TC:266-269).

### C4 — no state/epoch recheck in abortTimedOutTransactions (CONFIRMED robustness bug + PLAUSIBLE spurious abort)
Between the snapshot and TC:1086 the txn may have moved on:
* State PREPARE_COMMIT/PREPARE_ABORT/COMPLETE_* with no pending — e.g. the window between
  `completeTransitionTo(PrepareCommit)` (TSM:820, txn lock released) and `prepareComplete` in
  sendTxnMarkersCallback (TC:962-981, separate lock) — `prepareFenceProducerEpoch` throws
  IllegalStateException (TS:109 valid previous = {ONGOING}, TM:344-345). Exception escapes the
  scheduled task (caught/logged by KafkaScheduler:152-153) and **the remaining timed-out txns of this
  round are skipped**. CONFIRMED by code reading; effect = delayed aborts + error log.
* State ONGOING again with a new txn (TV2: client committed e→e+1 and started a new txn at e+1;
  TV1: same epoch): fence + abort of a **fresh, not-timed-out transaction**; the producer is fenced
  (TV2 epoch bumped to e+2). Window = iteration over the snapshot (can be long with many timed-out
  txns). PLAUSIBLE. Fix: recheck `state == ONGOING`, `producerEpoch == snapshot epoch`, and timeout.

### C5 — fence pending consumed by client abort, then timeout endTransaction re-aborts (PLAUSIBLE)
TV2: after TC:1097 releases the lock, client EndTxn(ABORT, e) sees pending FENCE ⇒ `isEpochFence`
(TC:804), valid (TC:827), clears pending (TC:847-851) and writes PrepareAbort(e+1), completes to
CompleteAbort(e+1) and returns epoch e+1 to the client (TC:1005). The timeout thread then calls
endTransaction(producerEpoch=e+1): no pending ⇒ not a fence; COMPLETE_ABORT accepts
`producerEpoch == txnMetadata.producerEpoch` (TC:823) ⇒ writes another empty abort bumping to e+2
(TC:895-900), or if the client already began a new txn at e+1 (ONGOING, TC:819) aborts it. Either way
the live producer holding e+1 is fenced. The fence epoch e+1 collides with the epoch the client
legitimately obtained. Window: between TC:1099 and TC:1102-1110 (getTransactionState may block on
stateLock.read behind a queued writer). PLAUSIBLE.

### C6 — client EndTxn(COMMIT) while pending FENCE throws (CONFIRMED, minor)
TV2: pending FENCE ⇒ `isEpochFence`=true, valid for client epoch e (TC:827); nextState PREPARE_COMMIT
does not clear pending (TC:847 only for abort) ⇒ `prepareAbortOrCommit` → `prepareTransitionTo` throws
"already a pending state" (TM:319-321). TV1 same at TC:569-587. Uncaught ⇒ request handler returns
UNKNOWN_SERVER_ERROR (KafkaApis handleError) ⇒ client `fatalError("Unhandled error in EndTxnResponse")`
(TransactionManager.java:1848-1849) instead of CONCURRENT_TRANSACTIONS/PRODUCER_FENCED. Same window
exists in the InitProducerId fence path (TC:191-208). Outcome for the client is fatal either way, so
low severity (wrong error code + server-side exception).

### C7 — InitProducerId retry at the epoch-exhaustion boundary returns UNKNOWN_SERVER_ERROR (CONFIRMED, deterministic, minor)
State EMPTY/COMPLETE_* at (pid, MAX-1, last=MAX-2) after a successful InitProducerId bump from MAX-2.
The producer did not get the response and retries with expected (pid, MAX-2). TC:266-267: exhausted but
expected epoch ≠ current ⇒ `prepareIncrementProducerEpoch`, which throws IllegalStateException first
thing (TM:158-159) before reaching the "retry of a successful call" branch (TM:174-181) or the
PRODUCER_FENCED branch (TM:182-186). Caught at TC:273-274 ⇒ `Errors.forException` = UNKNOWN_SERVER_ERROR.
Same for a genuinely stale epoch (should be PRODUCER_FENCED) and for TV1 after a fence to Short.MaxValue
with expected MAX-1. No test covers it (TransactionCoordinatorTest ~2265-2372 only tests after rotation).

### Other observations (false positives / by design)
* Timeout vs. client EndTxn(ABORT) completed first: timeout endTransaction sees PREPARE_ABORT with
  epoch e+1 and producerEpoch e+1 ⇒ PRODUCER_FENCED (TC:820-821) ⇒ cancelled. OK.
* Timeout vs. InitProducerId: pending FENCE ⇒ CONCURRENT_TRANSACTIONS (TC:249-251). OK.
* `generateProducerId` failure on the fence path clears pending before failing (TC:836-856) ⇒ no stuck
  pending. OK.
* PrepareX txn whose completion append fails with a non-retriable error (NOT_COORDINATOR from
  NOT_LEADER/KAFKA_STORAGE_ERROR with matching epoch, or UNKNOWN_SERVER_ERROR) clears pending and
  leaves the txn in PrepareX with no partitions; `timedOutTransactions` ignores non-ONGOING, so it stays
  until a reload (TMCM:315-326; "other" even throws fatal in the callback thread). NOT_COORDINATOR implies
  leadership loss ⇒ reload elsewhere; UNKNOWN_SERVER_ERROR for a tiny CompleteX record is improbable.
  Liveness-only, low priority.
* Expiration vs. any client request: pending DEAD fully guards (see §3). OK.

---------------------------------------------------------------------------------------------------
## Summary table

| ID | Area | Class | Effect |
|---|---|---|---|
| C1 | tombstone callback TSM:264-291 | plausible race | NPE in produce-completion; clears pending of a new-epoch object ⇒ IllegalStateException in later completeTransitionTo, hung client, memory/log divergence |
| C2 | load install TSM:542/577 + onElection | plausible race | stale-epoch entry overwrites newer ⇒ partition permanently NOT_COORDINATOR until next election |
| C3 | removeLoadingPartitionWithEpoch TSM:731-737 | plausible (contrived) | stale load runs without HWM wait, sends markers for uncommitted PrepareX ⇒ possible atomicity violation |
| C4 | abortTimedOutTransactions TC:1086-1099 | confirmed (exception) / plausible (spurious abort) | round aborted by exception; fresh txn aborted, producer fenced |
| C5 | fence window TC:1099→1102 | plausible race | double abort / abort of new txn, live producer fenced |
| C6 | EndTxn(COMMIT) vs pending FENCE | confirmed, minor | UNKNOWN_SERVER_ERROR instead of retriable/fenced |
| C7 | InitProducerId at MAX-1 | confirmed, minor, deterministic | UNKNOWN_SERVER_ERROR instead of idempotent retry / PRODUCER_FENCED |
| — | durable-not-applied / applied-not-durable | false positive | log order + acks=-1 + markers-after-durable keep it safe |
