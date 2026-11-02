# Deep analysis: SharePartitionManager / DelayedShareFetch / sessions (KIP-932)

Source revision: `8aa57006f` (kafka/). All paths relative to `kafka/`.
Abbreviations: SPM = `core/src/main/java/kafka/server/share/SharePartitionManager.java`,
DSF = `core/src/main/java/kafka/server/share/DelayedShareFetch.java`,
SFU = `core/src/main/java/kafka/server/share/ShareFetchUtils.java`,
SP = `core/src/main/java/kafka/server/share/SharePartition.java`,
SSC = `server/src/main/java/org/apache/kafka/server/share/session/ShareSessionCache.java`,
SF = `server/src/main/java/org/apache/kafka/server/share/fetch/ShareFetch.java`,
IFS = `server/src/main/java/org/apache/kafka/server/share/fetch/InFlightState.java`,
KA = `core/src/main/scala/kafka/server/KafkaApis.scala`.

## 0. Structural facts (verified)

* **Only two triggers release a member's acquired records eagerly**: (a) client FINAL_EPOCH
  (KA:3502-3511 for ShareFetch, KA:3733-3742 for ShareAcknowledge) and (b) connection
  disconnect (SSC:282-296 -> SSC:137-159 -> `ShareGroupListenerImpl.onMemberLeave` SPM:933 ->
  `releaseSession` SPM:408-456). There is **no wiring from the group coordinator**: `onMemberLeave`
  /`onGroupEmpty` are only called from SSC (grep: no other callers). A member fenced by the GC
  (heartbeat expiry) on a live connection keeps its session, keeps fetching/acking, and its records
  are only released by lock timeout. `ShareSessionCache` has **no time-based eviction**
  (class javadoc mentions `tryEvict`, but it does not exist).
* KafkaApis does not validate GC membership for ShareFetch/ShareAcknowledge (`isMemberIdValid` is a
  format check only, KA:3305, KA:3699).
* INITIAL_EPOCH re-creation removes the old session *without* releasing records (SPM:537-547), by
  design: the same memberId may still ack them via the new session.
* `releaseSession` = `cachedTopicIdPartitionsInShareSession(get(key))` then `cache.remove(key)`
  (SPM:413-417) — two separate synchronized calls, keyed by (group, member), not by session object.
* `SharePartition.releaseAcquiredRecords(memberId)` walks the whole cachedState and releases every
  batch/offset in ACQUIRED owned by memberId (SP:1082-1124, 1142-1240). It uses
  `startStateTransition` (pending AVAILABLE, rollback on write failure, IFS:174-212).
* Ack ownership check: `validateAcknowledgementBatchMemberId` (SP:2282-2298) for whole batches,
  per-offset owner check in `acknowledgePerOffsetBatchRecords` (SP:2341). Ack of a batch with an
  ongoing transition is rejected (SP:2228-2232).
* Purgatory keys per partition in request: `DelayedShareFetchGroupKey(group,topicId,part)` and
  `DelayedShareFetchPartitionKey(topicId,part)` (SPM:712-718). Group key is notified on: ack
  (SPM:380-381, synchronously after the in-memory transition, *before* persist), releaseSession
  (SPM:448-449), init completion (SPM:731-734), SP removal (SPM:847), lock timeout (SP:3124,
  `maybeCompleteDelayedShareFetchRequest(!stateBatches.isEmpty())`), persist commit only if
  start offset moved (SP:2590-2594), partition-lock release with data (DSF action queue
  1114-1141). Partition key: HWM advance (ReplicaManager.scala:866) and leader loss
  (ReplicaManager.scala:304-312, Partition.scala:66-76).
* Fetch lock (`SP.fetchLock`, AtomicReference<Uuid>) serializes `acquire` per share-partition across
  DelayedShareFetch instances (SP:1547-1584). Two requests from the same member therefore never
  acquire on the same partition concurrently; the session epoch (`newContext` SPM:560-584) rejects
  two ShareFetches with the same epoch before any acquisition.
* Acquisition happens **only** in `ShareFetchUtils.processFetchResponse` -> `SP.acquire`
  (SFU:98-121), invoked from DSF `processFetchResultAndComplete` (DSF:374-403) and
  `completeRemoteFetchRequest` (DSF:1233-1264). Everything before that only holds the fetch lock;
  all those pre-acquire error paths release the fetch lock (verified each: DSF tryComplete catch
  493-506, completeShareFetchAsyncRequest 258-294, completeAsyncRequestWithEmptyResponse 799-814, minBytes
  not satisfied 776-790 (processAsyncReadResponse), remote paths 1220-1231, 1266-1310). So *fetch-lock* leaks are fixed;
  the open problems are *record* leaks after `acquire`.
* `SP.markFenced()` only flips `partitionState` (SP:1589-1596); `SP.close()` only closes metrics
  (SP:1129-1137). **Acquisition-lock timer tasks are not cancelled**, and neither the timeout handler
  (SP:2945-3017) nor `acknowledge` (SP:1010-1070) nor `releaseAcquiredRecords` check `partitionState`.
  `acquire` checks `stateNotActive()` *outside* the write lock (SP:760) and does not re-check after
  `lock.writeLock().lock()` (SP:792).
* `SharePartitionCache.remove(key)` is unconditional by key (SharePartitionCache.java remove()),
  so every `removeSharePartitionFromCache(key)` (SPM:837-851) removes *whatever instance is currently
  cached*, not the instance that produced the error.
* Coordinator fencing accepts equal epochs: `leaderEpochMap.get(k) > partitionData.leaderEpoch()`
  rejects only strictly lower (ShareCoordinatorShard.java:788, 846; stateEpoch 792, 956).
  `writeShareGroupState` sends the instance's own `leaderEpoch`/`stateEpoch` (SP:2841-2849).
  Persister retries writes with backoff (PersisterStateManager.java:258-266), so a write can land
  long after it was issued.

---

## 1. Session close/fence vs acquired records; late acks

### 1a. Ordering of release vs late ack — SAFE (verified)
* FINAL_EPOCH ShareFetch: release is issued inside the `thenCombine` after both fetch and ack futures
  complete (KA:3502). FINAL_EPOCH ShareAcknowledge: release after ack future completes (KA:3731-3742).
  So piggybacked/explicit final acks are applied before release.
* A late ShareAcknowledge after close is rejected by `acknowledgeSessionUpdate` (SPM:600-606,
  SHARE_SESSION_NOT_FOUND); piggybacked acks in an INITIAL_EPOCH fetch are rejected (SPM:531-534).
* Check-then-act race (session check passes, release runs, then `SP.acknowledge`): both run under the
  SP write lock, so they serialize. Ack-first => batch ACKNOWLEDGED (pending), release skips it
  (`batchState()==ACQUIRED` check SP:1201). Release-first => batch AVAILABLE (pending,
  member=EMPTY) => ack fails on ongoing transition / EMPTY owner (SP:2287-2292). If re-acquired by
  member B => memberId mismatch => `InvalidRecordStateException` (SP:2294-2297). **A late ack from A
  cannot succeed on records now owned by B.**
* Rollback corner: ack-first then persist failure rolls back to ACQUIRED(A) (IFS:189-212). If
  session was meanwhile closed, release already skipped it; the original timer is still attached
  (not cancelled until commit, IFS:191-196), so the lock timeout releases it. Bounded, no leak.
* Same memberId reuse: a client reconnecting keeps its memberId, so after release + re-acquire by the
  *same* member, an old ack for the previous delivery succeeds against the new delivery. Within the
  contract (same consumer), noted only for modeling (memberId is not a delivery token).

### 1b. FINDING F1 — disconnect release can tear down a *newer* session of the same member and release its live records
`maybeRemoveAndNotifyListenersOnMemberLeave` (SSC:137-159): `get(key)` under lock, then *outside*
the lock calls `onMemberLeave(group, member)` -> `releaseSession(group, member)` which re-reads the
session **by key** (SPM:413) and `cache.remove(key)` (SPM:417). Interleaving:
1. Processor thread: `onDisconnect(conn1)` removes conn1 mapping, not stale (SSC:283-290); `get(key)`
   returns old session S1.
2. Handler thread: client already reconnected on conn2, INITIAL_EPOCH `newContext` removes S1 (marks
   conn1 stale — too late, mapping already taken) and creates S2 with the same key (SPM:537-547).
   S2's first fetch acquires records R for member M.
3. Processor thread: `releaseSession` reads S2's partitions, removes **S2**, and
   `releaseAcquiredRecords(M)` releases R although the live consumer holds them. R can be acquired
   by member B while M is processing => concurrent delivery to two consumers; M's ack then fails;
   M's next fetch gets SHARE_SESSION_NOT_FOUND.
Compensation: `stale` marking (SSC:179-182) covers the common late-disconnect case, but not this
window (mapping already removed in step 1). Also, the release is not tied to the session object.
Classification: **Model-checkable** (session-object vs key identity; small model with 2 connections),
Test-verifiable with a latch in a ShareGroupListener. Confidence: medium (real race, narrow window;
consequence is at-least-once redelivery overlap rather than loss).

### 1c. FINDING F2 — records acquired after the member's release are never eagerly released
`releaseSession` releases only what is ACQUIRED at that instant. A DelayedShareFetch already in
purgatory for M (created before the close) keeps running: nothing checks session liveness at acquire
time (SPM:681-756, SFU:98). It acquires records for M after the release; the response goes to a
closed connection (or to a client that is closing and whose later ack is rejected with
SHARE_SESSION_NOT_FOUND). Records stay ACQUIRED until the lock timeout and the delivery count is
consumed without delivery. Same for FINAL_EPOCH ShareAcknowledge racing an in-flight ShareFetch
(KA:3733). Bounded by `share.record.lock.duration.ms`; contributes to F4's "archived without
delivery". Classification: **Model-checkable** (liveness/delivery-count accounting), Confidence: high
that it happens, low severity alone.

### 1d. Design note D1 — GC-fenced zombie member
No GC->SPM listener; a member removed by the GC but with a live TCP connection keeps acquiring
(KA does not consult membership). Records only free via lock timeout. Code-review-only; likely by
design (share assignment is advisory) — worth confirming against KIP-932.

---

## 2. Concurrent fetches, DelayedShareFetch, records acquired but not returned

Pre-acquire paths are clean (see §0). Post-acquire, **no path releases acquired records when the
response does not carry them**; the only compensation is the lock timeout. Each instance below burns
one delivery attempt (deliveryCount++ in `acquire`, SP:944) without delivery. Combined with
`maxDeliveryCount` (default 5) and the timeout path archiving at the limit
(IFS:131-133, SP:3025-3030) a record can be **ARCHIVED (or sent to DLQ) without ever reaching a
consumer**.

### F3 (strongest) — throttled ShareFetch response drops acquired records
KA:4317-4335: after `fetchMessages` has acquired records, `processShareFetchResponse` computes quota
throttle; if `maxThrottleTimeMs > 0` the response is replaced by `shareFetchContext.throttleResponse`
(empty partitions, ShareSessionContext.java:104-118). Acquired records are silently dropped.
A repeatedly throttled consumer (request-rate quota) can push the same records through
maxDeliveryCount acquisitions => archived undelivered. No test covers throttling for share fetch
(grep KafkaApisTest: none). Classification: **Test-verifiable** (KafkaApisTest with quota mock
returning >0, then inspect SP state) and **Model-checkable** as an abstract "ResponseDropped" action.
Confidence: high on mechanism; impact depends on quotas being configured.

### F4 — session-epoch mismatch at response time drops acquired records
`ShareSessionContext.updateAndGenerateResponseData` (ShareSessionContext.java:187-201) returns
INVALID_SHARE_SESSION_EPOCH with **no partitions** if the session epoch moved since `newContext`
(e.g. a pipelined ShareFetch/ShareAcknowledge with the next epoch from the same member — the
"two fetches in flight" case). The first response's acquired records are dropped. Java client likely
does not pipeline per node, but the broker must not rely on it. Classification: **Test-verifiable**,
**Model-checkable** (two in-flight requests per member). Confidence: medium-high.

### F5 — exception in `processFetchResponse` after partial acquisition
SFU:64-125 loops over partitions calling `SP.acquire`. If partition B throws (e.g. FileRecords batch
iteration failing after the segment was deleted by retention, `IllegalStateException` from
`updateAcquisitionLockTimeoutTask`/`fetchSubMap`), the caller catches it
(DSF `processFetchResultAndComplete` catch -> `handleFetchException`, DSF:392-396; remote
`completeRemoteFetchRequest` catch DSF:1247-1250) and completes with `maybeCompleteWithException(all
partitions)` (SF:195-205): partition A's already-acquired records are replaced by an error entry and
never released. `exceptionHandler` only removes the SP for the four fencing exception types
(SPM:813-824), so A's SP survives with orphaned ACQUIRED records. Same if `acquire` itself throws
midway through its own loop (records of earlier sub-batches already moved to ACQUIRED under the lock,
SP:792-997). Classification: **Test-verifiable** (DelayedShareFetchTest with sp1.acquire throwing),
Confidence: high on mechanism, trigger is exceptional.

### F6 — erroneous-map overwrite of an acquired partition
`ShareFetch.maybeComplete` copies the data then `addErroneousToResponse` **overwrites** any partition
also present in `erroneous` (SF:176-186, 224-243). A partition can be in both: in
`isMinBytesSatisfied` an exception from `endOffsetMetadata` adds the partition to `erroneous`
(DSF:603-611) but leaves it in `topicPartitionData`/`localPartitionsAlreadyFetched`; if another
partition satisfies minBytes, `forceComplete` runs `processFetchResponse` over the already-read data
including the erroneous partition (no `filterErroneousTopicPartitions` on that path, only in
`readFromLog`), so `acquire` runs (SP still ACTIVE for non-fencing exceptions) and its result is then
overwritten by the error. Also `handleInitializationException` (SPM:788-805) can add a partition
asynchronously. `DelayedShareFetchTest.testExceptionInMinBytesCalculation` only covers the single
partition case. Classification: **Test-verifiable**, Confidence: medium (needs non-fencing exception).

### F7 — acquire on a just-fenced instance (TOCTOU)
`SP.acquire` checks `stateNotActive()` at SP:760 without the lock; `markFenced` can run before
SP:792. The fenced (removed) instance then acquires and returns records, scheduling timers on the
stale instance. Same window exists for a DelayedShareFetch holding the old instance in its
`sharePartitions` map. Consequence: a delivery from an instance the broker already removed; the next
instance does not know about it (acquisitions are not persisted) and can redeliver concurrently.
Classification: **Model-checkable** (atomicity of check+acquire), Confidence: high on race,
low severity on cross-broker leader change (already at-least-once), higher combined with F9.

Error paths verified **clean** (release fetch lock, nothing acquired yet): readAsync sync/async
failure (DSF:345-372, 458-474 + 258-284), remote scheduling failure (remoteStorageFetchException
path DSF:493-506 + `completeErroneousRemoteShareFetchRequest` (DSF:1090-1097)), remote completion exceptions
(handleFetchExceptionAndReleaseLocks), local read failure inside remote completion
(DSF readAndProcessFetchResultCompletion), partition rotation (only reorders session partitions
after the response is built, ShareSessionContext.updatePartitionOrder). The double-release of
non-remote partitions is fixed (DSF `nonRemoteFetchTopicPartitions.forEach(partitionsAcquired::remove)`);
note `releaseFetchLock` force-clears a lock it does not own (SP:1569-1577) so any future double
release would steal another request's lock — keep as a model invariant (FetchLockOwnerOnlyReleases).

---

## 3. Piggybacked acks vs fetch; acks on removed/recreated instances

* Ordering inside one ShareFetch: `handleAcknowledgements` runs first (KA:3437-3448) and its
  in-memory part (`SP.acknowledge` under the write lock) completes synchronously in
  `SPM.acknowledge` before `fetchMessages` is called (KA:3459). Persist is async. RELEASEd records
  sit in AVAILABLE-with-ongoing-transition and `acquire` skips them (SP:902-906, 920-924), so the
  same request never redelivers what it just released; they become acquirable after commit.
  ACCEPTed records are ACKNOWLEDGED-pending. Response waits for both futures (`thenCombine`,
  KA:3466-3467); ack errors are reported per partition, fetch proceeds regardless.
* Ack completion futures: `SPM.acknowledge` maps throwable -> per-partition error
  (SPM:360-376, 473-497); never fails the whole request.
* Acks route by `partitionCache.get(key)` at arrival (SPM:357). After removal+recreation they hit
  the **new** instance. New instance state comes from the persister: never-acked in-flight offsets
  are AVAILABLE with member EMPTY => rejected (`InvalidRecordState`); if re-acquired by another
  member => rejected; if re-acquired by the same memberId => accepted (see 1a). If no instance =>
  UNKNOWN_TOPIC_OR_PARTITION (SPM:384-386). No state mix-up on the new instance from routing.
* **FINDING F8 — ABA removal by error callbacks.** The ack whenComplete
  (SPM:360-365), releaseSession (SPM:438-443), DSF exceptionHandler (DSF `handleFetchException`)
  and `handleInitializationException` (SPM:801) all call `removeSharePartitionFromCache(key)`, which
  removes *the current* instance for the key. A late failure from the old instance A (e.g. its write
  rejected with FENCED_LEADER_EPOCH -> `NotLeaderOrFollowerException`, SP:2900-2901, after persister
  retries) arriving after B was created and initialized removes and fences **B**. B's ACQUIRED
  records lose their owner path (acks now go to C or fail), B's timers keep running and writing
  (F9). Classification: **Model-checkable**, Confidence: medium-high on mechanism (code is
  unambiguous); impact: spurious fencing + stale-writer exposure.

---

## 4. SharePartition lifecycle; stale instances that keep mutating/persisting

Creation: `getOrCreateSharePartition` (SPM:758-786) reads `leaderEpoch` then registers a listener,
then constructs the SP; init is lazy via `maybeInitialize` (SPM:720; SP:420-598). Init failure
(non-LeaderNotAvailable) removes the SP (SPM:800-805). Removal: listener onBecomingFollower /
onFailed / onDeleted (SPM:895-923), fencing error types (SPM:813-824), group empty on this broker
(SPM:938-950), share version disabled (SPM:626-637).

### F9 — removed instances keep persisting with the same epochs (stale writer)
After `removeSharePartitionFromCache` the old instance is FENCED but:
* its acquisition-lock timers still fire and `writeShareGroupState` (SP:2991-3009) — no state check;
* an `acknowledge`/`releaseAcquiredRecords` that obtained the reference before removal proceeds and
  persists (no state check, SP:1010-1070, 1082-1124);
* writes already in persister retry are delivered later.
They carry the instance's `leaderEpoch`/`stateEpoch`. When the replacement instance is created on
the **same broker with the same leader epoch** — `onGroupEmpty` (all sessions disconnected, then a
member reconnects), `onShareVersionToggle`, ABA removal (F8) — the coordinator accepts the stale
writes (equality accepted, ShareCoordinatorShard.java:788/846). Concrete bad trace:
1. Instance A: member M holds offsets 10-19 ACQUIRED; M disconnects => `releaseSession` puts 10-19
   AVAILABLE(pending) and issues write W1(AVAILABLE, dc=1); coordinator unavailable -> W1 retries.
   Group-empty => A removed (FENCED).
2. Member N connects; instance B reads state (W1 not yet applied), acquires 10-19, N ACCEPTs,
   B writes W2(ACKNOWLEDGED) — accepted.
3. W1 finally lands (same leaderEpoch/stateEpoch) => durable state for 10-19 regresses to AVAILABLE.
4. Next re-initialization (leader change / restart / another group-empty) redelivers 10-19 — an
   acknowledged record is redelivered (guidance Goal #2).
The same applies with a stale timer firing on A after step 2 (timer write AVAILABLE/ARCHIVED over
B's ACKNOWLEDGED), which does not even need persister retries if B acks within the lock duration.
Cross-broker leader change is protected only after the new leader's `readState` bumps the leader
epoch (guidance PQ2) — before that, stale writes are accepted, which is benign only because they
describe the old leader's in-flight records.
Classification: **Model-checkable** (primary target: model SP instances as versions with
(leaderEpoch, stateEpoch) and timers that survive removal; invariant
`AckedNeverRegresses` on coordinator state). Confidence: medium-high that the stale write is
accepted; medium that the full trace is reachable (needs W1 delayed past B's init, or lock timer
alive past B's ack — the timer variant is easy: records acquired after release per F2/F7 keep their
timers on A).

### F10 — listener registration not atomic with leader-epoch read
SPM:761-767: `leaderEpoch()` then `addPartitionListener` (Partition.scala:271-283 only checks
`log` presence). If the partition becomes follower in between, `onBecomingFollower` is missed and the
ACTIVE instance stays cached on a follower. Log-read NOT_LEADER errors do not trigger removal
(SFU:73-99 only handles OFFSET_OUT_OF_RANGE; `anyPartitionHasLogReadError` short-circuits before
`isMinBytesSatisfied`), so it can survive until the broker is leader again, then serve acquisitions
from its pre-interim in-memory state (not reflecting acks made on the interim leader) until a write
is fenced. Classification: **Code-review-only / Test-verifiable** (needs a precise interleave),
Confidence: low-medium.

DelayedShareFetch keeps its captured `sharePartitions` map (DSF field); after removal, the fenced
instance refuses `maybeAcquireFetchLock` (SP:1548) so no new acquire except via F7's window; the
request completes on timeout. Sessions hold only TopicIdPartitions, not SP references.

---

## 5. Purgatory liveness

* No starvation: every DelayedShareFetch has `maxWaitMs`; on expiry `onComplete` re-tries
  `acquirablePartitions` and acquires (DSF:296-325). So records AVAILABLE with consumers waiting are
  delivered at worst after maxWaitMs per round. Not "never woken".
* Latency/spurious-completion gaps (Model-checkable as weak-fairness liveness if the model omits
  timeouts):
  - Ack/release notify the group key synchronously, **before** persist commit (SPM:380-381,
    448-449). The woken fetch sees the records with an ongoing transition and cannot acquire them; it
    may complete with an empty response (minBytes satisfied by log bytes) — consumer gets empty
    poll. After commit, notification is sent only if the start offset moved
    (`cacheStateUpdated`, SP:2588-2594); a RELEASE in the middle of the cache does not wake waiters.
    Waiters get them on timeout.
  - Lock-timeout releases use `tryUpdateBatchState` (immediate) and notify (SP:3016) — fine.
  - Leader-change keys: completion uses partition key; stale operations remain watched until purge
    (documented, DSF:484-492).
  Classification: **Model-checkable** (liveness under fairness without fetch timeout), low severity.

---

## Summary table

| ID | Issue | Contract impact | Class | Conf. |
|---|---|---|---|---|
| F1 | disconnect release removes newer same-member session & releases live records | concurrent double delivery | Model-checkable / Test | med |
| F2 | records acquired after release (in-flight fetch) not eagerly released | delivery count burnt, stuck until timeout | Model-checkable | high (low sev) |
| F3 | throttled ShareFetch drops acquired records | undelivered, may archive w/o delivery | Test-verifiable / Model | high |
| F4 | epoch mismatch at response drops acquired records | same as F3 | Test-verifiable / Model | med-high |
| F5 | exception after partial acquire in processFetchResponse | same as F3 | Test-verifiable | high (rare trigger) |
| F6 | erroneous map overwrites acquired partition | same as F3 | Test-verifiable | med |
| F7 | acquire TOCTOU with markFenced | delivery from removed instance | Model-checkable | high (low sev alone) |
| F8 | ABA removal by stale error callbacks | fences healthy instance | Model-checkable | med-high |
| F9 | fenced instances keep writing with same epochs | acked record regresses -> redelivery | Model-checkable | med-high / med |
| F10 | listener registration race | stale ACTIVE SP on follower | Code-review | low-med |
| D1 | GC-fenced member not released | design | Code-review | - |

Suggested first checks: F9 (+F8, F2/F7 as enablers) in TLA+; F3 and F5 as unit tests
(`KafkaApisTest` with quota mock; `DelayedShareFetchTest` with two partitions, second `acquire`
throwing; assert first partition's records are not left ACQUIRED).

---

## TLA+ sketch (manager/session/fetch layer)

Constants: `Members`, `Offsets` (2-3), `MaxDelivery` (2-3), `Brokers` optional (1 suffices for
F8/F9; 2 for leader change), `Conns`.

State:
* `sess[m]` ∈ {None} ∪ [conn: Conns, epoch: Nat, id: Nat] — session object identity (`id`) is
  essential for F1.
* `connMap[c]` ∈ [m, stale: BOOLEAN] — SSC connection map.
* `spInst` : current instance id in cache per broker (or None); `inst[i]` =
  [state ∈ {Init, Active, Fenced}, le, se, rec: Offsets -> [st, owner, dc, timer: BOOLEAN,
  pending: BOOLEAN]] — keep records of *all* instances, current or not (stale instances still act).
* `inflight` : set of fetch ops [m, inst, phase ∈ {Waiting, LockHeld, Read, Acquired, Responded,
  Dropped}, acquired: SUBSET Offsets, sessId].
* `net` : persister messages [inst, le, se, batches] (unordered bag, may be delayed — models
  retries).
* `coord[o]` = [st, dc] plus `coordLe`, `coordSe`.
* `client[m]` = set of offsets m believes it holds (delivered), for delivery-contract invariants.

Actions (each atomic step = one lock-protected critical section in the code):
* `NewSession(m,c)` (INITIAL_EPOCH: remove old by key, mark old conn stale, create new id).
* `Disconnect(c)` split in two steps: `DiscTake(c)` (remove mapping, read sess) and
  `DiscRelease(c)` (releaseSession by key: remove current `sess[m]`, release m's records in current
  instance) — the split exposes F1.
* `FetchStart(m)` (epoch check+bump; creates op capturing current instance), `TryLock(op)`,
  `Acquire(op)` (split `CheckActive` / `DoAcquire` for F7), `Respond(op)` with nondeterministic
  outcome {Delivered, Dropped} (Dropped abstracts F3/F4/F5/F6 and closed connections),
  `Expire(op)`.
* `Ack(m, o, type)` (route to *current* instance; owner check; pending transition; send write),
  `FinalEpoch(m)`.
* `LockTimeout(i, o)` for **any** instance i (current or fenced) with timer set — key for F9.
* `RemoveSP(cause)` (group empty, fencing error, follower) and `ErrorCallback(i)` that removes
  `spInst` by key regardless of i (F8).
* `CreateSP` + `InitRead` (reads coord; bumps `coordLe` if higher).
* `CoordApply(msg)` (accept iff msg.le ≥ coordLe ∧ msg.se ≥ coordSe; merge batches),
  `WriteResp(msg, ok)` (commit/rollback pending).

Invariants:
* `NoConcurrentDelivery`: ∀o: |{m : o ∈ client[m] ∧ m's lock still valid}| ≤ 1 (or: no two
  members hold o with unexpired lock on any instance considered authoritative).
* `AckedNeverRegresses`: once coord[o].st = ACKNOWLEDGED it stays ACKNOWLEDGED/ARCHIVED (F9).
* `ArchivedOnlyAfterDelivery`: coord/inst ARCHIVED due to dc ⇒ ≥1 Delivered response for o
  (exposes F2/F3-class leaks; may be relaxed to "or explicit REJECT").
* `DeliveryCountMonotone`, `OneOwnerPerAcquired`.
* `FetchLockOwnerOnlyReleases` (regression guard for DSF double release).
Liveness (with WF on Expire/LockTimeout): AVAILABLE records with a waiting fetcher are eventually
delivered.

Abstract away: bytes/minBytes/PartitionMaxBytesStrategy (nondeterministic "satisfied"), remote
storage (fold into Read with possible failure), partition rotation, metrics, batch vs per-offset
representation (use per-offset records), transaction aborts, DLQ (treat ARCHIVING≈ARCHIVED), the
coordinator's snapshot/update compaction (separate model), throttle details (one nondeterministic
Dropped outcome), action-queue indirection (notify = enable TryLock).
