# Deep analysis: KRaft dynamic quorum voter changes (KIP-853)

Source: `kafka/` trunk @ `8aa57006ff`. Paths below are relative to
`kafka/raft/src/main/java/org/apache/kafka/raft/`. Every claim was re-read in code;
compensating mechanisms were searched for before listing something as a finding.

Probe tests: `.specula-output/analysis/probes/SpeculaVoterChangeProbeTest.java` (copy into
`kafka/raft/src/test/java/org/apache/kafka/raft/` to run; see "Probe results" at the end).

---

## 0. Mechanics that everything else depends on

* **Voter-change records go to the accumulator, not the log.** `LeaderState.appendVotersRecord`
  (LeaderState.java:389) -> `BatchAccumulator.appendVotersRecord` -> `appendControlMessages`, which
  calls `forceDrain()` (BatchAccumulator.java:237). The record reaches the log only on the next
  `pollLeader -> maybeAppendBatches -> appendBatch -> appendAsLeader` (KafkaRaftClient.java:3202,
  3063, 1867), which calls `partitionState.updateState()` (1870).
* **`partitionState` (KRaftControlRecordStateMachine) reads the log with `Isolation.UNCOMMITTED`**
  (KRaftControlRecordStateMachine.java:158-162), so `lastVoterSet()` / `lastVoterSetEntry()` /
  `lastKraftVersion()` are the *latest, possibly uncommitted* values. `lastVoterSetEntry()` is empty
  when only the static `controller.quorum.voters` set is known (VoterSetHistory.java:46-54).
* **`poll()` order** (KafkaRaftClient.java:3687-3711): `pollCurrentState` (for a leader: resign
  check, expire pending ops, drain accumulator to log, flush, recompute HW) runs *before* at most
  one inbound message is handled. So an appended voter record is always in the log (and visible
  in `partitionState`) before the next request/response is processed — unless the leader resigns
  first (`pollLeader` checks resign at 3187, before `maybeAppendBatches` at 3202).
* **Leader HW/voter-state switch** happens in `updateLocalState(log.endOffset(), partitionState.lastVoterSet())`
  (KafkaRaftClient.java:361, LeaderState.java:753-769) which calls `updateVoterAndObserverStates`
  (LeaderState.java:881-909). That runs from `flushLeaderLog`, i.e. right after the record is
  appended. => **configuration takes effect on append, not on commit** (standard Raft).

---

## Q1. Is "at most one uncommitted voter-set change" enforced?

### Every leader path that appends KRaftVersionRecord / VotersRecord

| # | Path | Where the record is created | Guards |
|---|------|------------------------------|--------|
| 1 | AddVoter | `AddVoterHandler.handleApiVersionsResponse` (AddVoterHandler.java:371-392), after an API_VERSIONS round trip | at request time: `isOperationPending` (134), HW present (149-157), `kraft.version>=1` (160-172), `lastVoterSetEntry` present **and** `offset < HW` (175-187), id not in set (191-202). At response time: only kraft.version range (316) and `isReplicaCaughtUp` (350); voter set re-read from `partitionState.lastVoterSet()` (371) |
| 2 | RemoveVoter | `RemoveVoterHandler.handleRemoveVoterRequest` (RemoveVoterHandler.java:177-196), synchronous | `isOperationPending` (119-132), HW present (135-143), kraft v1 (146-158), `lastVoterSetEntry` present and `offset < HW` (161-173), `removeVoter` exact key + size>1 (177, VoterSet.java:221-234) |
| 3a | UpdateVoter, kraft.version>=1 | `UpdateVoterHandler.storeUpdatedVoters` (UpdateVoterHandler.java:466) | request: `isOperationPending` (146), HW present (163). Response (`completeUpdateVoter`): kraft range (331), `lastVoterSetEntry` present and `offset < HW` (367-372), `updateVoter` key match (447) |
| 3b | UpdateVoter, kraft.version 0 | no record; in-memory CAS `compareAndSetVolatileVoters` (UpdateVoterHandler.java:481) | same request guards; CAS against the upgrade state |
| 4 | kraft.version upgrade 0->1 | `LeaderState.maybeAppendUpgradedKRaftVersion` (LeaderState.java:456-589): KRaftVersionRecord + (if different) VotersRecord | **no** `isOperationPending`, **no** HW check. Guard = `kraftVersionUpgradeState` CAS (543) + ids equal to persisted set (505) + all voters have dir ids (524-536). Called from a non-raft thread (KafkaRaftClient.java:3880-3897) |
| 5 | Leader start-of-epoch | `LeaderState.appendStartOfEpochControlRecords` (LeaderState.java:320-387): LeaderChange, and if kraft>=1 and (`offset == -1` or leader endpoints changed) KRaftVersionRecord + VotersRecord | **none**; the written set is `voterSetAtEpochStart.updateVoter(localVoterNode)` – same ReplicaKeys as the latest (possibly uncommitted) set, only the leader's endpoints/version range change |
| 6 | Auto-join | follower/observer only *sends* AddVoter / RemoveVoter RPCs (KafkaRaftClient.java:3373-3410, ackWhenCommitted=false at 3465-3471) | inherits guards of #1/#2 at the leader |

### Are the checks equivalent?
* Add and Remove do the "last voter set committed" check at **request** time; Update does it at
  **API_VERSIONS response** time (UpdateVoterHandler.java:367). Upgrade and start-of-epoch do not do
  it at all. The difference is benign:
  * Between Add's request-time check and its append, no other voter record can enter the log:
    Add/Remove/Update are blocked by the pending handler state (`validateMutualExclusivity`,
    ChangeVoterHandlerState.java:183-204 throws if two are set), upgrade is a no-op once
    kraft.version>=1 (LeaderState.java:481-482), and start-of-epoch only runs in a new LeaderState.
  * Upgrade only applies in kraft.version 0 where Add/Remove are rejected (UNSUPPORTED_VERSION)
    and Update does not append. After the upgrade record is in the log, `lastVoterSetEntry` is the
    upgrade's uncommitted VotersRecord, so Add/Remove/Update(v1) are refused until it commits. In
    the window where the upgrade is still in the accumulator, `lastKraftVersion()` is still 0 and
    `volatileVoters()` is empty, so Update fails with REQUEST_TIMED_OUT (comment at
    UpdateVoterHandler.java:378-391).
* **Timeout of a pending op while its record is still in the accumulator** — would the guard be
  bypassed? No: an expired op is cleared in `pollLeader` (3192-3199) immediately followed by
  `maybeAppendBatches` (3202), so by the time the next request is handled the record is in the
  log and `lastVoterSetEntry().offset() >= HW` blocks the next change. (Expiry inside
  `isOperationPending` from a request handler can only happen after a full `pollCurrentState`.)
  The guard relies on this poll ordering, which a TLA+ model must preserve (append-to-accumulator
  and append-to-log are distinct steps, but no request handling interleaves between them).

### Leader elected with an uncommitted voter change from a previous epoch
Kafka does require the leader to commit an entry of its own epoch first:
`maybeUpdateHighWatermark` only sets a HW if `highWatermarkUpdateOffset > epochStartOffset`
(LeaderState.java:680), the HW starts empty for every new leader (QuorumState.java:699-708 comment),
and Add/Remove/Update refuse while `highWatermark().isEmpty()`. Once the HW exists it is
> `epochStartOffset` > offset of any previous-epoch voter record, so the `offset < HW` guard
passes only after that inherited change is committed. Together with the up-to-date-log vote rule
this closes Ongaro's single-server-change bug. The HW during this window is computed over the
latest (inherited, uncommitted) voter set — standard Raft.

### Can Update and Add overlap?
No. Mutual exclusion via `isOperationPending` (each handler) plus the `IllegalStateException` in
`validateMutualExclusivity`. API_VERSIONS responses are dispatched to whichever single handler is
pending (KafkaRaftClient.java:2331-2356). Update(v1) and Add both keep their state until
HW > lastOffset (AddVoterHandler.java:415-432, UpdateVoterHandler.java:545-566).

**Q1 verdict: enforced** (no finding). Defense-in-depth gap: `VoterSetHistory.addAt` only *logs*
a non-overlapping-majority transition (VoterSetHistory.java:24-37) instead of failing.

---

## Q2. Which voter set is used for HW, and self-removal

* HW is computed over `voterStates` (LeaderState.java:660-719, `indexOfHw = voterStates.size()/2`
  over replicas sorted by descending end offset). `voterStates` is rebuilt from
  `partitionState.lastVoterSet()` on every `updateLocalState` (881-909) => switches **on append**.
* Leader counts itself only if it is in the set. After a self-removal is appended, the leader's
  ReplicaState is moved to `observerStates` (904-907) and is excluded from the HW; `clearInactiveObservers`
  never evicts the local key (868-874). Leader's end offset is its LEO taken *before* flush
  (KafkaRaftClient.java:679-683) — pre-existing Kafka design, not KIP-853 specific.
* Commit with new set: yes, the leader keeps appending/committing with the new set until it
  resigns. It resigns only from `RemoveVoterHandler.highWatermarkUpdated` (RemoveVoterHandler.java:212-243)
  -> `requestResign()` -> next `pollLeader` (KafkaRaftClient.java:3187).
  **Finding F2** below: if the RemoveVoter op is gone (timed out) when the record commits, the
  leader never resigns.
* HW never moves backward: smaller computed HW is ignored with a log line (LeaderState.java:695-701).
* Stale replica state: identity is by id + `matchesKey` (982-991). A ReplicaState whose key has
  no directory id (kraft.version 0 static voters) matches *any* directory id, so in kraft v0 two
  physical replicas with the same id feed one state (only a `warn` on non-monotonic offset,
  796-801). After the upgrade VotersRecord, `setReplicaKey` (950-972) adopts the record's dir id
  but keeps the end offset accumulated from whichever replica fetched. With dir ids present
  (kraft v1) a replica with a new disk/dir id is tracked as a separate observer and does not count.
  New voters inherit their observer ReplicaState (real fetch offsets of this epoch); removed voters'
  states move to observers. No path found where a stale-high offset is counted.
* **Secondary finding (F2b):** `timeUntilCheckQuorumExpires` returns `Long.MAX_VALUE` when
  `voterStates.size() == 1` (LeaderState.java:225-227), assuming that single voter is the leader.
  After the leader removes itself from `{L, F}`, `voterStates == {F}` and check-quorum is disabled
  even if F is dead.

---

## Q3. Replica identity (id, directoryId) comparison per path

| Location | Comparison |
|----------|-----------|
| `VoterNode.isVoter` / `VoterSet.isVoter` (VoterSet.java:391-401, 124-128) | id equal; dir id equal **if the stored voter has one**, otherwise wildcard |
| `ReplicaState.matchesKey` (LeaderState.java:982-991), used by `getOrCreateReplicaState`, `getReplicaState`, `isVoter`, `updateFetchedVoters` | same wildcard rule |
| `VoterSet.addVoter` (VoterSet.java:199-208) and AddVoterHandler (191) | **id only** -> DUPLICATE_VOTER even if dir id differs |
| `VoterSet.removeVoter` (VoterSet.java:221-234) | **exact `ReplicaKey.equals`** (no wildcard) + size>1 |
| `VoterSet.updateVoter` (242-252) | `isVoter` (wildcard) |
| `VoterSet.updateVoterIgnoringDirectoryId` (263-272), used by Update in kraft v0 and LeaderState ctor | **id only**, overwrites dir id |
| `voterNodeNeedsUpdate` (103-110) | `isVoter` + node inequality |
| `LeaderState.isReplicaCaughtUp` (415-426) | `observerStates.get(replicaKey)` exact key |
| `LeaderState.addAcknowledgementFrom/ensureValidVoter` (829-840) | **id only**, throws if id not a voter (see F1) |
| `updateFetchedVoters`/`updateReplicaState` self-check (270-273, 788-792) | **id only**: a remote key with the leader's id throws |
| `RemoveVoterHandler` resign check (232) | `isVoter(localKey)` |
| KafkaRaftClient request validation (2281, 2433, 2521) | Add/Remove/Update all require a non-empty dir id |

Inconsistencies:
* Remove is strict while Update/isVoter are wildcard: a voter stored without dir id could be
  updated but never removed (requests must carry a dir id). In kraft v1 every voter has a dir id
  (enforced at upgrade, LeaderState.java:524-536), so latent only.
* Add rejects by id; auto-join compensates by first sending RemoveVoter for the old key
  (KafkaRaftClient.java:3391-3401). Two live replicas sharing an id will flap (each removes the
  other's key, then adds itself) — misconfiguration, not a protocol bug.
* kraft v0 Update uses `updateVoterIgnoringDirectoryId`: last UpdateVoter for an id wins and
  its dir id is what the upgrade persists.
* `ensureValidVoter` and the self-id checks are by id only and *throw* (F1, F4).

---

## Q4. Timeouts / abort

* Pending op storage: one `ChangeVoterHandlerState` per `LeaderState`. Leader resign/epoch change
  -> `LeaderState.close()` completes all pending futures with NOT_LEADER_OR_FOLLOWER
  (LeaderState.java:1079-1088). Timeout -> `maybeExpirePendingOperation` with REQUEST_TIMED_OUT
  (ChangeVoterHandlerState.java:237-286), called from `pollLeader` and `isOperationPending`.
* **Failure reported but change commits later: yes**, by design (tests
  `testRemoveVoterTimedOut`, `testRemoveVoterFailsWhenLosingLeadership` assert the voter is gone
  from the latest set even though the RPC failed). The appended record stays in the log and is
  committed later by this or the next leader. Clients must re-check with DescribeQuorum.
  Side effect: F2 (lost self-resign).
* **Success reported but change never written/committed: possible** for AddVoter with
  `ackWhenCommitted=false` (AddVoterHandler.java:393-397) and UpdateVoter in kraft v1
  (UpdateVoterHandler.java:471-478): the future is completed when the record is only in the
  accumulator. If the leader resigns before the next drain (resign check at
  KafkaRaftClient.java:3187 precedes `maybeAppendBatches` at 3202; `close()` closes the
  accumulator) the record is dropped. It can also be truncated by a later leader. This matches
  the "ack on append" contract; auto-join (which uses ackWhenCommitted=false) retries
  periodically. Low.
* Timer used for Add/Update is the API_VERSIONS request timeout returned by `requestSender.send`
  (AddVoterHandler.java:234, UpdateVoterHandler.java:222); the request's own `TimeoutMs` field
  (set by auto-join at 3467) is ignored by `handleAddVoterRequest` (KafkaRaftClient.java:2254-2311).
  For Remove it is `requestTimeoutMs` (RemoveVoterHandler.java:194). The timer covers both the
  API_VERSIONS wait and the commit wait.
* **AddVoter catch-up check does not use offsets (F3).** `isReplicaCaughtUp`
  (LeaderState.java:415-426) returns true if the observer has *ever* been caught up during this
  leader epoch (`lastCaughtUpTimestamp > 0`) and fetched within the last hour. It does not compare
  the replica's end offset with the leader LEO, contrary to the class javadoc step 7
  ("caught up to the log end offset of the leader", AddVoterHandler.java:63-64). A replica that
  caught up once and then fell far behind can be added; for 1->2 or 2->3 changes this can stall the
  HW until it catches up (liveness only; the 1-hour window is also much longer than
  `OBSERVER_SESSION_TIMEOUT_MS` = 5 min used elsewhere, 64).

---

## Findings (ranked)

### F1 (High, crash) — BeginQuorumEpoch response from a just-removed voter throws on the raft thread
`handleBeginQuorumEpochResponse` calls `state.addAcknowledgementFrom(remoteNodeId)` for any
NONE response while leader (KafkaRaftClient.java:1217-1221). `ensureValidVoter` throws
`IllegalArgumentException("Unexpected acknowledgement from non-voter")` when the id is no longer in
`voterStates` (LeaderState.java:829-840). `voterStates` drops a voter as soon as the Remove
VotersRecord is appended (881-909). Nothing in between validates membership; `maybeHandleCommonResponse`
only handles epoch/leader mismatches. Exceptions from `poll()` go to the fatal fault handler
(KafkaRaftClientDriver.java:65-66) => the leader process dies.
Trigger: voter V is slow/unresponsive, so the leader (re)sends BeginQuorumEpoch to it
(`needToSendBeginQuorumRequests`, LeaderState.java:203-212 — every `fetchTimeout/2` for any voter
that has not fetched); operator removes V (the common reason to remove a voter); V then answers the
in-flight BeginQuorumEpoch. Pre-KIP-853 the voter set was static so this could not happen.
Existing unit test `LeaderStateTest:171` asserts the throw, but no client-level test covers the
race. Model hint: add an in-flight BeginQuorumEpoch response action and an invariant
"leader never processes an ack from a non-voter without crashing".

### F2 (Medium, liveness/membership) — self-removing leader never resigns if RemoveVoter timed out before commit
Resignation after self-removal exists only inside `RemoveVoterHandler.highWatermarkUpdated`,
which runs only if `removeVoterHandlerState` is still present (RemoveVoterHandler.java:215-218).
If the op expired (`maybeExpirePendingOperation`, ChangeVoterHandlerState.java:251-257, after
`requestTimeoutMs`) before HW passes the record — e.g. one of the remaining voters is lagging/down —
then when the record later commits nothing calls `requestResign()`. No other code path resigns a
leader that is not in the voter set (searched `requestResign`, `isVoter` uses in KafkaRaftClient/QuorumState).
Result: a non-voter stays leader indefinitely: it keeps check-quorum alive from the remaining
voters' fetches (majority computed over `voterStates` without itself, 251-268), keeps sending
BeginQuorumEpoch, and serves as leader for the epoch. Retrying RemoveVoter returns VOTER_NOT_FOUND
(already removed), so the operator cannot trigger resignation except with `resign()` / restart.
F2b: if the remaining set has one voter, `timeUntilCheckQuorumExpires` returns `Long.MAX_VALUE`
(225-227), so even the death of that last voter will not depose the removed leader.
Safety is not violated (HW uses the new set), but the "removed leader resigns once the change
commits" property from KIP-853 does not hold.

### F3 (Low/Medium, liveness, doc mismatch) — AddVoter "caught up" check ignores offsets
See Q4. LeaderState.java:415-426 vs AddVoterHandler.java:63-64, 349-368.

### F4 (Low, robustness) — UpdateVoter carrying the leader's own key throws on the raft thread
UpdateVoter for `voterKey == leader's (id, dirId)` passes `updateVoter` (key matches the leader's
voter node), then `storeUpdatedVoters` calls `updateCheckQuorumForFollowingVoter(current.voterKey())`
(UpdateVoterHandler.java:469 and 493) -> `updateFetchedVoters` throws
`IllegalArgumentException("Received a FETCH/FETCH_SNAPSHOT request from the leader itself.")`
(LeaderState.java:270-273) after the VotersRecord was already put in the accumulator. Not reachable
by a correct follower (followers send their own key to a different leader); reachable by a stale or
crafted UpdateRaftVoter request whose API_VERSIONS probe to the leader's own endpoint succeeds.
Not executed.

### F5 (Low) — success acknowledged for records still in the accumulator
See Q4 (AddVoter ackWhenCommitted=false, UpdateVoter v1).

### F6 (Info) — defense in depth
* `VoterSetHistory.addAt` only logs non-overlapping majorities (VoterSetHistory.java:24-37).
* `appendStartOfEpochControlRecords` (LeaderState.java:342-352) throws `IllegalStateException`
  if kraft.version>=1 but `offsetOfVotersAtEpochStart` is empty. This would be hit if a leader
  inherits an upgrade batch that contained only a KRaftVersionRecord (upgrade omits the
  VotersRecord when `inMemoryVoters.equals(persistedVoters)`, 575). With static voters this cannot
  happen (persisted voters have no dir ids, in-memory voters must have them), so latent.
* Only TODO-like marker: AddVoterHandler.java:79 (KAFKA-17147, wait instead of failing).

---

## Checked and safe (with compensating mechanism)

1. Two concurrent voter changes via Add/Remove/Update: `isOperationPending` + `validateMutualExclusivity` throw.
2. New change while the previous one is uncommitted after its op timed out/aborted: `lastVoterSetEntry().offset() < HW` guard (Add 176, Remove 162, Update 368).
3. Timed-out op whose record is still in the accumulator: `pollLeader` expires then drains in the same call (3192-3202) before any request is handled; `poll()` handles one message after `pollCurrentState`.
4. Inherited uncommitted change from a previous epoch: HW only exists once > `epochStartOffset` (LeaderState.java:680) and all three handlers require HW.
5. kraft.version upgrade overlapping a voter change: Add/Remove unsupported in v0; Update v0 uses CAS on the same `kraftVersionUpgradeState`; after upgrade the uncommitted VotersRecord blocks changes.
6. Start-of-epoch VotersRecord: same ReplicaKeys as the latest set; it is not a membership change, and its offset > epochStartOffset keeps the guard closed until it commits.
7. HW monotonicity across membership changes: decrease ignored (LeaderState.java:695-701).
8. Removing the last voter: `removeVoter` requires size > 1.
9. Removed (uncommitted) replica becoming leader: `LeaderState` ctor requires the local key in the latest voter set (LeaderState.java:128-136); candidates must be voters.
10. Add of an id already present with a different dir id: DUPLICATE_VOTER; auto-join removes the old key first.
11. API_VERSIONS from a wrong node: `expectingApiResponse` checks id and that the record was not yet appended (AddVoterHandlerState.java:74-76).
12. Double completion of a future (ack-on-append then reset on commit/close): `CompletableFuture.complete` is idempotent.
13. Resign-on-removal reading `lastVoterSet()` (latest) instead of committed set: while the Remove op is pending no other record can follow, so at HW > lastOffset latest == committed.

## Probe results

Gradle could not build the test tree: Maven Central returned HTTP 429, and another session's
untracked `SpeculaProbeTest.java` does not compile. So the probe was compiled with `javac` against the
existing `raft/build/classes` (newer than the unmodified sources) and run through a small reflective
runner:

* `beginQuorumEpochResponseFromRemovedVoterThrows`: **PASS**. A 3-voter leader re-sends
  BeginQuorumEpoch to a voter that has not fetched, the voter is removed, and its NONE response makes
  `client.poll()` throw `IllegalArgumentException: Unexpected acknowledgement from non-voter 7`.
  **F1 confirmed.**
* `leaderRemovesItselfCommitAfterTimeoutNeverResigns`: **PASS**. The leader removes itself, the RPC
  times out (REQUEST_TIMED_OUT), then both followers fetch to the LEO and HW passes the VotersRecord.
  After 6 more fetch/poll rounds the node is still leader of the same epoch and not a voter.
  **F2 confirmed.**
* F3, F4 and F5 were established by reading the code only; they were not executed.
