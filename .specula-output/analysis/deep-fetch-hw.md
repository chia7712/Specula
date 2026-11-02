# Deep analysis: KRaft Fetch / High-Watermark / request-response handling

Source: `kafka/raft/src/main/java/org/apache/kafka/raft/` (trunk checkout under `/home/user/Specula/kafka`).
Files read in full for the relevant sections: `KafkaRaftClient.java` (4185 lines), `LeaderState.java`,
`FollowerState.java`, `RequestManager.java`, `QuorumState.java` (transitions + initialize),
`internals/RemoveVoterHandler.java`, `internals/ChangeVoterHandlerState.java`,
`internals/KafkaRaftLog.java` (truncate / snapshot / HW), `RaftUtil.java` (response endpoints).

All line numbers below refer to `KafkaRaftClient.java` (KRC) unless prefixed by another file.

Process note: any exception escaping `KafkaRaftClient.poll()` reaches
`KafkaRaftClientDriver.java:65-66` (`throw fatalFaultHandler.handleFault("Unexpected error in raft IO thread", t)`),
i.e. the controller process terminates. "Crash" below means exactly this.

---

## Summary of findings (ranked)

| # | Severity | Finding | Location |
|---|----------|---------|----------|
| F1 | Medium-High (availability, leader crash) | Late BeginQuorumEpoch response from a voter that was removed after the request was sent -> `addAcknowledgementFrom` throws `IllegalArgumentException` -> leader process dies | KRC:1218-1220, LeaderState:829-839, LeaderState:881-909 |
| F2 | Medium (crash on restart path) | Unattached-with-known-leader (leader not in local voter set) sends bootstrap Fetch; if the bootstrap node it hits IS that leader, the NONE response falls through `maybeHandleCommonResponse` and `followerStateOrThrow()` throws | QuorumState:204-231, KRC:3304, KRC:3535-3537, KRC:2668-2671, KRC:1744 |
| F3 | Medium (liveness/config) | A leader that removes itself does not resign if the RemoveVoter operation expired (REQUEST_TIMED_OUT) before the VotersRecord committed; afterwards nothing re-checks "leader not in committed voter set" -> non-voter leader keeps leading indefinitely | RemoveVoterHandler:214-242, ChangeVoterHandlerState:251-257, KRC:3182-3199 |
| F4 | Low-Medium (crash by misconfig / duplicate node.id) | FetchSnapshot request whose `replicaId == leader id` (any directory id) throws `IllegalArgumentException` from `updateFetchedVoters` without a try/catch (Fetch has a catch-all, FetchSnapshot does not) | KRC:2031-2038, LeaderState:270-273; compare KRC:1613/1654 |
| F5 | Low (liveness) | `fetchedVoters` is not pruned on voter-set change; ids of removed voters still count toward the new majority in check-quorum; also `voterStates.size()==1` short-circuit applies even when the leader is not that single voter | LeaderState:223-268, LeaderState:881-909 |
| F6 | Low (benign ordering inconsistency) | Vote and BeginQuorumEpoch handlers mutate local state (epoch bump / become follower) *before* checking the voter key (directory id); EndQuorumEpoch has no voter-key field at all | KRC:896-918, KRC:1138-1161 |
| F7 | Low (cosmetic) | BeginQuorumEpoch ack, Vote grant, and EndQuorumEpoch ack are keyed by node id only (no directory id); compensated by the receiver-side `isValidVoterKey` check | KRC:1220, KRC:1015, KRC:1395 |

Verified-safe items are listed in Section 7.

Dynamic confirmation: a probe test (`raft/src/test/java/org/apache/kafka/raft/SpeculaProbeTest.java`,
copy saved to `.specula-output/analysis/SpeculaProbeTest.java`) with
`testLateBeginQuorumEpochResponseFromRemovedVoter` (F1) and
`testUnattachedWithLeaderReceivesFetchResponseFromLeader` (F2) was run: both FAIL with the predicted exceptions (Section 8).

---

## 1. Q1 - Inbound request handler x check matrix

Legend: Y = checked, - = not checked, n/a = not applicable.

| Handler | cluster id | topic-partition | req epoch < local | req epoch > local | req epoch == local | leader-id consistency | voter key / directory id | voter-only / leader-only | other |
|---|---|---|---|---|---|---|---|---|---|
| Vote (835) | Y 840 | Y 844 | Y FENCED via `validateVoterOnlyRequest` 886/2774 | transition to Unattached 896 | `canGrantVote` 925 | n/a (no leader in req) | Y `isValidVoterKey` 902 (**after** epoch bump at 896) | remoteId>=0 only 2776; candidate *not* checked to be a voter | lastEpoch/offset sanity 867-884 |
| BeginQuorumEpoch (1100) | Y 1106 | Y 1110 | Y FENCED 1121 | `maybeTransition` -> Follower 1138 | `maybeTransition`, `hasConsistentLeader` throws ISE on conflict 2707 | Y (throw) | Y 1146 (**after** `maybeTransition`) | sender not checked to be a voter | endpoints from req or voter set 1131-1136 |
| EndQuorumEpoch (1256) | Y 1262 | Y 1266 | Y FENCED 1277 | `maybeTransition` 1293 | only acts if Follower and `leaderId()==requestLeaderId` 1300-1302 | Y (throw) | **no voter key in request**; preferred-candidate match uses dir id 1333-1336 | - | |
| Fetch (1487/1605) | Y 1493 | Y + topicId 1497 | Y FENCED 2802 | UNKNOWN_LEADER_EPOCH 2804 | must be leader 2806, not shutting down 2811 | n/a | directory id used only to select voter-vs-observer `ReplicaState` (LeaderState:846-854, 876-879) | leader-only | fetchOffset/lastFetchedEpoch sanity 1505-1517; `replicaId==leaderId` -> ISE caught by catch-all 1654 |
| FetchSnapshot (1926) | Y 1932 | Y 1936/1940 | Y 1961 | Y | leader-only | n/a | directory id only for check-quorum match (LeaderState:276) | leader-only | **no try/catch**: `replicaId==leaderId` -> IAE escapes (F4) |
| DescribeQuorum (1879) | **-** (no field) | Y 1884 | - | - | - | n/a | n/a | `isLeader()` 1891 | read-only |
| AddRaftVoter (2254) | Y | n/a | uses `quorum.epoch()` itself (2274) => only leader/shutdown check | | | | requires dir id 2282 | leader-only | |
| RemoveRaftVoter (2411) | Y | n/a | same as above (2431) | | | | requires dir id 2439 | leader-only | |
| UpdateRaftVoter (2494) | Y | n/a | Y 2511 (`currentLeaderEpoch`) | Y | Y | | requires dir id 2524 | leader-only | |

Response handlers (all go through `maybeHandleCommonResponse` 2640 except ApiVersions/AddVoter/RemoveVoter):

| Response | top-level error | topic-partition | common epoch handling | post-check state assertion |
|---|---|---|---|---|
| Vote 953 | Y (+ UNSUPPORTED_VERSION->Candidate 960) | Y | Y | ignores if Leader / not Nominee 1009-1028 |
| BeginQuorumEpoch 1170 | Y | Y | Y | if Leader -> `addAcknowledgementFrom(id)` **throws for non-voter** (F1) |
| EndQuorumEpoch 1347 | Y | Y | Y | `resignedStateOrThrow()` 1394 (safe, see 7.6) |
| Fetch 1693 | Y | Y + topicId | Y | `followerStateOrThrow()` 1744 (**unsafe for Unattached-with-leader**, F2) |
| FetchSnapshot 2082 | Y | Y | Y | `followerStateOrThrow()` 2126 (only sent as Follower -> safe) |
| UpdateVoter 2560 | n/a | n/a | Y | `followerStateOrThrow()` 2596 (only sent as Follower) |
| ApiVersions 2314 | | | **no epoch handling** | ignores if not leader 2318 |
| Add/RemoveVoter 2356/2454 | | | **no epoch handling** | `followerStateOrThrow()` 2368/2466 (only sent as observer-Follower; RequestManager reset on any transition) |

### Directory-id (old incarnation) analysis

* **Fetch from (id X, old dir D_old) when the voter set holds (X, D_new)**: `getOrCreateReplicaState`
  (LeaderState:846-854) puts it into `observerStates`; `updateReplicaState` returns
  `isVoter(state.replicaKey) && maybeUpdateHighWatermark()` (LeaderState:812) -> not counted for HW.
  `updateFetchedVoters` (LeaderState:275-278) requires `matchesKey` -> not counted for check-quorum.
  **Safe.**
* **Fetch with empty dir id while voter has a dir id**: `matchesKey` (LeaderState:982-991) returns
  false (Optional.equals) -> observer. **Safe.**
* **kraft.version 0 (voter state without dir id)**: any directory with the same id matches
  (LeaderState:988-989). Known protocol limitation (the reason for KIP-853), not a bug. On upgrade
  `updateVoterAndObserverStates` reuses that state and `setReplicaKey` stamps the new dir id
  (LeaderState:887-895, 950-972), so an endOffset that came from another directory may be carried
  into the (X, D) voter until X/D fetches again. HW is already monotone so this cannot lower HW; it
  could only hold a stale (possibly higher) offset that was already counted under v0 semantics.
* **BeginQuorumEpoch ack** keyed by id (KRC:1220). Compensation: request carries voter key
  (KRC:2969-2977) and the receiver answers `INVALID_VOTER_KEY` on mismatch (KRC:1146-1161), which
  `maybeHandleCommonResponse` swallows (KRC:2685-2693). In addition `hasAcknowledgedLeader` no longer
  drives resending (`needToSendBeginQuorumRequests` uses `lastFetchTimestamp`, LeaderState:203-212),
  so an incorrect ack would be cosmetic.
* **Vote grant** keyed by id (`recordGrantedVote(remoteNodeId)` KRC:1015). Compensation: voter key
  check at the receiver (KRC:902). The old incarnation and the new one cannot both answer at the same
  endpoint in practice.
* **EndQuorumEpoch preferred successors**: matching accepts empty dir id or equal dir id
  (KRC:1333-1336); a non-matching (old) dir id just yields max backoff. **Safe.**
* **Disk-replacement auto-remove** (KRC:3392-3402): the observer with (X, D_new) asks to remove
  (X, D_old). Leader's BeginQuorumEpoch to X is answered with INVALID_VOTER_KEY, so this specific
  scenario does **not** trigger F1.

---

## 2. Q2 - High watermark

### Leader
* HW computed in `maybeUpdateHighWatermark` (LeaderState:660-719) as the `voterStates.size()/2`-th
  element of voters sorted by descending endOffset (leader's LEO included only if leader is in
  `voterStates`).
* **Own-epoch commit rule**: `if (highWatermarkUpdateOffset > epochStartOffset)` (LeaderState:680).
  `epochStartOffset` = LEO at election (KRC:652/666); the LeaderChange control record is the first
  record of the epoch (KRC:673). Requiring HW > epochStartOffset means that record is on a majority.
  Follower endOffsets only come from VALID fetches (KRC:1634-1639) where `validateOffsetAndEpoch`
  checked `lastFetchedEpoch`, so offset-based comparison is equivalent to log-matching. **Verified.**
* **Monotonic**: decreases are refused (LeaderState:695-701, logged only). Leader starts every epoch
  with HW empty (QuorumState:699-708 comment; LeaderState:77). **Verified monotone per epoch;
  globally monotone** because new HW > epochStartOffset >= any previously committed offset (Leader
  Completeness).
* Follower endOffset may regress (only a WARN, LeaderState:796-801); harmless for HW because of the
  monotone guard, but means `voterStates` can hold a value lower than what was used for commit.
* **Voter set grows/shrinks**: `updateVoterAndObserverStates(lastVoterSet)` runs only from
  `updateLocalState` (LeaderState:753-769), invoked from `flushLeaderLog` after each append
  (KRC:679-683, 3106). Uses the *latest* (possibly uncommitted) voter set, per KIP-853. New voter
  without endOffset sorts last (compareTo LeaderState:1025-1034) -> HW can stall, never regress.
  Removed voters move to `observerStates` (LeaderState:903-907).
* **Leader removed from voter set**: leader's own ReplicaState moves to observers; HW is the median
  of remaining voters only; `needToSendBeginQuorumRequests` skips local id; check-quorum majority is
  adjusted (LeaderState:259-261). Resignation depends on `RemoveVoterHandler.highWatermarkUpdated` -
  see F3.

### Follower
* `updateFollowerHighWatermark`: `newHW = min(endOffset().offset(), leaderHW)` (KRC:342-354).
  **Verified HW = min(leader HW, local LEO).**
* `FollowerState.updateHighWatermark` throws on regression (FollowerState:201-208) - a fail-stop
  assertion; HW is carried across states via `state.highWatermark()` in every QuorumState
  transition (QuorumState:416, 465, 519, 593, 631, 650). New leader advertises -1 until it commits
  (`buildFetchResponse` 1423-1425 -> `OptionalLong.empty()` at 1815-1816 -> no update). When the new
  leader's HW appears it is > its epochStartOffset >= old committed HW, so no regression.
* Snapshot path: after full download `truncateToLatestSnapshot()` then
  `updateFollowerHighWatermark(state, log.highWatermark())` (KRC:2190-2203). Leader only offers a
  snapshot when follower fetchOffset==0 or < leader log start (KRC:1625-1631), so snapshot end >
  follower LEO >= follower HW. **Safe.** If truncation does not happen the node throws ISE (2205).
* **Truncation below HW**: guarded in the diverging branch (KRC:1752-1758, KafkaException ->
  fail-stop). `truncateToEndOffset(divergingOffsetAndEpoch)` can truncate to <= the diverging end
  offset; the guard compares the diverging end offset, not the returned truncation offset, but the
  leader computes the diverging offset from its own log which contains every committed entry, so
  returned truncation offset >= HW in Raft-correct executions. Worth an invariant in the model.

---

## 3. Q3 - Fetch response handling

* Stale epoch response: `epoch < quorum.epoch()` -> ignored (KRC:2657-2659). Higher epoch / FENCED /
  NOT_LEADER -> `maybeTransition` and return (2660-2667); records are **not** appended in that
  response. Equal epoch, unknown leader -> become follower then process the same response (2668-2682).
* Records are appended only when epoch == local epoch and local is Follower (1744, 1813). The
  RequestManager additionally drops any response whose correlation id is not the one in flight
  (KRC:2859-2863, RequestManager:226-233), and all state transitions call `resetConnections()`
  (KRC:675, 723, 738, 750, 760, 766). **No path appends records from a stale-epoch leader.**
* `appendAsFollower(records, quorum.epoch())` (1842): CorruptRecord/InvalidRecord are swallowed
  (1844-1850) and the fetch timer is still reset (1820) - liveness only (refetch loop), no safety
  impact.
* Diverging epoch: truncation guarded by HW (above). Snapshot id with invalid parts -> `return false`
  (1773-1786). Snapshot id is intentionally not validated against the local log (1793-1796).
* The response source is never compared with `quorum.leaderId()`. Compensation: follower sends Fetch
  only to the leader or bootstrap servers; a non-leader at the same epoch answers
  NOT_LEADER_OR_FOLLOWER (KRC:2806-2810); a stale leader answers with its lower epoch; consistency
  of (epoch, leaderId) is asserted in `hasConsistentLeader` (2607-2619) when transitioning.

### F2 (details) - Unattached-with-leader + bootstrap Fetch answered by the leader
1. `QuorumState.initialize` (QuorumState:204-231): persisted election has leader L but
   `lastVoterSet().listeners(L)` is empty -> `UnattachedState(epoch, OptionalInt.of(L), ...)`. Same
   state reachable from `prospectiveTransitionAfterElectionLoss` (KRC:3294-3305) when leader endpoints
   are unknown. The code comment says the node should "discover the leader's endpoint through Fetch
   requests to the bootstrap servers".
2. `pollUnattached` -> `pollUnattachedCommon` -> `maybeSendFetchToAnyBootstrap` (KRC:3512-3538).
3. If the chosen bootstrap server is L itself, L validates `currentLeaderEpoch == epoch` and is
   leader -> returns NONE with records/HW.
4. `maybeHandleCommonResponse`: epoch equal; third branch requires `!quorum.hasLeader()` but
   `UnattachedState.election()` reports the leader (UnattachedState:75-78), so it falls to
   `return Optional.empty()` (2698).
5. `handleFetchResponse` -> `quorum.followerStateOrThrow()` (1744) -> `IllegalStateException`
   "Expected to be Follower" (QuorumState:758) -> raft IO thread fatal.
   If a *non-leader* bootstrap server answers, it returns NOT_LEADER_OR_FOLLOWER and
   `maybeTransition` takes the "more endpoints" branch (2717-2723) -> Follower, so the crash is
   probabilistic (random bootstrap choice, RequestManager:114).
   Realistic trigger: observer/voter persisted (epoch e, leader L) via `durableTransitionTo` before
   replicating the VotersRecord that added L (e.g. L added via AddVoter after this node's
   bootstrap checkpoint), then restarts while L is still leader of e.
   Missing check: the "leader present, but local is not Follower" case in `maybeHandleCommonResponse`
   should transition (as `maybeTransition` does) rather than fall through.

---

## 4. Q4 - Check-quorum, resignation, observers

* Check-quorum timer: 1.5 x fetchTimeout (LeaderState:153); reset when `fetchedVoters.size() >=
  majority` (LeaderState:251-268); expiry -> `transitionToResigned` (KRC:3186-3189). Fetch and
  FetchSnapshot both feed it (KRC:1637 via LeaderState:810; KRC:2032).
* **F5**: `fetchedVoters` holds ids and is only cleared on reset; voter-set changes don't prune it,
  so a removed voter's id can complete a "majority" of the new set. `voterStates.size()==1` returns
  `Long.MAX_VALUE` (225-227) even if the leader is not that voter (leader removed from {L,A} -> {A}):
  check-quorum never fires. Liveness only.
* **F3**: Self-removal resign is only triggered inside `RemoveVoterHandler.highWatermarkUpdated`
  while `removeVoterHandlerState` is present (RemoveVoterHandler:214-242). `pollLeader` expires the
  pending operation after requestTimeoutMs (KRC:3192-3199 -> ChangeVoterHandlerState:251-257,
  `resetRemoveVoterHandlerState(REQUEST_TIMED_OUT, ...)`). If the VotersRecord commits later (e.g.
  remaining set {A,B} and B was slow/down), `highWatermarkUpdated` finds no state and
  `requestResign()` is never called; no other code path checks "leader not in voter set"
  (only callers of `requestResign` are RemoveVoterHandler:239 and the public `resign()` KRC:3814).
  The non-voter leader keeps leading as long as voters fetch (check-quorum keeps resetting).
  Safety is preserved (HW only counts voters; it can never win another election), but this
  contradicts the KIP-853 intent and blocks operator retry (RemoveVoter for it now returns
  VOTER_NOT_FOUND).
* Observers learning stale endpoints: endpoints taken from the message when present; otherwise from
  `lastVoterSet().listeners(leaderId)` (e.g. KRC:1719-1730, 2648-2655), which can be stale for
  kraft.version 0 peers. Same-epoch endpoint upgrades only when the new set is strictly larger
  (2719, QuorumState:558). An observer *can* be told about a leader that is no longer in the voter
  set (by design while the removal is uncommitted; indefinitely in F3).

---

## 5. Q5 - Responses after a state transition

* Primary filter: `requestManager.isResponseExpected(source, correlationId)` (KRC:2859) +
  `resetAll()` on every transition (`resetConnections` 647-649 called from onBecomeLeader 675,
  onBecomeCandidate 723, onBecomeProspective 738, transitionToUnattached 750, transitionToResigned 760,
  onBecomeFollower 766). Transitions without reset: `unattachedAddVotedState` /
  `prospectiveAddVotedState` (KRC:931-936) - same epoch, same state kind; harmless.
* Request timeout: `hasRequestTimedOut` -> `reset(leaderNode)` (KRC:3421-3424) so a late response
  from the old leader connection is ignored.
* Secondary filter: epoch comparison in `maybeHandleCommonResponse`.
* EndQuorumEpoch response after resign: Resigned only exits via epoch+1 (KRC:3173) or a higher-epoch
  message, and both reset connections -> `resignedStateOrThrow` (1394) cannot fail. **Safe.**
* **Gap (F1)**: voter-set change while Leader does **not** reset connections or filter by
  membership. Timeline: leader sends BeginQuorumEpoch to V (only voters whose lastFetchTimestamp is
  >= fetchTimeout/2 old, or all voters right after election: LeaderState:156, 203-212) -> RemoveVoter(V)
  appended -> `flushLeaderLog`/`updateLocalState` drops V from `voterStates` (LeaderState:881-909) ->
  V's NONE response arrives within requestTimeout -> `addAcknowledgementFrom(V)` ->
  `ensureValidVoter` throws `IllegalArgumentException("Unexpected acknowledgement from non-voter")`
  (LeaderState:834-839) -> no catch in `handleResponse` (KRC:2743-2765) -> fatal. `LeaderStateTest:171`
  even asserts this throw, but no KafkaRaftClient test covers the race.
* Vote responses: `EpochElection` voter set is fixed at nominee creation, and responses only come
  from those voters; `recordGrantedVote` on a non-voter would throw (EpochElection:129) but is not
  reachable. Resigned `acknowledgeResignation` for non-voter would throw (ResignedState:118) but the
  voter set is fixed at resign time and requests only go to `unackedVoters`. **Safe.**

---

## 6. Q6 - TODO/FIXME and error-handling gaps

* No TODO/FIXME in KafkaRaftClient/LeaderState/FollowerState/RequestManager/QuorumState.
  `KafkaRaftLog.java:289` "Temporary log message until we fix KAFKA-14825" (log HW may differ from
  replica HW).
* `maybeHandleCommonResponse` throws ISE on partition-level INVALID_REQUEST (KRC:2694-2695): a peer
  answering INVALID_REQUEST (e.g. Vote with `remoteNodeId<0` or the illegal-epoch check 867-884)
  kills the requester. Fail-stop, only on protocol violation.
* `maybeTransition` throws ISE on inconsistent leader (KRC:2707-2710) - correct Raft assertion;
  good TLA+ invariant (`ElectionSafety`).
* F4: FetchSnapshot lacks Fetch's catch-all.
* `transitionToFollower` throws on empty endpoints (KRC:790-800); reachable only if a response lacks
  nodeEndpoints *and* the leader is not in the local voter set (pre-KIP-853 RPC versions).
* `handleUnexpectedError` only logs and backs off (KRC:2737-2741).

---

## 7. Checked and found safe (with compensating mechanism)

1. Records from stale-epoch leader never appended - epoch filter (2657) + correlation-id filter (2859) + reset on transitions.
2. Leader HW only after own-epoch entry is majority-replicated - `> epochStartOffset` (LeaderState:680).
3. Leader HW monotone - explicit refusal (LeaderState:695-701); new epoch starts empty.
4. Follower HW monotone & <= LEO - `min` (KRC:347) + throw on regression (FollowerState:201).
5. Truncation below HW - explicit KafkaException (KRC:1752-1758).
6. EndQuorumEpoch response vs Resigned - resigned only exits with epoch bump + reset.
7. Old-incarnation Fetch can't advance HW or check-quorum - `matchesKey`/`isVoter` (LeaderState:276, 812, 876-879).
8. Old-incarnation Vote/BeginQuorumEpoch - receiver `isValidVoterKey` (KRC:902, 1147) + INVALID_VOTER_KEY swallowed.
9. Parked fetch completion revalidates epoch/leadership (`tryCompleteFetchRequest` 1614) and purgatory is failed on resign/follow (754-758, 770-774).
10. Diverging/snapshot fetches never update replica state (only VALID at 1634-1639).
11. Fetch with `replicaId == leaderId` - ISE caught (1654) -> UNKNOWN_SERVER_ERROR.
12. Only one Fetch in flight - `hasAnyInflightRequest` (RequestManager:74-95, KRC:3427).

---

## 8. Dynamic probe status

Probe test file: `.specula-output/analysis/SpeculaProbeTest.java` (copy of
`kafka/raft/src/test/java/org/apache/kafka/raft/SpeculaProbeTest.java`). Each test asserts
`assertDoesNotThrow(context::poll)`, so a FAILURE confirms the bug.

Run: `./gradlew --max-workers=2 :raft:test --tests org.apache.kafka.raft.SpeculaProbeTest -x checkstyleTest -x checkstyleMain -x spotbugsMain -x spotbugsTest`
(checkstyle skipped: missing license header in probe files). **Both tests FAIL as predicted (bugs confirmed):**

* F1 `testLateBeginQuorumEpochResponseFromRemovedVoter`: voters {100 (leader), 101, 102}; leader's
  BeginQuorumEpoch to 102 left in flight; 101 fetches (HW established); RemoveVoter(102) handled and
  VotersRecord appended; then 102's NONE response is delivered ->
  `java.lang.IllegalArgumentException: Unexpected acknowledgement from non-voter 102`
  at `LeaderState.ensureValidVoter(LeaderState.java:837)` <- `addAcknowledgementFrom(830)` thrown out of `poll()`.
* F2 `testUnattachedWithLeaderReceivesFetchResponseFromLeader`: local 10 (observer), voters {11},
  persisted election (epoch 5, leader 12), bootstrap = [12]; Fetch NONE response from 12 ->
  `java.lang.IllegalStateException: Expected to be Follower, but the current state is
  UnattachedState(epoch=5, leaderId=OptionalInt[12], ...)` at `QuorumState.followerStateOrThrow(758)`
  <- `KafkaRaftClient.handleFetchResponse(1744)`.

The probe file was removed from the kafka tree after the run (kept only in `.specula-output/analysis/`).
F3-F5 are code-verified only (not executed).

---

## 9. Suggested TLA+ modeling hooks

* Model BeginQuorumEpoch request/response with the leader's `voterStates` changing in between
  (AddVoter/RemoveVoter actions interleaved); invariant: "leader never processes an ack from a non-voter"
  (or model the throw as a `Crash` action and check that it's unreachable).
* Model QuorumState initialization from persisted election with leader not in local voter set,
  plus bootstrap-Fetch to arbitrary node; invariant: handler precondition `state = Follower` holds
  when a NONE Fetch response is processed.
* Model RemoveVoter of the leader with operation timeout; liveness/invariant: "a leader not in the
  committed voter set eventually resigns" (expected to be violated -> F3).
* Safety invariants: LeaderHWMonotone, FollowerHW <= LEO, FollowerHWMonotone, NoTruncateBelowHW,
  CommitOwnEpoch (HW > epochStartOffset), ElectionSafety (hasConsistentLeader never fails).
