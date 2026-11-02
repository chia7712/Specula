# Deep analysis: KRaft elections (pre-vote KIP-996, dynamic quorum KIP-853)

Source: `kafka/` trunk @ 8aa57006ff. Paths below are relative to
`raft/src/main/java/org/apache/kafka/raft/` unless stated. "KRC" = `KafkaRaftClient.java`,
"QS" = `QuorumState.java`.

Files read in full: QuorumState, UnattachedState, ProspectiveState, CandidateState,
FollowerState, ResignedState, ElectionState, FileQuorumStateStore, EpochState, NomineeState,
internals/EpochElection. In KRC: initialize, state-transition helpers (487-805), handleVoteRequest /
handleVoteResponse / maybeHandleElectionLoss (835-1062), BeginQuorumEpoch and EndQuorumEpoch
handlers (1075-1400), handleFetchResponse (1693-1825), appendAsFollower (1835-1865),
hasConsistentLeader / maybeHandleCommonResponse / maybeTransition / handleResponse /
validateVoterOnlyRequest / isValidVoterKey / handleInboundMessage / maybeSendRequest (2607-2921),
every poll* method (3119-3556). Tests: KafkaRaftClientPreVoteTest (test list + key bodies),
QuorumStateTest (initialize* tests), RaftClientTestContext.

---------------------------------------------------------------------------------------------------

## Findings ranked by severity

### F1 (Medium, availability; CONFIRMED by unit test) Unattached-with-known-leader plus a successful Fetch from that leader crashes the raft thread

* `UnattachedState` can hold a leader id without endpoints:
  - QS:204-231 `initialize`: persisted leader not in `lastVoterSet()` → `new UnattachedState(epoch, OptionalInt.of(leaderId), ...)`.
  - KRC:3294-3305 `prospectiveTransitionAfterElectionLoss`: leader known but `leaderEndpoints().isEmpty()` → `transitionToUnattached(epoch, leaderId)`.
* An Unattached replica (voter or observer) sends FETCH to bootstrap servers: KRC:3535-3538
  `pollUnattachedCommon -> maybeSendFetchToAnyBootstrap`, with `currentLeaderEpoch = quorum.epoch()`.
* If the bootstrap server it picks is the leader for that epoch, the leader accepts the fetch
  (validateLeaderOnlyRequest KRC:2801-2815 passes: epoch equal, isLeader) and answers `error=NONE`,
  `currentLeader=(L,E)`, with leader endpoints.
* In `maybeHandleCommonResponse` (KRC:2640-2699): `epoch == quorum.epoch()`, no error, and
  `quorum.hasLeader()` is **true**, so neither the epoch branch (2660) nor the
  "discover leader" branch (2668-2682, which needs `!quorum.hasLeader()`) is taken. It returns `Optional.empty()`.
* KRC:1744 `FollowerState state = quorum.followerStateOrThrow();` then throws
  `IllegalStateException("Expected to be Follower ...")`. KafkaRaftClientDriver:62-66 turns this into
  `fatalFaultHandler.handleFault(...)`, which stops the controller process.
* No other mechanism transitions Unattached(E, L, no endpoints) to Follower while the epoch stays
  the same. `maybeTransition` (KRC:2717-2724) handles the case where "endpoints grow"
  (`leaderEndpoints.size() > quorum.leaderEndpoints().size()`), but it is only reached for requests
  (BeginQuorumEpoch/EndQuorumEpoch) or for responses with FENCED_LEADER_EPOCH/NOT_LEADER_OR_FOLLOWER or a higher epoch.
  A non-leader bootstrap server answers NOT_LEADER_OR_FOLLOWER, which does reach maybeTransition and
  recovers. Only the case where the leader itself answers successfully is broken.
* Tests cover only the unknown-leader case (KafkaRaftClientTest:2083
  `testUnattachedAsVoterCanBecomeFollowerAfterFindingLeader` uses `withUnknownLeader`).
  `testProspectiveLosesElectionHasLeaderButMissingEndpoint` (PreVoteTest:1015) creates the state but never delivers a Fetch response.
* How it is reached in production: a KIP-853 cluster where leader L was added after this replica's
  local log (or snapshot) ends, so L is missing from the local voter set. The replica learned L
  through BeginQuorumEpoch, which put L in `quorum-state`. It then restarts and L is in `controller.quorum.bootstrap.servers`.
* Modeling note: in the spec, `Unattached` should carry an optional leader, and a successful Fetch
  response in Unattached with leader already set must be handled. Otherwise the model silently diverges from the code.

### F2 (Low-Medium, liveness/disruption) A PreVote request with a higher epoch bumps the receiver's epoch (it also deposes a leader)

* KRC:896-898: `if (replicaEpoch > quorum.epoch()) transitionToUnattached(replicaEpoch, OptionalInt.empty());`
  runs for both standard votes and pre-votes. The `preVote` flag is not consulted there, and it runs
  before the voter-key check (KRC:901-918) and before `canGrantVote`.
* A leader at E that receives a pre-vote with epoch E+1 durably moves to Unattached(E+1). Its leadership ends.
* Result: pre-vote prevents the prospective from inflating its own epoch (QS:621-635 is
  memory-only and keeps epoch()). It does not stop a node whose epoch is already higher from
  disrupting the quorum. Example: a Candidate at E+1 gets majority pre-votes, then is
  partitioned before any vote request is delivered. It falls back to Prospective(E+1), heals, and
  its pre-vote at E+1 kicks every node, the leader included, to E+1. (etcd/raft does not change its term for a PreVote.)
* The same applies to a request whose `voterKey` does not match (INVALID_VOTER_KEY): the epoch is bumped first (KRC:896 before 902).
* It is bounded: each event raises the epoch only once, to an epoch some candidate has already reached.

### F3 (Low-Medium, design) One UNSUPPORTED_VERSION response skips pre-vote entirely

* KRC:960-967: if `topLevelError == UNSUPPORTED_VERSION && quorum.isProspective()`, the code calls
  `transitionToCandidate(currentTimeMs)` immediately, with no majority of pre-votes. That transition is a durable epoch bump (QS:638-654).
* In a mixed-version voter set, any single old voter makes every prospective behave like
  classic Raft. Once that happens, disruption protection is gone. It only fires on a response the client expected (isResponseExpected, KRC:2859).

### F4 (Low) Version-0 quorum-state drops votedDirectoryId, and the empty directory id then matches any incarnation

* ElectionState.toQuorumStateData (ElectionState:123-147): version 0 writes `votedId` only.
  `quorumStateVersion` comes from `partitionState.lastKraftVersion()` (QS:731,
  FileQuorumStateStore:141-149). The value is the *uncommitted* latest kraft.version, so it can change back if the log is truncated.
* On read, `ReplicaKey.of(votedId, ZERO_UUID → empty)` (ElectionState:205-208).
  `unattachedOrProspectiveCanGrantVote` (QS:902-906) and `isVotedCandidate` (ElectionState:80-84) treat an empty
  stored directory id as a wildcard: `votedReplicaKey.directoryId().isEmpty() || ...equals(...)`.
* So after a restart with a v0 file, a replica that voted for (2, dirA) in epoch E can also grant
  (2, dirB) in E. Before the restart it would not. This only matters if two incarnations of
  id 2 are candidates at the same epoch under kraft.version 0, which is a misconfiguration (duplicate node id). Same-id re-grants skip the log check (QS:904-905). That is correct in Raft.

### F5 (Low, edge/recovery) A quorum-state epoch below the log epoch silently drops votedKey and leader

* QS:155-172: if `election.epoch() < logEndOffsetAndEpoch.epoch()`, the replica initializes
  `Unattached(logEpoch, no leader, no votedKey)`. Normal operation cannot produce this: the
  quorum-state write always precedes any log append in that epoch (QS:729-733 is called before
  onBecomeLeader appends at KRC:666-673, and followers persist before fetching). It happens only
  if `quorum-state` is lost or rolled back, for example when an operator deletes the file.
  - In that case, if this node had voted or was leader in logEpoch, it can vote again in that
    epoch. Two leaders become possible. Also, if it was leader in logEpoch, a later
    response with `leaderId == local` at that epoch makes hasConsistentLeader (KRC:2610-2612) return false, and the process crashes with IllegalStateException at KRC:2708.
  - The file is missing entirely → QS:130-139 starts at epoch 0, and the same branch applies.

### F6 (Low, liveness) A Prospective that already knows a leader ignores BeginQuorumEpoch from that same leader

* maybeTransition (KRC:2717-2724) only transitions at the same epoch if `!quorum.hasLeader()` or the endpoints grew.
  Prospective(E, leader=L, endpoints known), which comes from Follower via fetch timeout (QS:621-635 keeps leaderId and endpoints),
  receiving BeginQuorumEpoch(E, L) stays Prospective until it times out or loses the pre-vote (KRC:3280-3286, 1049-1056).
  It then becomes Follower again (KRC:3296-3302). Tests only cover the unknown-leader variant (PreVoteTest:847-879).

### F7 (Low, observational) Requester membership is never checked in Vote, BeginQuorumEpoch or EndQuorumEpoch

* handleVoteRequest has no `quorum.isVoter(replicaKey)` check. `isVoter(...)` appears in KRC only at
  1852, 3310 and 3514. A Leader explicitly answers pre-votes from observers (test
  PreVoteTest:511 `testLeaderAcceptPreVoteFromObserver`), and Unattached/Prospective would grant a
  standard vote to a non-voter whose log is up to date. Safety then rests on the requester
  counting votes against *its own* voter set (EpochElection) and on the log-up-to-date check. This matches KIP-853's design, in which voter sets may differ between replicas.
* BeginQuorumEpoch: `maybeTransition` runs (KRC:1138-1143) before the voter-key check (KRC:1146-1161),
  so a request addressed to another incarnation still makes this node a follower. That is benign: the leader is real.
* EndQuorumEpoch carries no voter key. A follower shortens its fetch timeout only when `state.leaderId() == requestLeaderId` (KRC:1300-1317).
  Preferred-successor matching treats an empty directory id as a wildcard (KRC:1332-1340).

---------------------------------------------------------------------------------------------------

## Q1 Vote durability

* **The vote is persisted synchronously before the response is built.** handleVoteRequest
  KRC:931-937 calls `quorum.unattachedAddVotedState` / `prospectiveAddVotedState`, and only then
  `buildVoteResponse` (KRC:945). Both helpers call `durableTransitionTo` (QS:458-469, 510-523). That method
  writes the file and then switches the in-memory state (QS:729-733). The whole thing runs on the single raft thread.
* **The file write is atomic and fsynced.** FileQuorumStateStore:166-195: write `quorum-state.tmp`,
  `writer.flush()`, `fileOutputStream.getFD().sync()` (181), then `Utils.atomicMoveWithFallback`
  (183). The default `needFlushParentDir=true` fsyncs the parent directory (clients Utils.java:928-957).
  The non-atomic fallback (`Files.move REPLACE_EXISTING`) is used only if ATOMIC_MOVE fails. An
  IOException becomes UncheckedIOException and is fatal, and the memory state stays unchanged because the write comes first.
* **Grant paths that skip a write are all safe:**
  - Already voted for the same candidate (QS:902-906): returns true without a new write. The earlier vote is already durable.
  - Pre-vote grants never write and never change votedKey (KRC:931 `!preVote && voteGranted`).
  - Follower, Candidate, Leader and Resigned never grant a standard vote:
    FollowerState:239-254, CandidateState:146-165, LeaderState:1051-1059, ResignedState:150-164.
  - Unattached with a leader rejects a standard vote unless it already voted for that candidate (QS:915-923).
* **votedKey is kept across same-epoch transitions:** transitionToUnattached (QS:409),
  transitionToFollower (QS:583), transitionToProspective (QS:629, `votedKey()`), and
  Candidate→Prospective (CandidateState.election() writes self as voted, CandidateState:117-123).
  The leader writes `votedKey=empty` (LeaderState:617-619). That is safe because a restart as leader becomes Resigned (QS:173-189), and Resigned refuses standard votes.
* **Restart:** QS:141-259. Voted for self → Candidate at the same epoch (QS:190-203; vote requests
  are re-sent and earlier grantors re-grant via the same-id rule). Leader known → Follower, or Unattached if no
  endpoints, keeping `optionalVotedKey()` (QS:204-244). Otherwise Unattached with votedKey (QS:245-255).
  Directory-id loss: see F4. Lower state epoch: see F5.
* **Log append versus quorum-state write:** the state is always written first. The candidate's
  transition is durable (QS:644) before any vote request. Leader: `transitionToLeader` is durable
  (QS:725) before `appendStartOfEpochControlRecords` (KRC:666-673). Follower: the transition is durable before
  the first Fetch. A crash in between leaves state epoch ≥ log epoch, which is safe. The reverse order can only arise from F5.
* Controllers (canBecomeVoter = controller role, core KafkaRaftManager.scala:177) always fsync follower appends (KRC:1852-1858), so the voter set seen after a restart matches the one seen before.

## Q2 Pre-vote

* **Voter set and LEO:** ProspectiveState builds `new EpochElection(voters.voterKeys())` from
  `partitionState.lastVoterSet()` (QS:630, ProspectiveState:76), which includes uncommitted VotersRecords.
  The request carries `endOffset()` = (log.endOffset, lastFetchedEpoch) at send time (KRC:2980-2992).
  Requests go to `epochElection().unrecordedVoters()`, and endpoints are looked up in the current `lastVoterSet()` (KRC:3223-3246).
* **The prospective's own epoch and vote:** never bumped. The state change is memory-only (QS:621-635) and keeps
  `epoch()` and `votedKey()`. Receivers never record a vote for a pre-vote (KRC:931). Receivers *do* bump their epoch when the prospective's epoch is higher (F2).
* **Request validation:** pre-vote is illegal if `lastEpoch > replicaEpoch`; a standard vote is illegal if
  `lastEpoch >= replicaEpoch` (KRC:867). Both get FENCED_LEADER_EPOCH if `replicaEpoch < epoch` (KRC:2773-2781).
* **Grant rules (isLogUpToDate = request (offset,epoch) ≥ local endOffset, KRC:925-929):**

| State | PreVote | Standard vote |
|---|---|---|
| Unattached (any leader / voted) | grant iff log up to date (QS:894-901). Leader and vote are ignored | voted: grant iff same id (dir match or stored dir empty); leader known: reject; else log check (QS:902-931) |
| Prospective | same as Unattached (ProspectiveState:103-113) | same as Unattached. A grant persists via prospectiveAddVotedState |
| Follower | grant iff `!hasFetchedFromLeader && upToDate` (FollowerState:239-241). This is the only "heard from leader recently" check; the flag is set only on a successful Fetch (FollowerState:143-146), not by BeginQuorumEpoch | reject |
| Candidate | grant iff up to date (CandidateState:151-153) | reject |
| Leader | reject (LeaderState:1051-1059) | reject |
| Resigned | grant iff up to date (ResignedState:150-153) | reject |

* **Voter set changing mid-election:** it cannot happen within one state. Nominee states never change
  the log: Prospective and Candidate do not fetch (pollProspective/pollCandidate, KRC:3251-3292),
  and `partitionState.updateState/truncateNewEntries` are called only from appendAsFollower,
  appendAsLeader, the follower truncation path and the snapshot load path (KRC:1768, 1860, 1870, 2201).
  The election is rebuilt from the current `lastVoterSet()` on every new state: Prospective (QS:630), Candidate (QS:649),
  and leader (QS:715). So Prospective→Candidate uses the same voter set.
* **Stale responses:** each response must match the single in-flight correlation id for that node
  (`requestManager.isResponseExpected`, KRC:2859; RequestManager:226-233, 352-354). Every nominee
  entry clears it with `resetConnections()` (KRC:723, 737, 750, 766, 675). So a pre-vote grant
  from Prospective(E) cannot be counted by Candidate(E+1), and the epoch check (KRC:2657) also drops it.
  Late standard-vote responses from Candidate(E) reaching Prospective(E) are blocked the same way. If a model lacks the
  correlation-id reset, it will produce spurious "pre-vote counted as vote" traces. Model the reset, or require response epoch plus request type to match.
* **A partitioned prospective** cannot raise its own epoch. It loses the pre-vote or times out and goes to
  Follower or Unattached at the same epoch (KRC:3294-3305). Disruption is still possible through F2 and F3.
* **Leader discovery from Prospective:** BeginQuorumEpoch, EndQuorumEpoch and responses go through
  maybeTransition / maybeHandleCommonResponse. A higher epoch with a leader leads to Follower (with no leader, to Unattached).
  At the same epoch, an unknown leader leads to Follower, keeping votedKey (QS:583). For a known leader, see F6.
  After an election loss, a known leader with endpoints leads to Follower, otherwise Unattached(E, leader) (KRC:3294-3305), which is the entry point for F1.

## Q3 Voter-set membership

* Starting an election requires being a voter: transitionToProspective throws if `isObserver()`
  (QS:605-615), and checkValidTransitionToCandidate and transitionToLeader do the same (QS:656-693).
  The callers check `quorum.isVoter()` first (KRC:3310, 3514). CandidateState's constructor checks
  `voters.isVoter(ReplicaKey.of(localId, localDirectoryId))` (CandidateState:58-67). isVoter
  uses id plus directory id against `lastVoterSet()` (QS:332-344). A replica whose id is in the set with a different directory id is an observer (KRC:3392-3402 sends RemoveVoter for the old key).
* A removed voter that has not yet read its removal record still considers itself a voter. It can
  campaign and its requests are not refused for membership (F7). Votes are counted only against its own
  voter set, and the log-up-to-date check is the safety net. That is standard Raft single-server-change reasoning: AddVoterHandler:149-183
  requires a known HWM and no uncommitted VotersRecord before a change.
* Vote counting uses node id only. `EpochElection` is keyed by `ReplicaKey::id` (EpochElection:31-35), and votes
  are recorded via `responseMetadata.source().id()` (KRC:957, 1015-1018).
  **Stale incarnations:** a vote request names the target's `voterKey` (with directory id). A receiver whose directory id differs
  answers INVALID_VOTER_KEY (KRC:901-918, isValidVoterKey 2786-2795), and the caller does not count it
  (KRC:2685-2693 returns handled). Gaps: the check is skipped when `voterId < 0` (old RPC versions) or when the directory id is empty
  (kraft.version 0), via `.orElse(true)` / `if (key.directoryId().isEmpty()) return true`. In that case a stale incarnation with the same id is counted.
* `recordVote` throws IllegalArgumentException for an id outside the election (EpochElection:126-131).
  This is unreachable because destinations come from the election itself and the voter set is fixed during nominee states (see Q2).

## Q4 Two leaders in one epoch: every path that sets a leader for an epoch

1. Self → Leader: only from Candidate with `epochElection().isVoteGranted()` (QS:691-697, KRC:685-692);
   the epoch was raised durably when it became Candidate. Single-voter shortcut: initialize → transitionToProspective → isVoteGranted
   with one voter (KRC:571-575, 734-740).
2. maybeTransition (BeginQuorumEpoch KRC:1138, EndQuorumEpoch KRC:1293, and response errors via
   KRC:2660-2667) is guarded by `hasConsistentLeader` (KRC:2607-2619). That throws if the same epoch already has
   a *different* known leader, or if the message names us as leader while we are not leader (fail-stop, not silent).
3. maybeHandleCommonResponse at the same epoch with `!quorum.hasLeader()` → transitionToFollower (KRC:2668-2682)
   for Vote, BeginQuorumEpoch, EndQuorumEpoch, Fetch and FetchSnapshot responses. There is no hasConsistentLeader check, but none is needed because
   no leader is known. A Candidate in E can adopt a leader X in E from a vote response, keeping votedKey=self (QS:583).
4. QuorumState.transitionToFollower (QS:529-598) rejects a lower epoch, self as leader, Leader at the same epoch, and
   Follower at the same epoch unless the endpoint set grows.
5. Initialize from the file (QS:204-244).

No path found that switches a replica from leader A to leader B within one epoch. Every same-epoch
follower transition either requires that no leader is known, or passes hasConsistentLeader (same id). Uniqueness of the
winner in an epoch rests on: (a) one durable vote per epoch, see Q1; (b) Candidate, Follower,
Leader and Resigned never granting standard votes; (c) a quorum of the candidate's own voter set. Residual
risks: F4 (directory-id wildcard after a v0 file) and F5 (state file loss).

## Q5 TODO/FIXME and inconsistencies

* No TODO/FIXME/XXX in `raft/src/main/java/org/apache/kafka/raft` (grep returns nothing).
* Inconsistencies: F1 (followerStateOrThrow after the common handler), F2 (epoch bump before
  validation and on pre-vote), F6. Also, `handleEndQuorumEpochResponse` calls `resignedStateOrThrow()`
  (KRC:1394). This is safe only because Resigned leaves solely by raising the epoch (KRC:3173) and the connections are reset.
* The comment for UnattachedState says "unattached ... doesn't know the leader or the leader's endpoint". It is
  the only state that can hold a leader without fetching from it, which feeds F1.
* QuorumState class javadoc (QS:44-47): "Unattached transitions to Follower: After discovering a leader with
  an equal or larger epoch". The code is missing this transition for an *already-known* leader on a successful Fetch (F1).

## Probe result (F1)

**CONFIRMED** by unit test `.specula-output/analysis/probes/SpeculaElectionProbeTest.java`. It was copied
into `raft/src/test/java/org/apache/kafka/raft/` to run and then removed from the kafka tree.
Setup: voters {100,101}, persisted leader 102 at epoch 3, which is not in the voter set, so the replica starts as
Unattached(3, leader=102). The replica sends a Fetch to a bootstrap server. The test answers it with
error NONE, currentLeader=(102,3) and leader endpoints. Command:
`./gradlew :raft:test --tests org.apache.kafka.raft.SpeculaElectionProbeTest -x checkstyleTest`.
Output:

```
SPECULA_PROBE state=LeaderAndEpoch[leaderId=OptionalInt[102], epoch=3] isFollower=false
thrown=java.lang.IllegalStateException: Expected to be Follower, but the current state is
UnattachedState(epoch=3, leaderId=OptionalInt[102], votedKey=Optional.empty, voters=[100, 101], ...)
  at KafkaRaftClient.handleFetchResponse(KafkaRaftClient.java:1744)
  at KafkaRaftClient.handleResponse(KafkaRaftClient.java:2747)
  at KafkaRaftClient.handleInboundMessage(KafkaRaftClient.java:2860)
  at KafkaRaftClient.poll(KafkaRaftClient.java:3709)
```
In production, KafkaRaftClientDriver:62-66 routes this to the fatal fault handler.
