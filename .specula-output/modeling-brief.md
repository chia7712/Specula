# Modeling Brief: Apache Kafka KRaft — dynamic quorum, pre-vote, snapshots

Guidance: `guidance/kafka-kraft-dynamic-quorum.md`. Source: `kafka/` trunk @ `8aa57006ff` (2026-11-02).
Unless stated, paths are relative to `raft/src/main/java/org/apache/kafka/raft/`. KRC = `KafkaRaftClient.java`,
QS = `QuorumState.java`, KCRSM = `internals/KRaftControlRecordStateMachine.java`.
Detailed evidence: `analysis/{archaeology,deep-voter-change,deep-election,deep-snapshot,deep-fetch-hw}.md`.

## 1. System Overview

- **System**: KRaft, Kafka's Raft implementation for the metadata log. Java, about 19.4k LOC in `raft/src/main`; KRC alone is 4.2k.
- **Category**: **A (Distributed / Message-Passing)**. It is crash-fault-tolerant consensus over RPC (Vote, BeginQuorumEpoch,
  EndQuorumEpoch, Fetch, FetchSnapshot, Add/Remove/UpdateRaftVoter), with durable log, snapshot and quorum-state files.
- **Protocol**: Raft with pull-based replication. Followers send Fetch; the leader answers with a diverging epoch or a snapshot id.
  Kafka-specific additions are the scope of this run:
  - Pre-vote through the `ProspectiveState` (KIP-996).
  - Dynamic voters, one change at a time, stored as `VotersRecord` control records (KIP-853).
  - Replica identity is (id, directoryId).
  - Snapshots, transferred by FetchSnapshot (KIP-630).
  - Observers.
- **Deviations from textbook Raft**:
  - The voter set is the *latest* `VotersRecord` in the local log, committed or not (`lastVoterSet()`). It is not read back from the log.
    It is kept in a side state machine, KCRSM, with its own cursor `nextOffset`, plus `VoterSetHistory` (a TreeMap from offset to set).
    Each change is applied when the record is appended, not when it commits.
  - Commit happens when the high watermark (HW) advances: the leader takes the majority over `voterStates` of the latest set, and the HW
    only advances past `epochStartOffset` (`LeaderState.java:680`).
  - The voted-for state is in a separate `quorum-state` file. It is written with fsync and an atomic rename.
- **Concurrency model**: a single raft IO thread (`KafkaRaftClient.poll`) handles all RPCs and state transitions, so each handler
  is atomic. `poll()` runs `pollCurrentState` (expire voter-change ops, then drain the accumulator) *before* handling one inbound message
  (KRC:3697-3709). The exceptions are `createSnapshot` and the snapshot freeze callback, which run on the controller thread.
  Any exception that escapes `poll()` is fatal (`KafkaRaftClientDriver.java:65`).

## 2. Scenarios

### Scenario 1: Voter-set history diverges from the log it summarises (HIGH)

**Mechanism**: KCRSM's in-memory voter/kraft.version history and its `nextOffset` cursor are updated by separate paths for append,
truncate, snapshot install and snapshot trim. Some of those paths leave the history out of step with the log contents, so the
replica uses a wrong `lastVoterSet()` for elections, HW computation, voter-change admission and the snapshots it writes.

**Evidence**:
- Code (each read and confirmed; unit-level repro in `analysis/deep-snapshot-repro.java`, run outside Gradle):
  - (a) Diverging-epoch truncation does not rewind `nextOffset`. `truncateNewEntries` (KCRSM:117-129) trims the histories, but
    `nextOffset` is assigned only at KCRSM:245/285/290. `maybeLoadLog` reads only while `log.endOffset() > nextOffset` (KCRSM:227).
    Result: a VotersRecord the new leader writes in `[truncationOffset, oldNextOffset)` is **never applied**.
    The call site is KRC:1760-1768.
  - (b) A snapshot install does not reload state when the snapshot epoch is higher but its offset is ≤ the old `nextOffset`.
    `truncateToLatestSnapshot` fully truncates (`internals/KafkaRaftLog.java:251`), but `maybeLoadSnapshot` only fires when
    `nextOffset < log.startOffset()` (KCRSM:253). The comment at KRC:2198-2199 ("will always reload") is false. The divergent
    log's voter entries survive.
  - (c) Off-by-one in the freeze trim. KRC:3834 passes the *exclusive* snapshot end S to `truncateOldEntries`, which keeps only the
    floor entry ≤ S (`internals/TreeMapLogHistory.java:64-70`). If a VotersRecord sits exactly at S, the snapshot's own set is
    dropped. A later truncation to S (= HW, allowed by KRC:1752 and `KafkaRaftLog.java:236`) empties the history, and
    `lastVoterSet()` falls back to the static set, which is empty for bootstrap-server clusters.
- Historical:
  - 15e938ef43 (KAFKA-17973) turned the `addAt` overlap check into a log-only warning, so a history that skips changes is no longer detected.
  - 058d8d530b (KAFKA-14618), 7ac40f4ded (KAFKA-21234) and 4a8a0637e0 (KAFKA-18723) fixed other offset-bookkeeping bugs in this area.
  - Existing test `testUpdateWithTruncation` never appends after truncating.

**Affected code paths**: KCRSM `updateState/maybeLoadLog/maybeLoadSnapshot/truncateNewEntries/truncateOldEntries`;
KRC `handleFetchResponse` (diverging epoch), `handleFetchSnapshotResponse`, `createSnapshot`; `VoterSetHistory`, `TreeMapLogHistory`.

**Suggested modeling approach**:
- Variables: per replica, the `log` (sequence of entries with an epoch; some entries are VotersRecords carrying a set), `logStart`,
  `snapshot` (end offset, epoch, voters), `vsHist` (function offset → voter set), `vsNext` (the `nextOffset` cursor), `hw`.
- Actions:
  - Model `AppendAsFollower` and `UpdateState` as code-faithful: apply entries in `[vsNext, len(log))`, then set `vsNext`.
  - `TruncateDiverging(i, off)` truncates `log` and `vsHist` but **not** `vsNext`.
  - `InstallSnapshot` truncates fully, then reloads only if `vsNext < logStart`.
  - `CreateSnapshot(i, S ≤ hw)` and `FreezeTrim` (keep the floor entry ≤ S).
  - `UpdateState` may run separately from the append, but always on the same thread.
- Granularity: keep `vsNext` explicit. Do *not* derive the voter set from the log in the spec, or the bug disappears.

**Priority**: High.
**Rationale**: these are the only candidate *safety* violations found. Every voter-set consumer reads this history, and the one-change rule
(Scenario 2) assumes it is correct. All three defects are confirmed by reading the code; their system-level consequence has not been demonstrated.

### Scenario 2: Voter-change lifecycle versus leader role, timers and late responses (HIGH)

**Mechanism**: the Add/Remove/Update handler state (`ChangeVoterHandlerState`) and `LeaderState.voterStates` change on *append*. Several
leader duties are tied to the handler being pending, or to the voter being present in `voterStates`, rather than to the committed voter set.

**Evidence**:
- Code, confirmed by probes:
  - **Late BeginQuorumEpoch ack crashes the leader.** Sequence: the leader sends BeginQuorumEpoch to V, then RemoveVoter(V) is appended,
    which drops V from `voterStates` (`LeaderState.java:881-909`). V's NONE reply then reaches `addAcknowledgementFrom` (KRC:1217-1221),
    which throws `IllegalArgumentException` (`LeaderState.java:829-840`) and kills the process.
  - **Removed leader never resigns.** The leader resigns *only* in `RemoveVoterHandler.highWatermarkUpdated`, while the op is still pending
    (`internals/RemoveVoterHandler.java:212-241`). If the op expires first (`internals/ChangeVoterHandlerState.java:251-257`) and the record
    commits later, the leader stays leader of the same epoch while not a voter. Retrying returns VOTER_NOT_FOUND.
    With 2 voters → 1, `voterStates.size()==1` turns check-quorum off (`LeaderState.java:223-227`).
- Code, read only:
  - `isReplicaCaughtUp` (`LeaderState.java:415-426`) never compares offsets.
  - `ackWhenCommitted=false` AddVoter and kraft.version-1 UpdateVoter reply before the record leaves the accumulator
    (`internals/AddVoterHandler.java:393-397`). The resign check at KRC:3187 runs before the drain at KRC:3202.
  - Check-quorum `fetchedVoters` is not cleaned when the voter set changes (`LeaderState.java:223-273`).
- Verified safe: "at most one uncommitted change" is enforced on Add (`AddVoterHandler.java:176`), Remove (`RemoveVoterHandler.java:162`)
  and Update (`UpdateVoterHandler.java:368`). Each requires the latest VotersRecord offset to be < HW, the HW is known only once it passes
  `epochStartOffset`, and `validateMutualExclusivity` throws if two ops are pending. The start-of-epoch VotersRecord (`LeaderState.java:320-387`)
  and the kraft.version upgrade are unguarded, but their record blocks later changes until it commits.
- Historical: ee71156295 (KAFKA-17332, resign on non-voter), 68b9770506 (KAFKA-17604, crash when the active controller is removed),
  f880ad6ccf (KAFKA-16530, HW with a removed leader), 05f61b9ff5 (KAFKA-20865). The handlers have little fix history;
  `UpdateVoterHandler` was rewritten in Aug 2026.

**Affected code paths**: `AddVoterHandler`, `RemoveVoterHandler`, `UpdateVoterHandler`, `ChangeVoterHandlerState.maybeExpirePendingOperation`,
`LeaderState.updateLocalState/updateVoterAndObserverStates/maybeUpdateHighWatermark/timeUntilCheckQuorumExpires`, KRC `pollLeader`.

**Suggested modeling approach**:
- Variables: `pendingOp[leader] ∈ {None, Add(r,off), Remove(r,off), Update(r,off)}`, `voterStates[leader]` (derived from `lastVoterSet`
  at the last `updateLocalState`), `resignRequested`, `inflight` messages including BeginQuorumEpoch replies.
- Actions: split each RPC into `Admit` (checks, append) and `HwAdvanced` (complete or resign). Add `ExpireOp` (a nondeterministic timeout).
  `PollLeader` = ExpireOp, then CheckResign, then Drain, in that order. Add `CheckQuorumExpire`. Late replies are delivered from a message bag.
- Granularity: leader steps are atomic per `poll()`, but expiry, append and HW-advance are separate steps.

**Priority**: High.
**Rationale**: two crash or liveness defects are confirmed. Removing an unresponsive voter is the common operator action, so these paths
are exercised in practice. They also cover the guidance's must-cover interactions on Add, Remove and HW.

### Scenario 3: Elections and pre-vote across different local voter sets (MEDIUM)

**Mechanism**: each replica campaigns and counts votes against its *own* `lastVoterSet()`, and requesters are never checked for membership.
Several request paths change the epoch or state before validating the request.

**Evidence**:
- KRC:896-898 bumps the receiver to `Unattached(replicaEpoch)` for a **pre-vote** with a higher epoch, deposing a leader. This happens
  before the voter-key check (KRC:901-918). Pre-vote stops a prospective from raising *its own* epoch (QS:621-635), but not this.
- KRC:960-967: one UNSUPPORTED_VERSION reply moves a Prospective to Candidate (a durable epoch bump) without a pre-vote majority.
- Requesters of Vote, BeginQuorumEpoch and EndQuorumEpoch are never checked against `isVoter`. A removed voter that has not yet applied
  its removal can still campaign (`deep-election.md` F7, Q3).
- **Probe-confirmed crash**: `Unattached(E, leader L)` with no endpoints is reached through QS:204-231 or KRC:3294-3305. A successful Fetch
  reply from L then gets past `maybeHandleCommonResponse` (KRC:2668-2671 requires `!hasLeader`) and reaches `followerStateOrThrow`
  (KRC:1744), which throws IllegalStateException.
- Open PR apache/kafka#23168 (KAFKA-20895): `FollowerState.canGrantVote` (`FollowerState.java:239-241`) refuses pre-votes after
  EndQuorumEpoch.
- Historical: c28f46459a and 97db06689b (KAFKA-18345, livelock), 05f9803d72 (KAFKA-10527), 16186eabcd (KAFKA-16927), e979fce94e (KAFKA-17030).
- Verified safe: votes are persisted before the response is built (QS:729-733; FileQuorumStateStore:166-195 uses fsync and an atomic move).
  A pre-vote never writes votedKey (KRC:931). `hasConsistentLeader` blocks a same-epoch leader switch (KRC:2607-2619). Stale responses are
  dropped by the correlation-id check plus `resetConnections()` on every transition (KRC:2859).

**Suggested modeling approach**:
- Variables: `state[i] ∈ {Unattached, Prospective, Candidate, Leader, Follower, Resigned}`, `leaderId[i]` (Unattached may carry a leader
  without endpoints, modelled as a flag), `currentEpoch`, `votedFor` (durable), `election[i]` (keyed by the voters of `lastVoterSet` at state entry).
- Actions: `RequestPreVote`, `HandlePreVote` (with the epoch bump at the top, per the code), `RequestVote`, `HandleVote`, `BecomeLeader`,
  `HandleBeginQuorumEpoch`, `HandleEndQuorumEpoch`, `UnsupportedVersionReply` (optional, mixed-version), `Timeout`.
- Model the per-destination correlation id, or require the response epoch and request type to match. Otherwise spurious traces appear.

**Priority**: Medium.
**Rationale**: the guidance asks about this directly (Q4). The behaviour differs from etcd-style pre-vote. Two-leaders safety looks sound by
reading, but it rests on the voter-set correctness of Scenario 1, which is where the composition matters.

### Scenario 4: Replica identity and quorum-state persistence (LOW-MEDIUM)

**Mechanism**: (id, directoryId) is compared inconsistently, and an empty directory id acts as a wildcard. The quorum-state file format
depends on the *uncommitted* kraft.version.

**Evidence**:
- `VoterSet.isVoter`, `matchesKey` and `updateVoter` treat an empty stored directory id as matching any id. `removeVoter` requires an exact
  match (`VoterSet.java:223-224`). `addVoter`, `ensureValidVoter` and EpochElection compare by id only.
- A v0 quorum-state drops `votedDirectoryId` (`ElectionState.java:123-147`). The version comes from `lastKraftVersion()`, which is
  uncommitted (QS:731).
- `isValidVoterKey` returns true for an empty directory id or `voterId<0` (KRC:2786-2795).
- A quorum-state epoch below the log epoch silently drops votedKey (QS:155-172).
- Verified safe on kraft.version 1: an old-incarnation Fetch lands in `observerStates` and cannot move the HW (`LeaderState.java:846-854, 982-991`).
  The disk-replacement auto-remove (KRC:3392) gets INVALID_VOTER_KEY.
- Historical: 25819cecdb (KAFKA-17426), 9be27e715a.

**Suggested modeling approach**: give each replica `dirId[i]` and a `Restart(i, newDir)` action for disk replacement (a fresh log and
quorum-state). Model replica keys as pairs. Keep kraft.version fixed at 1 by default; offer kraft.version 0 → 1 upgrade as an optional config.

**Priority**: Low-Medium.
**Rationale**: the guidance asks about this (Q2) and the remove/re-add flow must be covered. Analysis found no defect on kraft.version 1.

### Scenario 5: Snapshot transfer versus leader log changes (MEDIUM)

**Mechanism**: the follower's decision to install a snapshot, and the leader's choice to send one, depend on epoch comparisons that do not
always line up with the follower's own log.

**Evidence**:
- If the snapshot epoch is *lower* than the follower's latest epoch, `truncateToLatestSnapshot` is a no-op, so `handleFetchSnapshotResponse`
  throws IllegalStateException (KRC:2204-2213). After a restart, the "already stored" path (KRC:1806) makes no progress. Reachability is
  unproven; it needs a figure-8 history.
- Leader side: snapshot send when `lastFetchedEpoch < earliestSnapshotEpoch` (`RaftLog.java:104-106`).
- Verified safe:
  - A mid-transfer delete gives SNAPSHOT_NOT_FOUND (KRC:1982), and the follower restarts from Fetch.
  - Position and snapshot-id checks (KRC:2157-2172, 1999) reject mismatched chunks.
  - Snapshots exist only at offsets ≤ HW and on batch boundaries (`KafkaRaftLog.java:334-379`).
  - After install, log start = log end = snapshot end.
- Historical: b166ac43cb (KAFKA-14238), 8c8b5366a6 (KAFKA-14240), 3a9777a667 (KAFKA-14619), e0727063f7 (KAFKA-15312), d0d01b960d.

**Suggested modeling approach**: model the snapshot transfer as one atomic `InstallSnapshot` (chunking is checked and safe). Model the
follower's `lastFetchedEpoch` and the leader's earliest snapshot, and `DeleteBeforeSnapshot` (advances the leader's log start). Pairs with
Scenario 1(b).

**Priority**: Medium (low on its own, high in combination with Scenario 1).

## 3. Modeling Recommendations

### 3.1 Model
| What | Why | How |
|---|---|---|
| KCRSM cursor `vsNext` separate from the log, with code-faithful update, truncate, reload and trim | Scenario 1 (a)-(c) | per-replica vars; actions as in §2 S1 |
| Voter set read from the latest *appended* VotersRecord, used for elections, HW and admission | the system's own design; Scenario 1 and 2 | `LastVoterSet(i)` computed from `vsHist`, not from `log` |
| Pending voter-change op, expiry, and resign only on HW-advance while pending | Scenario 2 | `pendingOp`, `ExpireOp`, `HwAdvanced` |
| Leader's `voterStates` refreshed only at `updateLocalState`, and late replies from removed voters | Scenario 2 | snapshot of the voter set in the leader, message bag |
| Pre-vote with epoch bump on the receiver; UNSUPPORTED_VERSION shortcut as an optional flag | Scenario 3 | `HandlePreVote` |
| Unattached with a leader but no endpoints, plus Fetch to a bootstrap server | Scenario 3 (crash path) | `leaderKnownNoEndpoint` flag |
| Crash/restart: durable log (prefix up to the fsync point), snapshot, quorum-state; in-memory KCRSM rebuilt from the snapshot and log | guidance Q5, Scenario 1 | `Crash(i)` resets volatile state; `Restart` replays |
| Disk replacement (same id, new dirId) | guidance Q2, Scenario 4 | `ReplaceDisk(i)` |
| Snapshot create (≤ HW), freeze trim, install, leader log-start advance | Scenario 1 and 5 | atomic actions |

### 3.2 Do Not Model
| What | Why |
|---|---|
| Static-voter Raft leader election and log matching on their own | the guidance excludes them; they have been checked many times. Keep only the minimum needed to drive the extensions |
| `BatchAccumulator` memory, the pooled-buffer reuse (apache/kafka#23311), `BufferSupplier` | memory management; test or code review only |
| Snapshot chunking, positions, FetchSnapshot partition checks | verified safe; atomic install is enough |
| QuorumController, metadata records, listener SSL, endpoint probing (API_VERSIONS) | out of scope; endpoints only matter as a boolean "endpoint known" |
| Metrics, DescribeQuorum, error-message strings | no protocol effect |
| Busy-loop or timer-backoff liveness (KAFKA-17751, -19605, -18345) | timing-dependent; TLC abstracts timers to nondeterministic timeouts |
| kraft.version 0 → 1 upgrade (by default) | the main runs use kraft.version 1; the upgrade is an optional config |

## 4. Proposed Extensions

| Extension | Variables | Purpose | Scenario |
|---|---|---|---|
| ControlRecordStateMachine | `vsHist`, `vsNext`, `staticVoters` | voter set as the code computes it, with its own cursor | S1 |
| Snapshots | `snapshot[i]` (end, epoch, voters), `logStart[i]` | snapshot create, trim, install, log-start advance | S1, S5 |
| VoterChangeOps | `pendingOp`, `resignRequested`, `leaderVoterStates` | Add, Remove and Update lifecycle, timeouts | S2 |
| PreVote | `state` incl. Prospective, `election[i]` | prospective, pre-vote epoch-bump semantics | S3 |
| ReplicaIdentity | `dirId[i]`, ReplicaKey pairs in VotersRecords | disk replacement, stale incarnation | S4 |
| CrashRecovery | durable vs volatile split; `flushed[i]` | restart in every state | S1-S4 |
| Fatal-exception sink | `crashedByBug[i]` | turn IllegalState/IllegalArgument throws in handlers into an observable state | S2, S3 |

## 5. Proposed Invariants

| Invariant | Type | Description | Targets |
|---|---|---|---|
| ElectionSafety | Safety | at most one leader per epoch | S3, guidance Q1/Q4 |
| CommittedPrefixDurable (LeaderCompleteness) | Safety | an entry at or below any leader's HW is in every later leader's log | S1, guidance Q1 |
| VoterSetMatchesLog | Safety | `LastVoterSet(i)` = the voter set from replaying `snapshot[i]` plus `log[i]` | S1 (a)-(c), guidance Q3 |
| VoterHistoryNonEmpty | Safety | in a bootstrap-servers cluster, `LastVoterSet(i) ≠ {}` once initialized | S1 (c) |
| SingleUncommittedChange | Safety | at most one VotersRecord above the HW in any leader's log, and consecutive sets differ by at most one voter | S2, guidance Q1 |
| CommitQuorumIsValid | Safety | each HW advance is backed by a majority of a set that is committed or is the one being installed | S1, S2 |
| SnapshotVotersCommitted | Safety | `snapshot[i].voters` equals the leader's voter set at that offset | S1, S5 |
| HWMonotonic | Safety | leader and follower HW never decrease | S2 |
| NoFatalException | Safety | no handler reaches a throwing branch (`crashedByBug` stays FALSE) | S2, S3 |
| VoteOncePerEpoch | Safety | across crashes, a replica grants at most one standard vote per epoch | S4, guidance Q5 |
| RemovedLeaderStepsDown | Liveness (weak fairness) | a leader not in the committed set eventually resigns | S2 |
| ReAddedReplicaJoins | Liveness | a replica re-added with a new dirId eventually becomes a voter that is fetched from | S4, guidance Q2 |

## 6. Findings Pending Verification

### 6.1 Model-Checkable
| ID | Description | Expected invariant violation | Scenario |
|---|---|---|---|
| MC-1 | Can a follower skip a VotersRecord written after a diverging truncation (`vsNext` not rewound), and then, as leader, compute HW or admit a second change against the stale set, so that a commit is made without a majority of the real set or a committed entry is lost? | VoterSetMatchesLog → CommitQuorumIsValid / CommittedPrefixDurable / SingleUncommittedChange | S1(a) |
| MC-2 | Can an AddVoter or start-of-epoch VotersRecord at offset S, plus a snapshot at S = HW, plus failover with truncation to HW, empty a replica's voter history? Does that block elections, or produce snapshots with no voters that survive a restart? | VoterHistoryNonEmpty, SnapshotVotersCommitted | S1(c) |
| MC-3 | After a snapshot install with a higher epoch and offset ≤ old `vsNext`, can stale divergent voter entries (from S1(b)) let the replica vote or lead with a set the leader never had? | VoterSetMatchesLog, ElectionSafety | S1(b), S5 |
| MC-4 | Is the follower state "snapshot epoch < latest local epoch while the leader sends a snapshot" reachable? If it is, the follower crashes or is permanently stuck | NoFatalException / liveness | S5 |
| MC-5 | If RemoveVoter(leader) expires before commit, can the cluster reach a state with no electable leader, for example a 2→1 voter change with check-quorum disabled and the remaining voter crashed? | RemovedLeaderStepsDown | S2 |
| MC-6 | Does the pre-vote epoch bump on receivers (KRC:896) let a partitioned or stale prospective repeatedly depose a healthy leader? Can the UNSUPPORTED_VERSION shortcut produce a candidate whose epoch exceeds that of any pre-vote majority? | bounded-disruption property (leader deposed only after a quorum timeout); ElectionSafety as a guard | S3 |
| MC-7 | Remove V, then re-add V with a new dirId, with stale Vote, BeginQuorumEpoch and Fetch traffic from the old incarnation in flight: can the old incarnation influence HW, votes, or the new incarnation's admission? | ElectionSafety, CommitQuorumIsValid, ReAddedReplicaJoins | S4 |
| MC-8 | Crash and restart at every state, including between quorum-state write and log append, and with a v0 quorum-state file: can a replica grant two votes in an epoch? | VoteOncePerEpoch, ElectionSafety | S4, guidance Q5 |

Suggested small configs:
- MC-1/2/3: 3 voters plus 1 observer, ≤ 3 epochs, one Add or Remove, one leader crash, one snapshot, about 12-15 steps.
- MC-5: 2 voters.
- MC-7: 3 voters with 2 dirIds for one of them.

### 6.2 Test-Verifiable
| ID | Description | Suggested test approach |
|---|---|---|
| T-1 | **Confirmed.** A late BeginQuorumEpoch NONE reply from a removed voter crashes the leader (KRC:1217-1221 → `LeaderState.java:834-839`) | RaftClientTestContext probe: `analysis/SpeculaProbeTest.java`, `analysis/probes/SpeculaVoterChangeProbeTest.java` |
| T-2 | **Confirmed.** Unattached(E, L, no endpoints) plus a successful Fetch reply from L gives IllegalStateException at KRC:1744 | `analysis/SpeculaProbeTest.java`, `analysis/probes/SpeculaElectionProbeTest.java` |
| T-3 | **Confirmed.** A removed leader stays leader after the RemoveVoter op times out (`RemoveVoterHandler.java:212-241`) | `analysis/probes/SpeculaVoterChangeProbeTest.java` |
| T-4 | **Unit-repro.** `nextOffset` is not rewound; a snapshot install skips the reload; trim off-by-one (KCRSM:117-129, 253; KRC:3834) | `analysis/deep-snapshot-repro.java` → port to `KRaftControlRecordStateMachineTest`, then end to end with `KafkaRaftClientSnapshotTest` |
| T-5 | A FetchSnapshot or UpdateRaftVoter request carrying the leader's own id throws in `updateFetchedVoters` (`LeaderState.java:270-273`; KRC:2031-2038; `UpdateVoterHandler.java:469,493`) | RaftClientTestContext with a crafted request |
| T-6 | AddVoter with `ackWhenCommitted=false` or UpdateVoter on kraft.version 1 acks, then the leader resigns before the drain, and the record is lost | RaftClientTestContext: ack, resign, then poll |
| T-7 | A snapshot reload clears history while `createSnapshot` on the controller thread reads it, so a snapshot is written with an empty voter set and kraft.version 0 (KCRSM:256-261 vs KRC:3829-3844) | concurrency stress test |

### 6.3 Code-Review-Only
| ID | Description | Suggested action |
|---|---|---|
| R-1 | `isReplicaCaughtUp` ignores offsets (`LeaderState.java:415-426`), which contradicts the javadoc (`AddVoterHandler.java:63-64`) | compare against the LEO or HW |
| R-2 | `voterStates.size()==1` turns check-quorum off even when the leader is not that voter; `fetchedVoters` is not pruned on a voter change (`LeaderState.java:223-273`) | check `isVoter(local)`, prune on a set change |
| R-3 | Vote and BeginQuorumEpoch change state before validating the voter key (KRC:896 vs 902, 1138 vs 1146) | reorder |
| R-4 | `VoterSetHistory.addAt` only logs a non-overlapping change (since KAFKA-17973) | make it an assertion in tests |
| R-5 | The KRC:2198 comment "will always reload" is incorrect (see S1(b)) | fix with S1 |
| R-6 | `handleAddVoterRequest` ignores the request TimeoutMs | document or honour it |
| R-7 | Open PRs: apache/kafka#23168 (pre-vote after EndQuorumEpoch), apache/kafka#23118 (pending fetches not failed on Unattached/Resigned), apache/kafka#23311 (pooled-buffer timestamp) | track upstream |
| R-8 | QS:155-172 silently drops votedKey when the state epoch is below the log epoch; this only happens after quorum-state loss | log at error level, refuse to start |

## 7. Reference Pointers

- Analysis files: `analysis/archaeology.md` (83 in-scope fixes out of 359 commits; 9 mechanism groups; open-PR scan),
  `analysis/deep-voter-change.md`, `analysis/deep-election.md`, `analysis/deep-snapshot.md`, `analysis/deep-fetch-hw.md`,
  `analysis-report.md` (the audit trail).
- Key code:
  - KCRSM:110-300; `internals/VoterSetHistory.java`, `internals/TreeMapLogHistory.java`.
  - KRC:835-1062 (Vote), 1075-1400 (Begin/EndQuorumEpoch), 1693-1870 (Fetch response, append), 1960-2215 (FetchSnapshot),
    2607-2921 (common response, validation), 3119-3556 (poll*), 3820-3846 (createSnapshot).
  - `LeaderState.java:128-136, 223-273, 320-387, 415-426, 650-710, 753-768, 829-909`.
  - QS:130-260 (initialize), 458-733 (transitions).
  - `FileQuorumStateStore.java:141-195`; `internals/KafkaRaftLog.java:236-260, 334-379, 646-660, 744-796`.
  - Handlers: `internals/{Add,Remove,Update}VoterHandler.java`, `internals/ChangeVoterHandlerState.java`.
- Tests to reuse for the harness: `KafkaRaftClientReconfigTest`, `KafkaRaftClientSnapshotTest`, `KafkaRaftClientPreVoteTest`,
  `RaftEventSimulationTest`, `RaftClientTestContext`.
- Historical context (reference only, not targets): KAFKA-17973, -14618, -21234, -18723, -17332, -17604, -16530, -16927, -17030,
  -18345, -14238, -14619, -17426.
- External KIPs: 595, 630, 853, 996. JIRA, the Apache wiki and the GitHub API were unreachable from this session, so the analysis relies on git history only.
