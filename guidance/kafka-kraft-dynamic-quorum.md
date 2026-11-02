# Apache Kafka: KRaft Dynamic Quorum and Snapshots Modeling Guidance

Target source: `./kafka`, module `raft` (`org.apache.kafka.raft`). Treat the checked-out
implementation as ground truth.

The core Raft algorithm has been model-checked many times, so this run should NOT spend its
budget re-proving leader election and log matching under a static voter set. The interesting
code is what Kafka added on top: dynamic voter membership (KIP-853), pre-vote (KIP-996),
the prospective state, snapshots, and observers.

## Required

### Goal

- Find safety bugs in the Kafka-specific extensions to Raft: two leaders in one epoch after a
  voter set change, a committed record lost after an AddVoter / RemoveVoter, a voter that can
  never rejoin, a follower that fetches a snapshot and ends up with a log that disagrees with
  the leader, or a quorum that cannot elect a leader after a membership change.

### Scope

- In scope: `KafkaRaftClient` (request handlers for Vote, BeginQuorumEpoch, EndQuorumEpoch,
  Fetch, FetchSnapshot, AddRaftVoter, RemoveRaftVoter, UpdateRaftVoter), `QuorumState` and the
  per-state classes (`UnattachedState`, `ProspectiveState`, `CandidateState`, `LeaderState`,
  `FollowerState`, `ResignedState`), `VoterSet` and `VoterSetHistory`,
  `KRaftControlRecordStateMachine` (reading voter set changes back from the log and snapshots),
  the `AddVoterHandler` / `RemoveVoterHandler` / `UpdateVoterHandler` in `raft/internals`,
  snapshot generation and `FetchSnapshot` handling, and `FileQuorumStateStore` persistence of
  epoch and voted-for.
- Boundaries/exclusions: the controller state machine built on top of the log
  (`QuorumController`), the metadata record formats, network transport, and the batch
  accumulator's memory management. Read them only to establish what the raft layer must
  guarantee to them.

### Priority Questions

1. When a voter set change record (AddVoter or RemoveVoter) is appended but not yet
   committed, and the leader fails, can the old and new voter sets elect two different leaders
   in the same epoch, or can a leader commit using a majority of a voter set that was never
   committed?
   Expected behavior: at most one leader per epoch, and every commit uses a majority of a
   voter set that is itself committed or is the one being installed by the leader (joint
   consensus is not used; Kafka adds and removes one voter at a time, so verify that the
   single-change invariant is actually enforced).
2. When a voter is removed, then re-added with the same id but a new directory id (disk
   replaced), can stale votes, stale BeginQuorumEpoch, or stale Fetch responses from the old
   incarnation be accepted, or can the new incarnation be refused forever?
   Expected behavior: replica identity is (id, directory id); messages from an old directory
   id are ignored and the new one joins cleanly.
3. When a follower is far behind and fetches a snapshot while the leader concurrently appends,
   commits, and deletes old log segments, can the follower end up with a snapshot plus log
   suffix that skips or duplicates records, or with a voter set that does not match the
   leader's at the same offset?
   Expected behavior: snapshot end offset plus the fetched log suffix is exactly the leader's
   log prefix, and the voter set history reconstructed from both matches the leader's.
4. With pre-vote, can a prospective replica that is partitioned from the leader keep
   disrupting the quorum, or can a prospective that wins pre-votes from a stale voter set
   become candidate and win a real election that the current voter set would reject?
   Expected behavior: pre-vote never increases the epoch, and a candidate transition requires
   pre-votes from a majority of the voter set at the candidate's current log end.
5. When the quorum state store write (epoch, voted-for) fails or is reordered against the log
   append of a vote-granting Fetch, can a replica vote twice in one epoch after a restart?
   Expected behavior: voted-for is durable before the vote response is sent.

### Must-cover Interactions

- `handleAddVoterRequest` / `AddVoterHandler` → leader appends the new voter set →
  `KRaftControlRecordStateMachine` updates `VoterSetHistory` on every replica as the record is
  fetched → `LeaderState` recomputes the high watermark against the new set.
- `handleRemoveVoterRequest` ↔ the removed voter being the leader itself (it must resign)
  and ↔ an in-flight AddVoter for a different node (at most one pending change).
- `ProspectiveState` → `maybeTransitionToCandidate` → `CandidateState` → `LeaderState`:
  epoch and voter set snapshot at each transition, and what happens when the voter set changes
  mid-election.
- `handleFetchSnapshotRequest` / `handleFetchSnapshotResponse` ↔ leader log truncation of
  segments covered by the snapshot ↔ `VoterSetHistory.truncateNewEntries` (and whatever trims old entries after a snapshot).
- `UnattachedState` with a directory id that differs from the one in the leader's voter set
  (the KIP-853 "replace a disk" flow) ↔ `UpdateVoterHandler`.
- `FileQuorumStateStore` write ordering ↔ `handleVoteRequest` response.
- Consider: leader failure immediately after appending a voter change, network partition
  isolating exactly one voter, two AddVoter requests in flight, observer that is promoted while
  fetching a snapshot, and restart at every state.

### Assumptions

- Kafka installs each voter set change as a single record and the leader waits for it to
  commit before accepting the next change; verify this in `AddVoterHandler` and
  `RemoveVoterHandler` instead of assuming it.
- The log is durable once `fsync` returns; the quorum state file is a separate file.

## Optional

### Known Incidents and References

- KIP-853 (KRaft controller membership changes), KIP-996 (pre-vote), KIP-630 (snapshots).
- Archaeology: `git log` for "KafkaRaftClient", "VoterSet", "AddVoter", "RemoveVoter",
  "directory id", "ProspectiveState", "FetchSnapshot", "VoterSetHistory".
- The existing TLA+ specs for KRaft (search the repo and KIP-853 discussion for `.tla`
  files) are a reference for the base algorithm only; this run targets what they do not cover.

### Suggested Starting Points

- `raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java`
- `raft/src/main/java/org/apache/kafka/raft/QuorumState.java`
- `raft/src/main/java/org/apache/kafka/raft/LeaderState.java`,
  `ProspectiveState.java`, `CandidateState.java`, `UnattachedState.java`
- `raft/src/main/java/org/apache/kafka/raft/VoterSet.java`
- `raft/src/main/java/org/apache/kafka/raft/internals/AddVoterHandler.java`,
  `RemoveVoterHandler.java`, `UpdateVoterHandler.java`
- `raft/src/main/java/org/apache/kafka/raft/internals/KRaftControlRecordStateMachine.java`,
  `VoterSetHistory.java`
- `raft/src/main/java/org/apache/kafka/raft/FileQuorumStateStore.java`
- Tests: `raft/src/test/java/org/apache/kafka/raft/KafkaRaftClientReconfigTest.java`,
  `KafkaRaftClientSnapshotTest.java`, `KafkaRaftClientPreVoteTest.java`, and the
  `RaftEventSimulationTest` which already drives the client with simulated time and network.
  `RaftClientTestContext` is the harness to reuse.

### Additional Exploration

- Observers (brokers) fetching from a leader during a voter set change: can an observer be
  told a leader endpoint that is no longer a voter?
- `UpdateRaftVoter` changing a voter's endpoints or supported version while an election is
  in progress.
