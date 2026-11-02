# Deep analysis: KRaft snapshots + voter set history (KIP-630 / KIP-853)

Source: `/home/user/Specula/kafka` @ `8aa57006ff` (trunk). Paths below are relative to
`raft/src/main/java/org/apache/kafka/` unless noted.

Files read in full: `raft/internals/{KRaftControlRecordStateMachine,VoterSetHistory,TreeMapLogHistory,LogHistory,KafkaRaftLog,RecordsIterator}.java`,
`raft/{RaftLog,ValidOffsetAndEpoch,LogOffsetMetadata}.java`, `snapshot/{FileRawSnapshotWriter,FileRawSnapshotReader,RecordsSnapshotWriter}.java`
(+ skim of `Snapshots`, `RecordsSnapshotReader`), and in `KafkaRaftClient.java`: lines 341-520 (HW/listeners/initialize),
1601-1925 (Fetch completion, Fetch response, appendAsFollower/Leader), 1926-2223 (FetchSnapshot request/response),
2640-2725 (common response handling), 3436-3450 (fetch vs fetch-snapshot), 3820-3848 (createSnapshot), 4040-4090 (listener context).
Also `LeaderState.updateLocalState/updateVoterAndObserverStates` (750-900), `AddVoterHandler` 150-200,
`FollowerState.setFetchingSnapshot`, test fixture `MockLog.truncateToLatestSnapshot`, storage `LogLoader` 222.

Unit reproductions (real `KRaftControlRecordStateMachine` + `MockLog`, run as a plain `main` because the shared
Gradle test compile was broken by another session's probe file): `.specula-output/analysis/deep-snapshot-repro.java`.
All three scenarios below FAIL the expected assertion (i.e. the bug behaviour is confirmed at the unit level).

---

## Findings (ranked)

### F1 (HIGH) `KRaftControlRecordStateMachine.nextOffset` is never rewound on log truncation -> control records re-appended below the old LEO are silently skipped

Evidence
- `nextOffset` is assigned only at `KRaftControlRecordStateMachine.java:245` (`nextOffset = batch.lastOffset() + 1`),
  `:285` (after snapshot load) and `:290` (start). `truncateNewEntries` (`:117-129`) trims the two histories but leaves
  `nextOffset` untouched.
- `maybeLoadLog` only reads `while (log.endOffset().offset() > nextOffset)` (`:227`) and reads from `nextOffset`.
- `maybeLoadSnapshot` reloads only if `nextOffset == -1 || nextOffset < log.startOffset()` (`:253`).
- Caller: `KafkaRaftClient.java:1760-1768` (diverging epoch): `log.truncateToEndOffset(...)` then
  `partitionState.truncateNewEntries(truncationOffset)`; no reset, no reload. After every append `nextOffset == LEO`
  (`appendAsFollower` -> `updateState`, `:1860`), so any real truncation (T < LEO) leaves `nextOffset = oldLEO > T`.
- Consequence: every KRaftVersion / Voters control record that the new leader has in `[T, oldLEO)` is never applied
  (unless it shares a batch with offset `oldLEO`, `log.read` returns the batch containing `nextOffset`).
  `lastVoterSet()`, `lastVoterSetEntry()`, `lastKraftVersion()`, `voterSetAtOffset()` are stale.
  `checkOffsetIsValid` (`:211`) does not catch it because `nextOffset` is too large, not too small.
- Repro 1: V1@0, filler@1..9 (epoch 1); truncate to 1; new leader writes V2@1; `updateState()` -> `lastVoterSet()==V1`,
  `voterSetAtOffset(1)==V1`.
- Existing test `KRaftControlRecordStateMachineTest.testUpdateWithTruncation` (~line 373) truncates but never appends
  afterwards, so the gap is untested.

Impact (when the missed record is a VotersRecord)
- Follower/observer uses the wrong voter set for `QuorumState.isVoter`, election (`maybeSendVoteRequests` `:3229`),
  leader HW (`updateLocalState(..., partitionState.lastVoterSet())` `:361`), AddVoter/RemoveVoter "no uncommitted
  change" checks (`AddVoterHandler.java:175-176` reads `lastVoterSetEntry`), and auto-join (`:3390`).
- If this replica becomes leader it can accept an AddVoter/RemoveVoter computed from a stale base, writing a VotersRecord
  that differs from the committed set by more than one voter (breaks the single-change overlap that KIP-853 relies on).
- Local snapshots embed the wrong set: `createSnapshot` uses `voterSetAtOffset(lastContainedLogOffset)` (`:3843-3844`),
  so the error becomes persistent and survives restart.
- Self-heals only on restart before a bad snapshot is written (startup reloads from scratch: `initialize` `:509`).
Reachability: needs a follower/observer with an uncommitted tail `[T, L)` from an old leader, then a new leader that
writes a voter/kraft.version record at offset in `[T, L)`. Normal in 5-voter clusters and for observers (brokers).

### F2 (HIGH) Off-by-one in the snapshot-freeze trim: `truncateOldEntries(snapshotId.offset())` can drop the only voter set at or below the HW; a later truncation to the HW leaves the history empty

Evidence
- `KafkaRaftClient.java:3832-3835`: on freeze, `partitionState.truncateOldEntries(offsetAndEpoch.offset())`, where
  `offset()` is the exclusive end offset (last contained offset + 1).
- `TreeMapLogHistory.truncateOldEntries` (`TreeMapLogHistory.java:64-70`) keeps only the largest entry `<= startOffset`.
  If a VotersRecord sits exactly at `snapshotId.offset()` (the first offset not in the snapshot), the entry for the snapshot's
  own voter set (`<= offset-1`) is removed.
- Snapshot creation allows `snapshotId.offset() == HW` (`KafkaRaftLog.java:341-346` only rejects `> HW`), so offset
  `snapshotId.offset()` can be uncommitted. Truncation to exactly the HW is allowed (`KafkaRaftLog.java:236`
  rejects only `< HW`; client check `KafkaRaftClient.java:1752-1757` same).
- Then `truncateNewEntries(HW)` removes the entry at HW -> `voterSetHistory` empty -> `lastValue()` falls back to the
  static voter set (`VoterSetHistory.java:90-94`), which is `VoterSet.empty()` for a `controller.quorum.bootstrap.servers`
  cluster. Repro 2: lastVoterSet = `VoterSet(voters={})`, `voterSetAtOffset(4) = Optional.empty`.
- Nothing reloads the snapshot (`nextOffset` > start offset, see F1).
Impact: a standby controller (or broker) whose state machine snapshots at its HW while an AddVoter/RemoveVoter
record at HW is in flight, followed by leader failover that drops that record, believes there are no voters (cannot
vote or become candidate) and writes later snapshots with no VotersRecord (kraft.version 1 header, no voters),
persisting the loss across restart. kraft.version history has the same off-by-one (`:140-142`), but only matters if
a KRaftVersionRecord sits exactly at the snapshot end offset.
Fix direction: trim with `snapshotId.offset() - 1`, or never trim above the HW-committed prefix.

### F3 (MEDIUM-HIGH) Snapshot install with a lower end offset than the follower's old LEO does not reload the state machine (comment at `KafkaRaftClient.java:2197-2198` is false)

Evidence
- After download: `log.truncateToLatestSnapshot()` (`:2190`) -> `partitionState.updateState()` (`:2201`), with the
  comment "This will always reload the snapshot because the internal next offset is always less than the snapshot id".
- `KafkaRaftLog.truncateToLatestSnapshot` (`:251`) fully truncates when `snapshot.epoch > latestEpoch` even when
  `snapshot.offset <= LEO`. The leader sends a snapshot whenever `lastFetchedEpoch < earliestSnapshot.epoch`
  (`RaftLog.java:104-106`), regardless of the offset. So a follower with a long divergent tail of an old epoch
  (LEO=L) can get a snapshot (S<L, higher epoch).
- Then `nextOffset (=L) < startOffset (=S)` is false -> no reload (`KRaftControlRecordStateMachine.java:253`). The
  history keeps the follower's divergent entries (including uncommitted VotersRecords from the dead leader) and the snapshot's
  voter set is never loaded. Then the leader's records in `[S, L)` are skipped as in F1.
- Repro 3: divergent epoch-3 log of 10 offsets with bad voter set @7; install snapshot (5,4) carrying V1; after
  `updateState()` `lastVoterSet()` = the divergent set `{8,9}` @7, not V1.
Same root cause as F1. A fix that sets `nextOffset = min(nextOffset, endOffset)` on truncation and forces a reload when
the log is fully truncated (or always reloads after `truncateToLatestSnapshot`) covers F1 and F3.

### F4 (MEDIUM, liveness/crash) Downloaded snapshot whose epoch is lower than the follower's latest log epoch -> `IllegalStateException`

Evidence
- `KafkaRaftLog.truncateToLatestSnapshot` (`:251`) truncates only if `snap.epoch > latestEpoch || (== && snap.offset > LEO)`;
  `MockLogTest.testDoesntTruncateFully` asserts the older-epoch case is a no-op.
- `handleFetchSnapshotResponse` throws `IllegalStateException("Full log truncation expected but didn't happen")`
  (`KafkaRaftClient.java:2204-2213`) in that case.
- Leader side: `validateOffsetAndEpoch` returns SNAPSHOT(latest) when `offset == startOffset && epoch != earliestSnapshot.epoch`
  or `offset < startOffset` (`RaftLog.java:104-121`). It does not check that the follower's `lastFetchedEpoch` is <= the snapshot epoch.
- Possible trace (figure-8 style, 5 voters): follower F holds an uncommitted entry at p with epoch e_f. A later leader commits a
  lower-epoch e_s entry at p, the leader's latest snapshot ends in that e_s region at offset S with S >= F.LEO, and the leader's log
  start equals S (or is above F.LEO). F fetches `(F.LEO, e_f)`, gets SNAPSHOT(S, e_s<e_f), downloads it, and throws. On restart the
  frozen snapshot stays, `truncateToLatestSnapshot` again does nothing, and the next Fetch gets the same snapshot id, which is
  "already stored" (`:1806-1812`). The follower loops without making progress.
Model-check before filing: it needs the leader's latest snapshot to end before the e_g epoch-start LeaderChange
record, so it is narrow. The general case is the raft layer trusting that the snapshot epoch is >= the follower's epochs.

### F5 (LOW) Race: concurrent `createSnapshot` vs. follower snapshot reload can write a snapshot with no voter set / kraft.version 0

- `createSnapshot` calls `log.createNewSnapshot` and then, as a separate step, `kraftVersionAtOffset` and `voterSetAtOffset` on the caller thread (`:3829-3844`).
- `maybeLoadSnapshot` on the driver thread clears both histories (`:256-261`) before re-adding. Between them,
  `voterSetAtOffset` returns empty and `kraftVersionAtOffset` returns `KRAFT_VERSION_0` (`:206-207`). `nextOffset` still
  passes `checkOffsetIsValid`. The builder accepts (kraft 0, no voters) (`RecordsSnapshotWriter.java` build checks).
- The resulting snapshot has an id below the new log start, so it is normally never loaded and is cleaned later. Low impact.
  The class comment at `KRaftControlRecordStateMachine.java:65-70` claims "This synchronization is enough". It does not cover clear-then-reload.

### F6 (LOW) In-flight FetchSnapshot slice vs. delayed delete

`handleFetchSnapshotRequest` returns `snapshot.slice(...)` (`:2033`), a lazy `FileRecords` slice. `removeSnapshots`
renames to `.deleted` and closes the reader after `internalDeleteDelayMillis` (`KafkaRaftLog.java:646-660, 798-803`). A
send outliving the delay fails with an I/O error. The follower times out and retries. Correctness is OK.

---

## Q&A with checked compensating mechanisms

### Q1 FetchSnapshot under concurrent append/commit/snapshot/delete
- Snapshot deleted mid-transfer: leader `readSnapshot` returns empty once forgotten (`KafkaRaftLog.java:401-426`) ->
  `SNAPSHOT_NOT_FOUND` (`KafkaRaftClient.java:1982-1995`). Follower resets `fetchingSnapshot` (closing the writer deletes the
  `.part` file, `FileRawSnapshotWriter.close`) and goes back to Fetch (`:2127-2142`). SAFE.
- Position semantics: the follower sends `position = local writer size` (`:3440-3442`). The leader rejects `<0 || >= size` with
  POSITION_OUT_OF_RANGE (`:1999-2009`). The follower requires same id and `position == local size` or throws
  IllegalStateException (`:2157-2172`). There is a single in-flight request per destination, and responses from older epochs are dropped (`:2657-2659`).
  An immutable frozen file plus the id check means there is no mixing. SAFE.
- Snapshot above the leader HW: impossible. Snapshots are created only via `createNewSnapshot` with `offset <= HW`, valid epoch and
  batch alignment (`KafkaRaftLog.java:334-379`). The leader serves only its own snapshots. The follower uses
  `createNewSnapshotUnchecked` and trusts the leader (`:1793-1796`). After install the follower's HW = `log.highWatermark()` = snapshot offset
  (`:2203`, `MockLog.java:108`), which is <= the leader HW. SAFE.
- Gap/overlap: after `truncateFullyAndStartAt(S)`, start = LEO = S and `lastFetchedEpoch` = snapshot epoch
  (`KafkaRaftLog.java:180-193`). The next Fetch is `(S, e)`. Batch alignment guarantees the leader returns a batch starting at S. SAFE
  for the log. NOT safe for the control-record state machine when S <= the old LEO (F3).
- Fetch with `fetchOffset == 0` always gets the latest non-bootstrap snapshot (`:1625-1628`). The follower's empty log has latestEpoch 0, so
  it truncates. SAFE.

### Q2 VoterSetHistory semantics and callers
- `addAt(offset, v)`: strictly increasing offsets, `>= -1` (`TreeMapLogHistory.java:31-47`). It only logs a non-overlapping
  majority (`VoterSetHistory.java:54-72`). It does not enforce the overlap.
- `valueAtOrBefore` = floorEntry. `lastValue` = last entry, else the static set. `truncateNewEntries(e)` removes `>= e`.
  `truncateOldEntries(s)` keeps the largest entry `<= s` and everything above.
- Callers: `truncateNewEntries` from diverging-epoch truncation only (`KafkaRaftClient.java:1768`). It is consistent for both
  the voter and kraft.version histories, but `nextOffset` is not rewound (F1). `truncateOldEntries` is called only from the snapshot-freeze callback (`:3834`) with
  an off-by-one (F2). It is NOT called on `deleteBeforeSnapshot`/`maybeClean` (log-start moves do not trim in-memory history, which is harmless).
- Snapshot load: the histories are cleared, and all snapshot control records are added at `lastContainedLogOffset` (`:279-285`). Then
  `nextOffset = snapshotEnd`. The log replay starts exactly there. There is no double apply because the snapshot entries sit at
  `offset-1` and log entries at `>= offset`, and `addAt` would throw on a duplicate. This is correct when the load actually happens. It is skipped in the F3 case.
- Window with a truncated offset: yes. After F1/F3, `lastVoterSet()` can come from a divergent, removed offset (repro 3: entry @7
  while the log ends at 5).

### Q3 Which voter set each consumer uses
All of the following use `lastVoterSet()` (log end, uncommitted): election and vote sending (`:3229`), BeginQuorumEpoch (`:3125`),
leader HW (`:361` -> `LeaderState.updateVoterAndObserverStates`), `QuorumState.isVoter` (`QuorumState.java:343`), endpoint
lookup (`:986,1133,1195,1288,1371,1720,2074,2653`), auto-join/remove (`:3390`), UpdateVoter (`:3323`). AddVoter and RemoveVoter use
`lastVoterSetEntry` plus the guard `entry.offset < HW` (`AddVoterHandler.java:175-176`). Using the latest config is the intended
Raft single-server-change design. No consumer uses a voter-set-at-HW view. The only snapshot consumer uses `voterSetAtOffset(snapshotEnd-1)`.
No misuse found, assuming the history is correct. The risk comes from F1, F2 and F3 corrupting the history.
DescribeQuorum uses `LeaderState.voterStates()`, which is derived from `lastVoterSet` (consistent).

### Q4 Snapshot generation
- Offset `<= HW`, epoch validated against the log, batch aligned (`KafkaRaftLog.java:334-379`). SAFE.
- The header contains a SnapshotHeader, then a KRaftVersionRecord and VotersRecord only if kraft.version >= 1
  (`RecordsSnapshotWriter.java` Builder.build). The values are taken at `lastContainedLogOffset` (`KafkaRaftClient.java:3830,3843-3844`). SAFE.
- Voter change exactly at the boundary: a record at `S-1` is included (floor). A record at `S` is excluded, which is correct. But the
  freeze trim then drops the `S-1` value (F2).

### Q5 Crash recovery
- Order: `UnifiedLog.create(...logStartOffset=0...)` -> `recoverSnapshots` (deletes `.part`/`.deleted` files and snapshots below
  the log start, and throws if the log start > 0 with no snapshot >= the log start) -> `truncateToLatestSnapshot` (`KafkaRaftLog.java:712-747,751-796`) ->
  `KafkaRaftClient.initialize` -> `partitionState.updateState()` loads the latest snapshot and then the whole log (`:509`).
- The log start offset is not checkpointed for the metadata log (created with `logStartOffset=0`). After restart it equals the first segment's
  base offset (`LogLoader.java:222-223`), which can be < the earliest snapshot. This is benign: those records are still on disk, and
  `validateOffsetAndEpoch` and the listeners handle it.
- Bootstrap `0-0.checkpoint`: kept while log start = 0. It is loaded at offset -1 (`TreeMapLogHistory` allows -1). It is never served
  (`:1982`, `:1625`). The history cannot lose the bootstrap set while it is the latest entry (`truncateOldEntries` keeps the floor), except via F2.
- Crash after a snapshot download and before truncation: handled by `createLog -> truncateToLatestSnapshot` (`:744-746`), except
  in the lower-epoch case (F4).

### Q6 TODO/FIXME and error handling
- `KafkaRaftLog.java:289` "Temporary log message until we fix KAFKA-14825" (HW mismatch only logs a warning).
- `KRaftControlRecordStateMachine.java:65-70` synchronization comment is incomplete (F5).
- `KafkaRaftClient.java:2197-2198` comment is incorrect (F3).
- `VoterSetHistory.addAt` only logs a non-overlapping majority. There is no invariant check, so it cannot detect F1-induced 2-voter jumps.
- `handleFetchSnapshotResponse` turns several protocol anomalies into an `IllegalStateException` (`:2151-2172,2204-2213`). This
  crashes the raft thread instead of resetting the fetch.

## Modeling suggestions (for spec-generation)
- Model per-replica `cr_next` (the state machine's next offset) separately from the log end, plus `vhist` as a set of (offset, voterset) pairs.
  Actions: `TruncateDiverging` (trims vhist >= T, cr_next unchanged), `AppendFollower`/`LoadLog` (applies only offsets
  `>= cr_next`), `InstallSnapshot` (full truncation; reload only if `cr_next < snapOffset`), `FreezeSnapshot` (trim
  `<= snapOffset`, keep the floor), `CreateSnapshot` (embeds `vhist@snapOffset-1`).
- Invariants: `VhistMatchesLog`: for every replica r and offset o < LEO(r), `valueAtOrBefore(vhist[r], o)` equals the voter set
  computed from r's snapshot plus its log at o. `NonEmptyVoters`: in a dynamic cluster `lastVoterSet(r) != {}`.
  `SnapshotVotersCommitted`: a snapshot's voter set equals the leader's committed voter set at that offset.
- Small config: 3-5 voters plus 1 observer, MaxLogLen about 4, one AddVoter, one leader crash. F1 and F2 should show up within depth of about 12.
