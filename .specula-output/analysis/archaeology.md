# Bug archaeology: KRaft `raft` module (KIP-595/630/853/996)

Source: `/home/user/Specula/kafka`, trunk @ 8aa57006 (2026-11-02). Only the local git history was used, plus `git ls-remote`/`git fetch` of open PR heads. JIRA and the wiki were not reachable.

## Coverage stats

| Item | Count |
|---|---|
| Commits scanned (`git log -- raft/src/main core/src/main/scala/kafka/raft`) | 359 |
| server-common commits screened (subject grep kraft/voter/quorum/feature) | ~50 |
| Other 2024+ commits with kraft/voter/quorum in the subject (outside raft/src/main) screened | ~80 |
| Bug fixes identified, in scope (raft/src/main, KafkaMetadataLog/KafkaRaftLog, RaftManager) | 83 |
| Peripheral bug fixes (formatter/ApiVersions/config, outside raft/src/main) | 6 |
| Deeply analyzed (full diff of main files read) | 16 commits + 4 open PRs |
| Excluded as feature/refactor/move/cleanup/docs/test-only | ~270 |
| Open PR heads examined (#23271..#24070 not merged into trunk) | 500 (fetched with `--filter=blob:none`); 9 touch raft/src/main, 3 are relevant fixes |
| TODO/FIXME/XXX/HACK in raft/src/main | 0 |

How fixes were classified: every subject was read. For each plausible fix, the full commit body and the `--stat` of the main files were read. Where the root cause was unclear or the fix concerns KIP-853 or pre-vote, the main diff was read too.

Severity scale:
- **Critical**: safety (committed data lost, uncommitted data exposed, divergent logs, two leaders)
- **High**: availability or stuck (no leader, crash, a voter change that never completes)
- **Medium**: wrong but recoverable behaviour, performance, or a busy loop
- **Low**: logging, metrics, error codes, format strings

## 1. Bug-fix table (in scope)

Component key: ELEC=election/prevote/check-quorum, VCHG=voter change Add/Remove/Update, VSH=VoterSetHistory/control records/kraft.version, SNAP=snapshot/FetchSnapshot/log start, FETCH=fetch/HW/replication, QSTATE=quorum-state persistence, DIRID=directory-id identity, OBS=observer, LSN=listener notification (RaftClient.Listener), DUR=flush/durability, NET=network/endpoints/config, THR=threading/memory, OTHER.

| Commit | KAFKA | Date | Summary | Root cause / mechanism | Comp | Sev |
|---|---|---|---|---|---|---|
| 7ac40f4ded | 21234 | 2026-10-23 | FileRecords#readInto honors slice boundaries | A committed read larger than 8 MiB made RecordsIterator read past the FileRecords slice end (the HWM), up to the end of the file. Uncommitted records reached the listener and could end up in a snapshot | FETCH/SNAP | Critical |
| f58e363954 | 20025 | 2026-09-08 | Reload controller-quorum SSL keystore | The raft channel's SslChannelBuilder was never registered as Reconfigurable, so it kept a stale certificate | NET | High |
| 05f61b9ff5 | 20865 | 2026-08-21 | Do not unregister KRaft voter set members | The controller could unregister a voter, racing with feature upgrades. Fixed with a check against the uncommitted voter set (UVS) | VCHG | High |
| d0d01b960d | MINOR | 2026-08-20 | Fix the FetchSnapshot topic/partition check | `size()!=1 && get(0)...` used `&&` instead of `||`. Malformed requests were not rejected (possible IndexOutOfBounds) | SNAP | Low |
| 7565048a5c | 20661 | 2026-08-19 | Validate the voter default endpoint on UpdateVoter | UpdateVoter persisted unreachable endpoints. After failover no one could reach the voters, so the quorum was permanently unavailable. Fix: an API_VERSIONS probe before writing | VCHG/NET | High |
| 05cbe688f7 | 20726 | 2026-07-17 | Don't reuse BufferSupplier in the kraft state machine | Cached buffers were never evicted, so memory grew without bound | THR | Medium |
| 72e380c149 | 20380 | 2026-05-05 | Backward-compatible advertised.listeners | The controller's advertised listener was derived incorrectly when it was not set | NET | Medium |
| a899cfc0f8 | 18679 | 2026-01-30 | raft metrics exposed doubles | Wrong metric value type | OTHER | Low |
| 9c9f1446a1 | 18061 | 2025-09-09 | AddRaftVoter error message "NONE" | `Errors.message()` was used when the message was null | VCHG | Low |
| 833e25f015 | 19605 | 2025-08-15 | Busy loop in observers | The observer poll backoff used `updateVoterSetPeriod` (auto-join) timer even when auto-join did not apply. The timer had expired, so the backoff was 0 | OBS | Medium |
| 7339d65b27 | 19354 | 2025-07-28 | Observer fetches from the best node | The observer stayed stuck fetching from bootstrap servers after it rediscovered the leader | OBS | High |
| 08aa469af7 | 19392 | 2025-06-10 | metadata.log.segment.ms not applied | A property line was dropped by mistake in #19371 | SNAP/OTHER | Medium |
| bcda92b5b9 | 19080 | 2025-05-25 | segment.ms/bytes constraints for the metadata log | Internal small-segment config handling | OTHER | Low |
| c28f46459a | 18345 | 2025-05-12 | Prevent livelocked elections | Exponential backoff saturated at electionBackoffMax with no jitter, so replicas started elections in lockstep | ELEC | High |
| 97db06689b | 18345 | 2025-05-23 | Wait the full election timeout on loss | Follow-up to the livelock fix: replaces the backoff with the remaining randomized election timeout | ELEC | Medium |
| a5325e029e | 17431 | 2025-03-18 | Invalid static config allowed when the dynamic config is valid | Startup read only the static config | OTHER | Low |
| a2653281c2 | 18837 | 2025-03-12 | Quorum timeouts must be ≥ 0 | Missing config validators | OTHER | Low |
| 4a8a0637e0 | 18723 | 2025-02-25 | Handle invalid records during replication | Race between the network thread (reading segments) and the KRaft driver thread (truncate/append). Corrupt or inconsistent batches could be sent. Fix: catch CRE, and accept only batches whose partition leader epoch is ≤ the fetch epoch | FETCH/THR | Critical |
| 15e938ef43 | 17973 | 2025-01-13 | Relax the voter set change restriction | VoterSetHistory.addAt threw when a new set lacked an overlapping majority (more than one change per record, e.g. after a snapshot plus log). Now it logs; safety relies only on the leader's "no uncommitted VotersRecord" check | VSH | High (crash) / opens a safety reliance |
| 3a9777a667 | 14619 | 2024-12-09 | Snapshot id must be at a batch boundary | createSnapshot accepted end offsets in the middle of a batch, giving misaligned snapshots | SNAP | High |
| e979fce94e | 17030 | 2024-12-11 | Unattached voters fetch from bootstrap servers | A replica that thinks it is a voter but is not in the leader's set never gets BeginQuorumEpoch, so it never discovers the leader | ELEC/VCHG | High |
| a8f84cab95 | 18001 | 2024-11-16 | UpdateRaftVoter in KafkaNetworkChannel | The request type was missing from the channel, so promoting an observer failed | VCHG/NET | High |
| 34ebccc79f | 17751 | 2024-10-10 | pollFollowerAsVoter busy loop | updateVoterTimer was not reset when no update was needed, causing high CPU | VCHG | Medium |
| 16186eabcd | 16927 | 2024-10-04 | Handle expanding leader endpoints | Follower→Follower in the same epoch was forbidden. A restarted follower with stale or partial leader endpoints could not learn new endpoints from BeginQuorumEpoch | ELEC/NET | High |
| 68b9770506 | 17608/17604/16963 | 2024-09-26 | Controller crash when the active controller is removed | (a) QuorumController advanced its offset on control batches without a snapshot, so a revert crashed. (b) LeaderState.updateVoterAndObserverStates did not copy listeners for added voters (DescribeQuorum) | VCHG | High |
| 25819cecdb | 17426 | 2024-08-28 | Check the node directory id | KafkaRaftClient accepted ZERO_UUID as the local directory id | DIRID | High |
| 7b6d4fbc57 | 17333 | 2024-08-20 | ResignedState should not notify leader change | A leader restarted as Resigned notified the listener of local leadership before it reached the LEO, so writes failed or were based on stale state | LSN/QSTATE | High |
| ee71156295 | 17332 | 2024-08-19 | Observer controllers flush; resign allowed on non-voter | (a) The leader assumes voters flushed before Fetch, but an observer being added did not flush. (b) resign() threw for a leader that had removed itself | DUR/VCHG | Critical (a) / Medium (b) |
| 20c3e7324b | 16842 | 2024-08-16 | Config validation; unknown voters | Observers could not start with only bootstrap servers configured. Added VoterSet.empty | VSH/OBS | Medium |
| b4d5f163a9 | 17067 | 2024-08-10 | Fix the transition to CandidateState | Non-voters (observers) could become Candidate. VotedState merged into Unattached | ELEC | High |
| 9be27e715a | MINOR | 2024-06-27 | Incompatible config change; BeginQuorumEpoch VoterId default | The VoterId default was 0 instead of -1 | NET/DIRID | Medium |
| d646a09dd0 | 16531 | 2024-06-21 | Check-quorum when the leader is not in the voter set | Majority was computed as `size/2` assuming the leader is a voter | ELEC/VCHG | High |
| f880ad6ccf | 16530 | 2024-06-05 | HW calculation must not assume the leader is a voter | The HW used `voterStates.get(localId)` and voterStates was never refreshed from lastVoterSet. Added updateVoterAndObserverStates; the HW can compute lower after a change | FETCH/VCHG | Critical |
| 524ad1e14b | 16452 | 2024-05-27 | Don't throw OOORE converting an offset to metadata | Missing checkpoint plus newly elected leader | FETCH | Medium |
| 90892ae99f | 16516 | 2024-05-20 | Controller node provider | Used a static voter map instead of the RaftManager | NET | Medium |
| 440f5f6c09 | MINOR | 2024-05-14 | Validate at least one control record | Empty control batch | VSH | Low |
| 474f8c1ad6 | 16286 | 2024-02-23 | Notify the listener of the latest leader | A listener registered after the follower transition was never notified | LSN | High |
| 70bd4ce8a7 | 16144 | 2024-01-23 | Skip check-quorum for a single voter | With one voter there are no fetches, so the leader resigned forever | ELEC | High |
| 37416e1aeb | 15489 | 2023-12-01 | Leader resigns without majority fetches | Check-quorum (KIP-595) was missing, so a partitioned leader stayed leader | ELEC | High |
| 7b669e8806 | 14273 | 2023-09-07 | Close the file before the atomic move (quorum-state) | Windows file handle | QSTATE | Medium |
| 2e3ff21c2e | 15412 | 2023-08-30 | Unknown quorum-state version must fail | Silently accepted | QSTATE | Medium |
| 3f4816dd3e | 15345 | 2023-08-17 | Leader notifies only after the listener reaches the epoch start | On an empty log the leader notified immediately | LSN | High |
| dafe51b658 | 15100 | 2023-08-09 | Data race with the expiration service | The expiration thread called tryCompleteFetchRequest, which mutates driver-thread state. Also diverging-epoch/snapshot responses were not sent immediately | THR/FETCH | High |
| e0727063f7 | 15312 | 2023-08-08 | Force the channel before the atomic move | Zero-length snapshot or quorum-state files on ext4 | QSTATE/SNAP | Critical |
| b7a6a8fd5f | 15076 | 2023-06-12 | Prefer the latest snapshot | A listener at offset 0 replayed the whole log | SNAP/LSN | Medium |
| 3a246b1aba | 15078 | 2023-06-28 | Leader replies with a snapshot for offset 0 | Efficiency | SNAP | Low |
| 44e613c4cd | 13884 | 2023-03-15 | Only voters flush on Fetch response | Performance; later found too weak (see 17332) | DUR | Medium |
| 35142d43e6 | 14664 | 2023-02-15 | Idle ratio metric | Averaging bug | OTHER | Low |
| 598ba5f2fd | 14644 | 2023-01-25 | Crash after a raft IO thread failure | Errors were swallowed, leaving a zombie | THR | High |
| 058d8d530b | 14618 | 2023-01-13 | Off-by-one in the snapshot id | MetadataProvenance used the inclusive last offset as the exclusive end offset | SNAP | Critical |
| 2fc1875d0b | 14557 | 2023-01-10 | Lock the metadata log dir | No file lock for controller-only nodes or a separate metadata dir | OTHER | High |
| 8c8b5366a6 | 14240 | 2022-09-19 | Validate snapshot state on startup | Log start > 0 without a covering snapshot | SNAP | High |
| b166ac43cb | 14238 | 2022-09-17 | Do not delete segments past the latest snapshot | Retention could delete past logStartOffset, breaking "LSO>0 ⇒ snapshot ≥ LSO" | SNAP | Critical |
| c5954175a4 | 14222 | 2022-09-13 | Memory pool always allocates | The snapshot writer with linger MAX exhausted the pool, causing a hang | THR/SNAP | High |
| a724166fcc | MINOR | 2022-08-19 | Unexpected request error in kraft shutdown | BROKER_NOT_AVAILABLE mapping | OTHER | Low |
| e5b865d6bf | 13940 | 2022-08-17 | DescribeQuorum to a non-leader | Returned INVALID_REQUEST (fatal) instead of NOT_LEADER | OTHER | Low |
| 50e5b32a6d | 13959 | 2022-08-13 | Unfence a broker with a busy metadata log | HW propagated only on the next fetch, so the broker's HW lagged forever | FETCH | High |
| 5a2f28ce30 | MINOR | 2022-08-01 | Synchronize the snapshots TreeMap | Concurrent access from the cleaner and raft threads | THR/SNAP | High |
| 5e4c8f704c | 13943 | 2022-07-06 | LocalLogManager/RaftClient contract (resign/scheduleAtomicAppend) | Contract mismatch | OTHER | Low |
| d6f6edd2b1 | 13168 | 2021-08-05 | Observers should not have a replica id | Brokers fetched with a node id | OBS | Medium |
| fd36e5a8b6 | 12851 | 2021-07-28 | Raft partition simulation | The test asserted HW incorrectly; LeaderState log | FETCH | Low |
| 56ef910358 | 13104 | 2021-07-20 | Controller tells the raft client when it resigns | Split view between the controller and raft | LSN | High |
| 8e48212343 | 13098 | 2021-07-16 | NoSuchFileException during snapshot recovery | Concurrent delete | SNAP | Medium |
| 0f00f3677f | 13078 | 2021-07-13 | Closing RawSnapshotWriter too early | FollowerState closed the in-progress fetch-snapshot writer | SNAP | High |
| 35e65b7f06 | MINOR | 2021-07-08 | Snapshot delete bug (Scala 2.12) | `sliding` semantics differ between Scala versions | SNAP | Medium |
| 4ddf1a5f94 | 12837 | 2021-06-18 | Empty batch reader (control records) | NoSuchElementException | LSN | Medium |
| d2c06c9c3c | 12619 | 2021-04-08 | Expose the HW only after the LeaderChange commits | Off-by-one: `hw >= epochStart` instead of `> epochStart` (Raft "commit one entry of the current term") | FETCH | Critical |
| f5f66b982d | 12181 | 2021-03-22 | Loosen fetch offset validation | A non-monotonic fetch offset threw on the leader after a replica lost its disk | FETCH | High |
| a3930aad6c | MINOR | 2021-03-19 | Assume unclean shutdown for the metadata log | Recovery was skipped | DUR | Medium |
| c6a0f76073 | 12460 | 2021-03-12 | No truncation below the HW | Committed data could be truncated | FETCH | Critical |
| 0685b9dcd5 | MINOR | 2021-03-04 | Max batch size propagates to the log config | A batch could exceed the segment/fetch size | OTHER | Medium |
| e29f7a36db | 12331 | 2021-02-18 | LEO as base offset of the LeaderChange batch | Base offset 0 was used | FETCH | High |
| ea459d1457 | 12153 | 2021-01-08 | Producer state before start/end offsets after truncation | Invalid first-unstable offset | FETCH | Medium |
| 125d5ea0fb | 10677 | 2020-12-11 | Complete fetches in purgatory after resigning | A new election was delayed by stuck fetches | ELEC | Medium |
| a8b668b37c | 10826 | 2020-12-09 | IO thread respects the linger timeout | The thread blocked forever after the first append | THR | High |
| 05f9803d72 | 10527 | 2020-10-06 | Voters don't reinitialize as leader in the same epoch | Unflushed data plus the same epoch broke (epoch, offset) uniqueness. Restart as Resigned | QSTATE/ELEC | Critical |
| a72f0c1eac | 10533 | 2020-10-13 | Flush the log after appends | No fsync, so the monotonic HW invariant failed in simulation | DUR | Critical |
| 1a09bac030 | MINOR | 2021-02-20 | Redundant log close | Double close | OTHER | Low |
| 742b327025 | 14145 | 2025-06-17 | Faster HWM replication | The follower HW lagged by one fetch-wait (follow-up of 13959) | FETCH | Medium |
| 8d12c1175c | 15152 | 2023-08-25 | Format specifiers | Logging | OTHER | Low |
| 9ac102596b | 16971 | 2024-06-17 | Format string in parseBootstrapServer | Logging | OTHER | Low |
| 78372b6883 | 17013 | 2024-06-24 | ConnectionState toString | Logging | OTHER | Low |
| 6c90f3335e | 13947 | 2022-06-10 | %d formatting | Logging | OTHER | Low |

Peripheral fixes (outside raft/src/main, relevant to the model's environment):

| Commit | KAFKA | Summary |
|---|---|---|
| 1bcaa19c46 | 19489 | Formatter check. `--standalone`/`--initial-controllers` combined with static `controller.quorum.voters` could form **two disjoint quorums** (split brain via configuration) |
| 857b1e92cc | 19719 | `--no-initial-controllers` must not assume kraft.version=1 |
| d04efca493 | 18979 | kraft.version FeatureLevelRecord wrongly written to the metadata log; ApiVersions reported it wrong |
| 343bc995f4 | 18920 | Controllers must set kraft.version in ApiVersionsResponse (used by AddVoter/UpdateVoter validation) |
| 075401e2e1 | 19964 | ApiVersions returns unknown features when the cluster metadata HWM is unknown |
| 10873e4210 | 18281 | Controller-address routability validation on non-advertised listeners |

## 2. Hotspots

Files touched by the 83 fixes (main files only, plus tests for scale):

| File | Fix commits |
|---|---|
| KafkaRaftClient.java | 39 |
| KafkaMetadataLog.scala (now `internals/KafkaRaftLog.java`) | 16 |
| LeaderState.java | 12 |
| QuorumState.java | 9 |
| RaftManager.scala | 8 |
| FollowerState.java | 6 |
| ReplicatedLog/RaftLog | 6 |
| RaftUtil, KRaftControlRecordStateMachine, BatchAccumulator | 4 each |
| VoterSet, VoterSetHistory | 3 each |

Recurring functions:
- `KafkaRaftClient.pollFollowerAsVoter`/`pollFollowerAsObserver`/`maybeSendFetchToBestNode` (timers and backoff): 34ebccc79f, 833e25f015, 7339d65b27, e979fce94e.
- `LeaderState.maybeUpdateHighWatermark`/`updateLocalState`/`updateVoterAndObserverStates`/check-quorum: d2c06c9c3c, f880ad6ccf, d646a09dd0, 70bd4ce8a7, 68b9770506.
- `KafkaRaftClient.maybeFireLeaderChange` / listener notification: 3f4816dd3e, 474f8c1ad6, 7b6d4fbc57.
- `QuorumState.transitionTo*` guards: 05f9803d72, b4d5f163a9, 16186eabcd.
- `KafkaMetadataLog` snapshot/retention/truncation: b166ac43cb, 8c8b5366a6, 0e5591beda, c6a0f76073, 8e48212343, 35e65b7f06, 5a2f28ce30.

The voter-change handlers (`AddVoterHandler`, `RemoveVoterHandler`, `UpdateVoterHandler`, `ChangeVoterHandlerState`) have had almost no explicit fixes. They are young code, and UpdateVoterHandler was rewritten in Aug 2026 (7565048a5c). That points to coverage gaps rather than maturity.

## 3. Mechanism groups and unfixed sites

Every file:line below was re-read in trunk @ 8aa57006.

### M1. Logic computed against the wrong or stale voter set (leader not a voter, removed voter, membership changes)

Fixes:
- f880ad6ccf: HW assumed the leader is a voter; voterStates not refreshed.
- d646a09dd0: check-quorum majority.
- 70bd4ce8a7: single voter.
- ee71156295: resign by a non-voter threw.
- 68b9770506: added-voter listeners.
- e979fce94e: replica thinks it is a voter, leader disagrees.
- b4d5f163a9: non-voter became Candidate.
- 05f61b9ff5: unregistering a voter.

Could the same mechanism fail at another, unfixed site?

**(a) Removed voter's late BeginQuorumEpoch response crashes the leader.**
- `KafkaRaftClient.java:1218-1220` calls `state.addAcknowledgementFrom(remoteNodeId)`.
- `LeaderState.java:829-840` (`ensureValidVoter`) throws `IllegalArgumentException("Unexpected acknowledgement from non-voter")` when voterStates no longer holds that id.
- voterStates is shrunk by `updateVoterAndObserverStates` (`LeaderState.java:881-907`) as soon as the leader appends a removal VotersRecord. Connections are not reset at that point, so a response already in flight is still "expected".
- Per KAFKA-14644 an exception in the raft IO thread is fatal.
- Plausible: **High**. A probe test `SpeculaVoterChangeProbeTest#beginQuorumEpochResponseFromRemovedVoterThrows` already exists, untracked in the working tree.

**(b) Leader that removed itself never resigns if the RemoveVoter operation expired first.**
- The resign happens only inside `RemoveVoterHandler.highWatermarkUpdated` (`RemoveVoterHandler.java:212-241`), and only while `removeVoterHandlerState` is present.
- `ChangeVoterHandlerState.maybeExpirePendingOperation` (`ChangeVoterHandlerState.java:251-257`) clears that state after `requestTimeoutMs`, using the timer created at `RemoveVoterHandler.java:192-195`.
- If the VotersRecord commits after the timeout, the leader keeps leading although it is not a voter. It even keeps passing check-quorum, because the majority excludes it.
- No other code path checks "leader ∉ committed voter set".
- **Medium–High**. A probe test `SpeculaVoterChangeProbeTest#leaderRemovesItselfCommitAfterTimeoutNeverResigns` exists.

**(c) Single-voter shortcut assumes the leader is that voter.**
- `LeaderState.java:223-227`: `if (voterStates.size() == 1) return Long.MAX_VALUE`.
- If the leader removed itself leaving `{B}`, check-quorum is disabled even though the only voter is remote. Combined with (b), the leader can stay leader forever with B dead.
- **Medium**.

**(d) Open PR #23118 (unmerged): leader-exit paths don't complete the purgatories.**
- In `KafkaRaftClient.java:746-750`, `transitionToUnattached` completes neither `fetchPurgatory` nor `appendPurgatory`.
- In `KafkaRaftClient.java:752-760`, `transitionToResigned` completes only `fetchPurgatory`.
- Leader→Unattached happens on a higher-epoch Vote, or when the old leader is not in the new set (the `transitionToUnattached(epoch+1)` path). Appends then wait until timeout. Same class as KAFKA-10677.
- **Medium**.

### M2. Voter set / control-record history out of sync with the log (offset bookkeeping)

Fixes:
- 15e938ef43: overlap invariant relaxed. Safety now rests only on the leader's "no uncommitted VotersRecord" check in `AddVoterHandler.java:174-176` and `RemoveVoterHandler.java:160-162`.
- 440f5f6c09.
- 058d8d530b (off-by-one between snapshot end offset and last contained offset).
- 7ac40f4ded (reading past the slice).

Same mechanism at unfixed sites:

**(e) `truncateNewEntries` does not rewind `nextOffset`.**
- `KRaftControlRecordStateMachine.truncateNewEntries` (`KRaftControlRecordStateMachine.java:117-128`) truncates the histories but leaves `nextOffset` unchanged.
- `maybeLoadLog` (`KRaftControlRecordStateMachine.java:226-248`; `nextOffset` advances only at line 245) resumes reading at the stale `nextOffset`.
- Caller: diverging-epoch truncation at `KafkaRaftClient.java:1760-1768`.
- Result: a VotersRecord or KRaftVersionRecord written by the new leader in `[truncationOffset, oldNextOffset)` is silently skipped. The follower keeps a wrong voter set (wrong elections, check-quorum and auto-join decisions).
- **Critical candidate**. A repro exists: `SpeculaNextOffsetReproTest#voterRecordBelowStaleNextOffsetIsSkippedAfterTruncation` (untracked). I did not run it.

**(f) Snapshot trim plus truncation at the HW can empty the voter history.**
- The snapshot callback calls `partitionState.truncateOldEntries(snapshotId.offset())` (`KafkaRaftClient.java:3830-3834`) with the exclusive end offset.
- `TreeMapLogHistory.truncateOldEntries` (`TreeMapLogHistory.java:65-71`) keeps only the last entry at or below that offset, inclusive. An uncommitted entry at exactly `snapshotId.offset()` (allowed, since snapshot end ≤ HW) therefore evicts the older committed entry.
- A later truncation to that offset (allowed when it equals the HW, see the check at `KafkaRaftClient.java:1752-1757`) removes the remaining entry. `lastVoterSet()` falls back to the static or empty set.
- Same off-by-one family as KAFKA-14618.
- **High/Critical candidate**. A repro exists: `SpeculaNextOffsetReproTest#snapshotFreezeTrimThenTruncateAtHwLosesVoterSet`.

### M3. Directory-id identity handling

Fixes:
- 25819cecdb: ZERO_UUID local id.
- 9be27e715a: VoterId default.
- Identity semantics live in `VoterSet.VoterNode.isVoter` (`VoterSet.java:391-400`) and `ReplicaState.matchesKey` (`LeaderState.java:982-991`). Both treat a missing directory id in the stored entry as a wildcard.

Add, Remove and UpdateVoter all reject requests without a directory id (`KafkaRaftClient.java:2281-2287` (Add), `2438-2445` (Remove), `2523-2533` (Update)). No inconsistency was found across the handlers.

Residual risks:
- `LeaderState.voterStates` is keyed by id while `observerStates` is keyed by ReplicaKey (`LeaderState.java:858-866`, `881-907`). A same-id/different-dir replica is tracked as an observer, which is fine.
- Auto-join removal uses the first id match (`KafkaRaftClient.java:3392-3402`).
- kraft.version 0 → 1 with `updateVoterIgnoringDirectoryId` (UpdateVoterHandler) is the main path where a wildcard entry gets a concrete directory id. **Low/unknown**: model the 0→1 upgrade with a replaced disk.

### M4. Election liveness (backoff, pre-vote, resign handoff)

Fixes: c28f46459a, 97db06689b, 37416e1aeb, 70bd4ce8a7, 125d5ea0fb, 05f9803d72 (safety: the same-epoch re-lead), 16186eabcd, 7b6d4fbc57.

Unfixed: **open PR #23168 (KAFKA-20895)**.
- `FollowerState.canGrantVote` (`FollowerState.java:239-241`) rejects PreVote whenever `hasFetchedFromLeader`, even after the leader sent EndQuorumEpoch (graceful resign).
- With 4 to 9 voters the first preferred successor cannot win, so every graceful controller shutdown wastes an election round.
- **Medium (availability)**.

### M5. Uncommitted data exposure / commit rule (HW)

Fixes:
- d2c06c9c3c: off-by-one on the current-epoch commit.
- c6a0f76073: truncation below the HW.
- 7ac40f4ded: read past the HWM slice.
- 4a8a0637e0: network-thread read racing truncation.
- f880ad6ccf: HW with membership change, which can compute lower. The decrease is suppressed and logged in `maybeUpdateHighWatermark` (`LeaderState.java:660-720`, the decrease branch ends at line 701).
- a72f0c1eac, ee71156295: flush before acknowledging.

Remaining site:
- `updateVoterAndObserverStates` runs only from `updateLocalState` (`LeaderState.java:753-768`). Between appending a VotersRecord and the next local update, `updateReplicaState` → `maybeUpdateHighWatermark` uses the previous voterStates.
- This is safe for single-change transitions, since old and new majorities overlap. KAFKA-17973 removed the history-level overlap check, so this now relies on a single change per commit. Worth a model invariant.

### M6. Snapshot / log-start-offset consistency

Fixes: b166ac43cb, 8c8b5366a6, 0e5591beda, 058d8d530b, 3a9777a667, 0f00f3677f, 8e48212343, 35e65b7f06, 5a2f28ce30, e0727063f7, d0d01b960d.

Remaining site: M2(f) above. FetchSnapshot validation is now `RaftUtil.hasValidTopicPartition(FetchSnapshot*Data)` (`RaftUtil.java:718-724`), which checks counts only. The topic name is resolved later by the partition lookup.

### M7. Poll-loop timers, busy loops, stuck observers

Fixes: 34ebccc79f, 833e25f015, 7339d65b27, a8b668b37c, e979fce94e.

No new unfixed site was confirmed. `pollFollowerAsObserver` (`KafkaRaftClient.java:3382-3412`) now returns `sendResult.timeToWaitMs()` and resets the period only when a request was sent.

### M8. Thread-safety / memory

Fixes: dafe51b658, 4a8a0637e0, 5a2f28ce30, c5954175a4, 05cbe688f7.

Unfixed: **open PR #23311 (KAFKA-20913)**.
- `BatchAccumulator.CompletedBatch.appendTimestamp()` (`BatchAccumulator.java:617-621`) reads `data.firstBatch().maxTimestamp()` from a pooled buffer that may already have been reused after `release()`.
- The leader's commit callback can report a wrong append time, which feeds snapshot timestamps.
- **Low/Medium**.

### M9. Endpoint / listener plumbing (KIP-853 endpoints)

Fixes: 7565048a5c, 16186eabcd, 68b9770506 (17604), a8f84cab95, 9be27e715a, 72e380c149, f58e363954, 90892ae99f.

Note on the new API_VERSIONS probe: `UpdateVoterHandler.handleApiVersionsResponse` matches the response by node id only (`expectingApiResponse(source.id())`). It relies on the RequestManager correlation-id filter (`KafkaRaftClient.java:2859`, `RequestManager.java:226-233`), which is keyed by `node.idString()`, to drop stale responses. That looks safe, but an old-endpoint/new-endpoint mix-up for the same id is worth modeling. **Low**.

## 4. Open / unmerged PRs touching raft/src/main (PR heads #23271..#24070)

| PR | Subject | Assessment |
|---|---|---|
| #23168 | KAFKA-20895 Grant pre-vote once the leader has resigned | **Real liveness bug** (M4). Fix adds `FollowerState.hasLeaderResigned`, set on EndQuorumEpoch |
| #23118 | (untitled "fix") complete leader purgatories on every leader-exit path | **Real** (M1d). transitionToUnattached and transitionToResigned leave appends or fetches pending |
| #23311 | KAFKA-20913 Snapshot CompletedBatch appendTimestamp before buffer reuse | **Real**, minor (M8) |
| #23778 | KAFKA-19022 log cluster ids on INCONSISTENT_CLUSTER_ID | Diagnostics only |
| #23779 | KAFKA-16020 nanoTime overflow in waitForFuture (Time.java) | Not raft-protocol |
| #23754 | Scatter-gather in BatchBuilder | Feature |
| #23160 | "make append durable" (benchmark scaffolding, +2 lines KafkaRaftClient) | Experimental |
| #23095 | canary interval config | Not raft |
| #23587 | Restore interrupt status in TimingWheelExpirationService | Hygiene |

About 50 heads in that range are release-branch backports with more than 25 commits ahead of trunk. They were skipped and none is raft-specific.

## 5. TODO/FIXME/XXX

None in `raft/src/main` (grep for TODO|FIXME|XXX|HACK returned nothing).

## 6. Suggested invariants and scenarios for the TLA+ model (derived from the above)

1. **Voter-set history agreement.** For every replica, `lastVoterSet` equals the voter set obtained by replaying the control records of its current log plus snapshot. Exercise diverging truncation followed by a new VotersRecord at an offset below the old next offset (M2e), and snapshot plus truncation at the HW (M2f).
2. **Removed leader eventually resigns.** If the leader is not in the committed voter set, it eventually leaves Leader. Exercise RemoveVoter expiry before commit (M1b), and the single remaining voter case (M1c).
3. **No crash on stale responses.** BeginQuorumEpoch or Vote responses from replicas no longer in the set must be ignored (M1a).
4. **HW monotonic and committed under membership change.** Include the HW lag between appending a VotersRecord and the `updateLocalState` refresh (M5).
5. **Graceful-resign successor wins** with pre-vote (M4 / PR #23168).
6. **Listener never sees offsets ≥ HW** (KAFKA-21234 class).
