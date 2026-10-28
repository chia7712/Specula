# Bug Archaeology: KIP-848 consumer group rebalance, commits 2022-11 .. 2024-12

Source: `/home/user/Specula/.specula-output/archaeology/commits-pre2025.txt` (246 commits).
Kafka HEAD = `1224f3d4b` (2026-10-27). Evidence is git only (commit messages and diffs).
Path abbreviations used below:
- `RT` = `coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/`
- `GC` = `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/`
- `GMM` = `GC/GroupMetadataManager.java`, `CG` = `GC/modern/consumer/ConsumerGroup.java`,
  `CAB` = `GC/modern/consumer/CurrentAssignmentBuilder.java`, `CR` = `RT/CoordinatorRuntime.java`

## 1. Coverage

| Item | Count |
|---|---|
| Commits in list | 246 |
| Messages read (`git show -s`) | ~75 (all candidates with fix/semantic wording plus every design commit named in the task) |
| Diffs read in detail (main code) | 12 (cf90382fb, ee550c4b7, 05fd36a3b, 18f17ed4d, 31f79055c, b888fa1ec, eb897c6ad, 7556ce366 (CompleteLoad only), plus the HEAD code of every touched component) |
| In-scope bug fixes / semantic corrections | 33 (table in section 2) |
| Pickaxe (`-S`) searches | 20 terms (previousMemberEpoch, UNRELEASED_PARTITIONS, deferredEventQueue, lastCommittedOffset, cancelConsumerGroupRebalanceTimeout, isSubset, waitingOnUnreleasedPartition, revertLastWrittenOffset, `tasks.remove(key, this)`, staticMemberId, isReconciledTo, ownsRevokedPartitions, enqueueFirst, updateLastCommittedOffset, readAtLeastOneRecord, `coordinators.get(tp);`, `prevValue < epoch`, `Cannot remove the epoch`, ...) |

Excluded, by approximate category (some commits fall in more than one):
- Initial feature scaffolding (KAFKA-14462/14367/14500/14499/1450x/1451x, record schemas, interfaces): ~67. Design contracts taken from the key ones, listed in section 3.
- MINOR refactors, renames, javadoc, checkstyle, spotless, logging: ~45.
- Metrics and histograms (KAFKA-14519, 15870, 16379, 16307, 16664, 15981, 16106): ~20.
- Config, deprecation and protocol removal (KIP-724/896, 16735/16736, 15853, 16909, 17081, 14510/14511 group configs): ~15.
- Server-side assignor internals and performance (range, uniform, rack-aware: 14514, 15484, 14515, 16864, 16944, 17710, 16694, 17578): ~20. Only the NPE fix 16930 is kept.
- Offsets, transactions and APIs outside rebalance (OffsetFetch/Commit/Delete, TxnOffsetCommit 14505 series, ListGroups, DescribeGroups, 17267, 18060, 17507, 15647, 15849): ~35.
- Share groups and KIP-932 refactors (17047, 17288, 17329): 3. Client side (17116 / KIP-1082 wire change): 1.

## 2. In-scope bug fixes

Severity: C = safety (overlap or lost state), H = unavailability or stuck group, M = stuck or incorrect single member or op, L = performance or noise.

| sha | ticket | summary | root cause | component | sev | mechanism tag |
|---|---|---|---|---|---|---|
| cf90382fb | KAFKA-16147 | Partition assigned to two members at once | `maybeUpdatePartitionEpoch` diffed old/new sets and lost the epoch when a partition pending revocation was re-assigned to the same member; it was replaced by remove-all/add-all and assertions | CG partition-epoch map | C | OWNERSHIP-BOOKKEEPING |
| e164d4d42 | KAFKA-16249 | Reconciliation state machine reworked | A new target recomputed the member's assignment and ignored reported owned partitions, so a revocation ack was missed and the member got stuck. The rebalance timer was also reset on every new target. Fix: revocation blocks, and the state is explicit in the record | CAB, GMM timers | H | RECONCILIATION-SM / TIMER-RESET |
| c66d66dc6 | KAFKA-16367 | Full heartbeat response must follow a full request | "Full" was inferred only from `ownedTopicPartitions` | GMM heartbeat | M | REQUEST-SHAPE |
| 8864cba0e | MINOR | Full-request condition with regex | Same inference was not updated for `SubscribedTopicRegex` | GMM:2745 | M | REQUEST-SHAPE |
| 275b995bf | KAFKA-18095 | Join without subscription | Validation rejected an empty subscription or regex, so a client-side regex consumer could not join | GMM validation | M | REQUEST-SHAPE |
| ee550c4b7 | KAFKA-16973 | Fix caught-up condition | A no-record write completed at once when nothing was pending in the deferred queue. Right after load (uncommitted tail) this released a response that depended on uncommitted state. Fix: compare lastCommitted with lastWritten | CR append/waitForPendingWrites | H | COMMIT-GATING |
| 18f17ed4d | KAFKA-18200 | Empty batches in runtime | Empty writes were attached to an empty open batch, and flushing an empty batch returned offset 0 | CR | M | COMMIT-GATING |
| 05fd36a3b | KAFKA-18174 | Second completion of a write event must be a no-op | Timeout and commit (or fail) could both complete the event; it was not idempotent | CR WriteEvent/CompleteTxnEvent | M | COMPLETION-IDEMPOTENCE |
| 8a9dd2bed | KAFKA-16663 | Cancel write-timeout task on completion | Timeout tasks piled up for 15 s each | CR | L | TIMER-LIFECYCLE |
| 5b34574e8 | MINOR | Refactor write timeout | Follow-up to the above | CR | L | TIMER-LIFECYCLE |
| be17df6fd | KAFKA-16374 | HWM update gets front-of-queue priority | Deferred events paid the queue cost twice | CR, EventAccumulator | L | EVENT-ORDERING |
| 9865d54c4 | MINOR | `signal` instead of `signalAll` | Thundering herd in EventAccumulator | RT/EventAccumulator | L | EVENT-ORDERING |
| d585a494a | KAFKA-16831 | MemoryRecordsBuilder write limit | Default 16 KB limit, so valid writes failed with RecordTooLarge | CR batch | M | BATCH-SIZING |
| b888fa1ec | KAFKA-15910 | Snapshots while loading | No snapshot existed at the HWM after load, so reads threw "No in-memory snapshot". Fix: a snapshot at every batch at or above the HWM, deleted once committed | Loader, SnapshottableCoordinator | H | LOAD-SNAPSHOT |
| 461328607 | KAFKA-16030 | Partition goes offline during load | Load stopped silently but the coordinator became ACTIVE. Also, committed was only advanced when written >= HWM | Loader | H | LOAD-LIFECYCLE |
| 3a6e699f1 | KAFKA-16118 | Unload fails when replica deleted | Unload with an empty leader epoch threw | CR scheduleUnload | H | LOAD-LIFECYCLE |
| a48ca490e | KAFKA-15643 | Unload on a broker that was never coordinator | Spurious error path | CR | L | LOAD-LIFECYCLE |
| dcabc295e | KAFKA-14048 | Context protected by a lock | Context accessed by several threads with no memory barrier. Allows FAILED to LOADING | CR context | M | CONCURRENCY |
| b3116f4f7 | KAFKA-16148 | `onUnloaded` completes awaiting futures | Join/sync futures hung after unload | GMM | M | LOAD-LIFECYCLE |
| cd3b3d980 | MINOR | Custom retry backoff kept on retry | The timer retry used the default 500 ms | RT/CoordinatorTimerImpl | L | TIMER-PARAM |
| 31f79055c | KAFKA-17306 | Soften tombstone replay validation | Replay assumed the entity existed for tombstones, assumed a fixed record order, and conflicted with a simple classic group made by offset records. A load failure leaves the partition FAILED | GMM replay | H | REPLAY-ORDER-ASSUMPTION |
| eb897c6ad | MINOR | NPE in ConsumerGroup static-member removal | `oldMember` was null when removing an absent member | CG | M | REPLAY-ORDER-ASSUMPTION |
| fa190cf18 | MINOR | Only replay may mutate the `groups` timeline map | The generate phase created groups directly | GMM | M | GENERATE-PURITY |
| 2c0b8b692 | MINOR | `getOrMaybeCreateMember` must not add the member | The generate phase mutated state | CG | L | GENERATE-PURITY |
| 9bc48af1c | MINOR | Type check in classic timeout operations | Timers survive a reverted upgrade, so they must be no-ops if the group type changed | GMM | M | TIMER-VS-REVERT |
| 11c85a93c | MINOR | Session timeout used on upgrade conversion | Used the consumer session timeout instead of each member's classic session timeout, so members were fenced early | GMM convertToConsumerGroup | M | TIMER-PARAM |
| 7973aa6a3 | KAFKA-16861 | No downgrade above classic max size | Missing size check on downgrade | GMM migration | M | MIGRATION-VALIDATION |
| 8ccb26de2 | KAFKA-17733 | Empty member assignment in upgrade | BufferUnderflow while deserializing a never-synced member | GMM/ClassicGroup | M | MIGRATION-VALIDATION |
| 9ae1b0f01 | KAFKA-18134 | Disallow upgrade with custom-assignor userData | userData was lost on conversion | GMM migration | M | MIGRATION-VALIDATION |
| ed0e1086a | KAFKA-17228 | New-protocol static member always replaces classic one | A classic static member never sends a leave, so the upgrade got stuck | GMM static | M | STATIC-IDENTITY |
| 366aa1014 | KAFKA-17317 | Validate downgrade after static replacement | Replacement path skipped the downgrade check | GMM static/migration | M | STATIC-IDENTITY |
| 98f7da917 | KAFKA-16930 | NPE in uniform heterogeneous assignor | Member with no subscription; every heartbeat of the group failed | assignor via TargetAssignmentBuilder | H | ASSIGNMENT-COMPUTATION |
| e99561e1f | KAFKA-17593[10] / KAFKA-18116 | Resolved regex removed when unsubscribed; tombstones written on downgrade | Resolved regexes leaked on leave, fence, admin leave and conversion | GMM regex | M | REGEX-LIFECYCLE |
| 57737a357 | KAFKA-18188 | Admin LeaveGroup can remove a consumer-protocol member by id | API rejected consumer members | GMM LeaveGroup | M | LEAVE-PATH |

## 3. Design contracts extracted (with the HEAD implementation site)

Runtime (7556ce366, 9cde3a791, 482299c4e, 5ae0b4983, b4c2d6680/638844f83, efe7ccaf7, a1e985d22 and the fixes above):
- **C1. Generate, then replay, then append.** A write op generates records. Each record is replayed into timeline state at `currentBatch.nextOffset` before it is appended. A replay exception fails the whole open batch, including other coalesced ops, and reverts to the batch's base offset. `CR:1008-1034`, `failCurrentBatch` `CR:786-793`.
- **C2. Responses are released only when the HWM reaches the batch.** At flush, the batch's deferred events go to `deferredEventQueue` keyed by the returned offset (`CR:738`). The HWM listener calls `completeUpTo(hwm)` (`CR:1831`). (7556ce366, be17df6fd)
- **C3. A no-record write waits for pending writes** (KAFKA-16973, KAFKA-18200). It attaches to the open batch if that batch has records, or waits for lastWrittenOffset if lastCommitted < lastWritten, or completes at once. `CR:880-891`.
- **C4. Offsets are monotonic.** lastWritten only increases; revert only goes backwards; committed <= written, and committed never decreases. Both throw otherwise. `RT/SnapshottableCoordinator.java:86-90,159-167`.
- **C5. The log is authoritative.** If the append offset differs from the expected `nextOffset`, the runtime goes FAILED, then LOADING, then throws NOT_COORDINATOR. `CR:721-736`.
- **C6. Completion is idempotent** (KAFKA-18174). `CR:1357,1662`. A write timeout (`OperationTimeout`, `CR:1121-1141`) fails only the response. The records still commit, so at-least-once state change with an error response is by design.
- **C7. Transactional writes and markers flush the open batch first**, so the log order is preserved. `CR:930-936,1072`.
- **C8. Non-atomic writes** (KAFKA-17058) may split one op across batches. A later failure leaves the earlier batches applied. This is only for internal cleanup ops.
- **C9. Load** reads to the log end with LOG_END isolation, so it includes the uncommitted tail. It creates snapshots at batch ends at or above the HWM (KAFKA-15910), commits per HWM, and fails if the log end offset becomes -1 (KAFKA-16030). `RT/CoordinatorLoaderImpl.java:142-167,298-314`.
- **C10. Lifecycle.** Load runs only if `context.epoch < partitionEpoch` (`CR:2371`). Unload runs if the epoch is absent or larger (`CR:2421`, KAFKA-16118). Unload deregisters the HWM listener, cancels timers and executor, fails deferred events and the open batch, then calls `onUnloaded` (`CR:618-636`). The allowed transitions are at `CR:271-316`.
- **C11. Timer.** Tasks are keyed by string, and `schedule` replaces and cancels the previous task (`RT/CoordinatorTimerImpl.java:138-139`). An expired task runs as a write op only if `tasks.remove(key, this)` succeeds (`:93`), which guards against stale incarnations. Failures are rescheduled when `retry=true` (`:124`). The `tasks` map is not timeline state: it is not reverted when a batch fails.

Group (7634eee26, e164d4d42, cf90382fb, aafbe3444, bb1c4465c, 98fbd8afc, fa190cf18):
- **G1. Member states.** STABLE goes to computeNextAssignment when memberEpoch != targetEpoch. UNREVOKED_PARTITIONS stays blocked until no revoked partition is reported owned (null owned counts as owning). UNRELEASED_PARTITIONS recomputes. UNKNOWN causes a fence. `CAB:187-262`.
- **G2. Epoch moves only on assignment.** The member keeps its epoch while revoking, and moves to the target epoch only when nothing remains to revoke. Newly granted partitions are added to `assignedPartitions` only if `currentPartitionEpoch == -1` or they are in the member's own pending revocation (KAFKA-19449, 2025). `CAB:404-475`.
- **G3. Single-owner bookkeeping.** `currentPartitionEpoch` maps each partition to its owning member epoch and is updated on every member replay (`CG:1090-1180`). KAFKA-16147 made violations throw. **HEAD (KAFKA-19862, 2025) softened this:** remove logs on mismatch (`CG:1120-1150`), and add allows overwrite when `prev < epoch` (`CG:1167-1172`).
- **G4. Epoch validation.** A request epoch greater than the member epoch is fenced. A smaller epoch is accepted only if it equals `previousMemberEpoch` and the owned partitions are a subset of the assigned ones (a lost response); epoch 0 means join. `GMM:1673-1694`.
- **G5. Rebalance timer.** It is armed whenever the member transitions into UNREVOKED and cancelled on any other transition (`GMM:4083-4096`). It fences only if the member is still at the armed epoch (`GMM:5569`). The session timer is rescheduled on every heartbeat (`GMM:2731`). Fencing cancels all timers (`GMM:5106`). `onLoaded` re-arms the session timer for every member and the rebalance timer for UNREVOKED members (`GMM:6928-6944`).
- **G6. Static temporary leave (epoch -2)** keeps the assignment with partition epochs reset to 0. It drops pending revocation and keeps the session timer, so the member is fenced if it does not return (`GMM:4927-4945`).
- **G7. Only replay mutates timeline state** (fa190cf18, 2c0b8b692). Exception: downgrade conversion mutates `groups` directly and returns `replayRecords=false` (`GMM:1388-1391`, `GMM:5060`). That is safe for the timeline (it reverts) but relies on type checks for timers (9bc48af1c).
- **G8. Soft state carries the group epoch.** The metadata refresh deadline stores the epoch, so a reverted epoch reads as expired (`GC/modern/ModernGroup.java:321,343`, 98fbd8afc). This is the right pattern for state that is not on the timeline.
- **G9. Target assignment** is computed when assignmentEpoch < groupEpoch (`GMM:2888`) and is stamped with the group epoch (`GC/modern/TargetAssignmentBuilder.java:150-153`). A static member inherits its predecessor's target by instance id (`CG:305-315`).
- **G10. Regex resolution** runs async through `CoordinatorExecutor` with key `groupId-regex` (`GMM:3822-3858`). Results for regexes nobody subscribes to any more are skipped (`GMM:3900`). An unresolved regex counts as subscribing to nothing (`CAB:494-496`).

## 4. Mechanisms and unaudited sibling sites in HEAD

### M1 COMMIT-GATING / LOAD-LIFECYCLE (KAFKA-16973, 15910, 16030, 16118, 14048)
- **S1. A stale load completion can be applied to a newer coordinator context. Verified suspicious (high: safety and availability).**
  - The bug: `load()` (`CR:578-614`) handles the completion with `coordinators.get(tp)` and checks only `state == LOADING`. It never compares the context identity or the epoch. This has been the design since 7556ce366.
  - Trigger: load L1 runs at epoch e. Unload(e+1) runs: the context goes CLOSED and is removed (`CR:2421-2431`). Load(e+2) then creates a new context in LOADING. Its load L2 queues behind L1, because `CoordinatorLoaderImpl` has a single thread (`RT/CoordinatorLoaderImpl.java:77`).
  - Outcome A: L1 finishes and its `CompleteLoad` event moves the **new** context to ACTIVE while that context's shard is still empty. L2 then replays on the loader thread into a shard that is already serving requests.
  - Outcome B: L1 failed with NOT_LEADER, which is likely because `getLogEndOffset` returns -1 for a non-leader. The new context then goes FAILED and stays FAILED until the next leader epoch (`CR:2371`).
  - Fix idea: guard with `context == this` or with the epoch. Worth a TLA+ action and a unit test (leader bounce during a long load).
- **S2. The HWM listener is registered without its current value. Verified suspicious (medium: liveness).**
  - The bug: `transitionTo(ACTIVE)` registers the listener (`CR:552-556`). `Partition.maybeAddListener` (`core/src/main/scala/kafka/cluster/Partition.scala:271-283`) does not push the current HWM. If the loaded tail is uncommitted (C9 leaves lastCommitted < lastWritten) and the HWM reaches the log end between the end of `doLoad` and `CompleteLoad`, no callback ever arrives for that advance.
  - Effect: every later no-record write (a steady-state heartbeat) waits in the deferred queue (`CR:885`) until some other write moves the HWM again, or until the write timeout. Reads also stay at a stale committed offset.
  - KAFKA-16973's test feeds the HWM manually, so this gap is untested.
- **S3. Loader `updateLastWrittenOffset` when the log start is above 0 or the log is fully compacted. Unsure.** If no batch is read (`readAtLeastOneRecord=false`), lastWritten stays 0 while the log end is larger. The first flush then hits C5 and reloads. It self-heals at the cost of one failed write. Related post-2025 fix: KAFKA-19862.
- **S4. HWM event left over from the old listener after a reload in the same tp. Looks fine.** The event uses `coordinators.get(tp)` (`CR:1822-1840`). A stale lower HWM makes `updateLastCommittedOffset` throw an ISE, which is logged and recovers on the next update.

### M2 TIMER-VS-REVERT / GENERATE-PURITY (9bc48af1c, fa190cf18, 2c0b8b692)
Timers are side effects of the generate phase that are not on the timeline (C11). A failed batch reverts the records but not the timers.
- **S5. Fence or leave cancels timers before the commit. Verified suspicious (low/medium: zombie member).** `consumerGroupFenceMembers` calls `cancelTimers` at generate time (`GMM:5106`). If the batch then fails without an unload, the member is restored but has no session timer. Possible causes are a replay ISE from another op coalesced into the same batch (C1), or RecordTooLarge. For a client-initiated leave (epoch -1), the client usually does not retry after close, so the member and its partitions stay forever. Timer-initiated fences are safe because `retry=true` reschedules them (`RT/CoordinatorTimerImpl.java:124`).
- **S6. Rebalance timer armed by a reverted transition to UNREVOKED. Unsure (low).** `GMM:4087` arms the timer with the member epoch. Revocation does not bump the epoch, so after a revert the timer can fence a STABLE member at the same epoch. That happens only if no later reconcile changes the member, because `maybeReconcile` returns early without cancelling (`GMM:4060-4062`).
- **S7. Downgrade and upgrade conversion timers. Looks fine.** Consumer timers left behind after downgrade fail `consumerGroup(groupId)` and do nothing (`GMM:5406-5424`). Classic timers have type checks (9bc48af1c). Join and sync futures completed with REBALANCE_IN_PROGRESS in `convertToConsumerGroup` cannot be reverted, but that is harmless.
- Modeling note: G8 (the deadline carries the epoch) is the safe pattern. Timers do not follow it.

### M3 OWNERSHIP-BOOKKEEPING / RECONCILIATION-SM (KAFKA-16147, KAFKA-16249)
- **S8. The safety net against double ownership was weakened in HEAD. Verified (design risk, high relevance to Priority Q1).** KAFKA-19862 made `addPartitionEpochs` overwrite any lower-epoch owner (`CG:1167-1172`) and `removePartitionEpochs` silently skip mismatches (`CG:1127-1135`). This applies on the live write path, not only during load. Any future regression of the KAFKA-16147 kind would now be silent. A TLA+ invariant must state that each partition has at most one owner; the code assertion can no longer be relied on.
- **S9. Rebalance timer reset in UNREVOKED when the subscription changes. Verified (low, client-driven).** `CAB:218-223` returns a changed member through `updateCurrentAssignment`, and `GMM:4086-4092` re-arms the timer with the full `rebalanceTimeoutMs`. A member that keeps changing its subscription postpones its revocation deadline indefinitely. This is the issue KAFKA-16249 described, still present on this path.
- **S10. The "own pending revocation is not unreleased" exception (`CAB:404-410`). Looks fine.** It is reached only after `ownsRevokedPartitions` is false, and the partition's epoch belongs to the same member.
- **S11. Static temporary leave keeps the state UNREVOKED with empty pending revocation (`GMM:4935-4939`). Unsure (cosmetic).** The pending-revocation partitions are freed immediately, and on rejoin the empty revocation set unblocks the member. The group state may report RECONCILING spuriously.

### M4 REPLAY-ORDER-ASSUMPTION (KAFKA-17306, eb897c6ad; continued post-2025 by KAFKA-19862)
- **S12. Remaining ISE checks in tombstone replay.** These are: the member subscription tombstone needs epoch -1 and no target assignment (`GMM:6058-6066`); the group epoch tombstone needs no members, an empty target and assignmentEpoch -1 (`GMM:6176-6189`); the target-assignment metadata tombstone (`GMM:6275-6278`). **Unsure (high if hit).**
  - Under ordinary compaction the removal records are written together (`GMM:5332-5336`), so these checks hold.
  - Under compaction that runs concurrently with loading (segment swap during the read, the KAFKA-19862 mechanism), a subset of records could be observed. KAFKA-19862 relaxed only the partition-epoch checks.
  - Any ISE fails the whole load, and the partition stays FAILED until the next leader epoch (C10).

### M5 EPOCH-VALIDATION / REQUEST-SHAPE (16367, 18095, 8864cba0e)
- **S13. A lost response recovers only one epoch. Unsure (by design).** `GMM:1689` accepts only `previousMemberEpoch`, and only when the request carries owned partitions (`isSubset(null)` is false, `GMM:1527`). Two consecutive lost or timed-out responses (C6: the write timeout still commits) fence the member. This matches the guidance case "stale by more than one". It is safe but disruptive.
- **S14. The full-request predicate at `GMM:2745`. Looks fine** after 8864cba0e.

### M6 STATIC-IDENTITY / MIGRATION (14516, 17228, 17317, 16861, 17733, 18134)
- **S15. Static replacement copies the predecessor's assignment** (`ConsumerGroupMember.Builder(previous, memberId)`, `GMM:3519-3525`). Ownership moves to the new member id without a revocation step. This is correct only if the old instance is really gone. Under KAFKA-17228, a classic static member is replaced even if it never left. **Unsure:** two live processes with the same `group.instance.id` would both consume. That is a client misconfiguration, so it may be out of contract.

### M7 REGEX-LIFECYCLE (17592/17593 series)
- **S16. Resolution runs against the image captured at schedule time** (`GMM:3855`). A result can include topics deleted meanwhile, and refreshes are rate-limited to 10 s (`GMM:3827-3831`). A new regex introduced while another resolution is in flight is skipped (`GMM:3823-3825`) and waits at least 10 s. **Looks fine for safety**, because `TopicIds` filters missing topics. Liveness lags are bounded.

### M8 COMPLETION-IDEMPOTENCE / TIMER-LIFECYCLE (18174, 16663)
- **S17. `CoordinatorInternalEvent.complete` and `CoordinatorReadEvent.complete` (`CR:1509,1764`)** have no `isDone` guard. **Looks fine:** they are not deferred and have no timeout.
