# Bug archaeology: Kafka group coordinator (KIP-848 consumer groups), 2025-01-01 .. trunk 1224f3d4b

Evidence: git only (`git log`, `git show`). All file:line citations refer to trunk HEAD `1224f3d4b`.
Abbreviations: `GMM` = `group-coordinator/.../group/GroupMetadataManager.java`,
`CG` = `.../modern/consumer/ConsumerGroup.java`, `CAB` = `.../modern/consumer/CurrentAssignmentBuilder.java`,
`CGM` = `.../modern/consumer/ConsumerGroupMember.java`, `TARB` = `.../group/TargetAssignmentRecordsBuilder.java`,
`CR` = `coordinator-common/.../runtime/CoordinatorRuntime.java`, `CTI` = `.../runtime/CoordinatorTimerImpl.java`.

## 1. Coverage

| Item | Count |
|---|---|
| Commits in `commits-2025plus.txt` | 273 |
| Titles skimmed | 273 |
| Inspected in detail (message + stat; diff for the key ones) | 71 from the list, plus 7 commits that the "share" title filter dropped but that touch the consumer path (`6fa3711c3` [3/N], `881f2907b` [7/N], `d53a249e6` [10/N], `ad4361cd5` [12/N], `3ec856322`, `a76e58a87`, `0e1e68db8`) |
| In-scope bug fixes found | 41 (table in §2) |
| Excluded (overlapping categories, approximate) | ~70 streams/KIP-1071 (assignors, refiner, IQ, topology plugin, warm-up tasks); ~20 offsets/txn/topic-id (KIP-1319, KIP-848 OffsetFetch); ~55 config/metrics/docs/javadoc; ~60 MINOR refactors and cleanups with no behaviour change; ~20 new features or performance work (uniform2 assignor, KIP-1251 parts 1-3, adaptive linger, batching configs, KAFKA-19955 perf, KAFKA-19427 buffers); 3 classic-group-only fixes (KAFKA-20938, a3e4a7e42, KAFKA-20288) |

## 2. In-scope bug fixes

Severity: Crit = can break a safety property (ownership, durability, state divergence); High = stuck/fenced member or coordinator, or a broken invariant with no proven overlap; Med = wrong rebalance trigger or a liveness gap; Low = cosmetic, performance or unreachable.

| sha | ticket | summary | root cause | component | sev | mechanism |
|---|---|---|---|---|---|---|
| d559af58d | KAFKA-20634 | HWM after the leader moves completes deferred writes for truncated records | the partition writer forwarded HWM updates after the partition became a follower; the HWM then covered records written by the new leader | PartitionWriter / runtime | Crit | commit/HWM |
| 475e0a1fb | KAFKA-20845 | an empty-batch flush reverts state while the records are still written | the KAFKA-19760 eager flush ran on a freshly allocated empty batch; `failCurrentBatch` reverted non-replayed mutations (downgrade) | CR append | Crit | runtime revert vs direct mutation |
| 8ff7addce | KAFKA-19974 | no epoch bump when leaving UNREVOKED_PARTITIONS | `min(e+1, target)` bump let one partition be owned by two members at the same epoch | CAB | High | epoch bookkeeping |
| 384586e52 | KAFKA-19233 | a fenced member can rejoin with epoch 0 | epoch validation rejected 0 when the server epoch was higher | GMM epoch check | High | epoch validation |
| 3e5df2f08 | MINOR | a static member was fenced by its own instance id (fatal UNRELEASED_INSTANCE_ID) | a join could change the member's instance id, so the coordinator saw a self-replacement | GMM static join | High | static identity |
| 83dc0d7ea | KAFKA-19862 | loading fails during concurrent compaction | strict `removePartitionEpochs`/`addPartitionEpochs` checks threw on replay | CG | High | replay/load (relaxed invariant, see §3.2) |
| d7fa406b3 | KAFKA-19064 | exceptions from deferred events on 0-record and transaction-completion paths | completion threw and left the runtime stuck | CR | High | deferred-event errors |
| 5a57473a5 | KAFKA-18484 | exceptions from deferred events in batches | same, batch path | CR | High | deferred-event errors |
| 5946f27ac | KAFKA-18484 | exceptions in `onUnloaded` | unload could throw and leave the runtime half-unloaded | CR unload | High | deferred-event errors |
| 388739f5d | KAFKA-19760 | RecordTooLarge with compression | optimistic compression estimate overfilled batches | CR batching | High | write failure |
| 52687216c | KAFKA-20442 | assignment epoch not bumped when the last member is fenced | fence bumped the group epoch only, so an empty group had groupEpoch > assignmentEpoch | GMM fence | Med | epoch bookkeeping |
| b8692f315 | MINOR | `previousMemberEpoch` not updated on unsubscribe shrink | `updateCurrentAssignment` did not call `updateMemberEpoch` | CAB | Med | epoch bookkeeping |
| 0d9fe518b | KAFKA-20434 | no recompute when static members rejoin with a different assignor | bump condition ignored the preferred assignor | GMM heartbeat | Med | rebalance trigger |
| 82241e187 | KAFKA-19891 | regex non-empty -> empty does not bump the group epoch | returned REGEX_UPDATED instead of ..._AND_RESOLVED | GMM regex | Med | regex resolution |
| 1df4a42b4 | KAFKA-18916 | resolved regexes did not update groupsByTopics | topic deletion did not trigger a rebalance | GMM replay | Med | regex resolution |
| af012e1ec | KAFKA-18961 | time-based regex refresh | ACL changes never re-resolved | GMM regex | Med | regex freshness |
| 0f4dbf7fd | KAFKA-19720 | regex kept when a classic member replaces a consumer member | regex not cleared, so no recompute | GMM migration | Med | regex / migration |
| 6ef10a9bb | KAFKA-20272 | classic members told to rejoin only on a new assignment | with batching the group epoch can run ahead of the assignment | GMM classic HB | Med | assignment lag |
| 4ea29da3c | KAFKA-20273 | rebalance on downgrade with a stale assignment | downgrade copied a lagging target | GMM downgrade | Med | assignment lag |
| cbea4f69b | KAFKA-19546 | subscription change during downgrade does not rebalance | static replacement path ignored the subscription change | GMM migration | Med | migration |
| 65b93fa8d | KAFKA-18150 | downgrade on classic leave of the last consumer member | missing downgrade path | GMM migration | Med | migration |
| e6d400336 | KAFKA-20640 | classic joins admitted under `disabled` policy | missing policy check | GMM migration | Low | migration |
| fcb722dc8 | KAFKA-18687 | spurious rebalance after classic->consumer conversion | subscription metadata not set in `fromClassicGroup` | CG | Low | migration |
| 349a647c4 | KAFKA-21303 | reload used the consumer session timeout for classic members | `onLoaded` ignored `classicProtocolSessionTimeout` | GMM onLoaded | Med | timers on load |
| 71faeec8b | KAFKA-20747 | stale `staticMembers` entry after an instance id change | `updateStaticMember` did not remove the old mapping | CG | Med | static identity |
| 4626eccfa | KAFKA-20292 [5/N] | same bug in UpdatedMembersAndTargetAssignmentView | same | util view | Low | static identity |
| e6919341b | KAFKA-20601 | static rejoin rejected when the group is full | size check ran before static resolution | GMM | Low | static identity |
| 02bd3420d | KAFKA-19449 | own pending-revocation partition treated as unreleased | ownership check included self | CAB | Low | reconciliation |
| d9462c277 | KAFKA-19857 | `CoordinatorExecutorImpl.cancelAll` always threw | `Iterator.remove` before `next` | runtime executor | Med | unload / executor |
| 06f7cb3e5 | MINOR | timer/executor misclassified wrapped exceptions | missing `CompletionException` unwrap | CTI | Med | timer errors |
| cad46800c | MINOR | timer task map not thread-safe | HashMap accessed from timer and event threads | CTI | Med | timer |
| e4bef376e | MINOR | schedule* threw synchronously | no try/catch around the future-returning code | CR | Low | runtime API |
| 82a1935b7 | MINOR | linger time comparison inverted | `<` vs `>=` | CR | Low | batching |
| d067c6c04 | KAFKA-19716 | snapshot memory growth while loading | snapshots never discarded during load | CoordinatorLoaderImpl | Med | load |
| bf05d2c91 | KAFKA-18672 | serde accepted unknown value versions | no version check | serde | Med | load |
| d5d986856 | KAFKA-20431 | describe omitted pending-revocation partitions | read model only | CGM | Low | observability |
| ec004fd80 | KAFKA-20662 | malformed migration subscription -> UNKNOWN_SERVER_ERROR | wrong exception type | GMM | Low | migration |
| b860fd33a | KAFKA-21318 | GroupSpec saw same-request removed members as assigned | stale inverted target assignment (unreachable today) | GroupSpecImpl | Low | assignor input |
| 647466c05 | KAFKA-21318 | negative partition IOOBE in `isPartitionAssigned` | missing bounds check | GroupSpecImpl | Low | assignor input |
| c16c240bd | KAFKA-18688 | uniform assignor moved partitions in a balanced group | extra-partition placement rule | assignor | Low | assignor |
| ae5d57b77 | KAFKA-21196 | cancelled executor tasks retained memory | references not released | executor | Low | resource |

## 3. Grouping by mechanism, with sibling-site audit of current trunk

### 3.1 Epoch bookkeeping on member state transitions (KAFKA-19974, b8692f315, KAFKA-20442, KAFKA-19233)
Invariants intended: (I1) a partition is owned by at most one member at a given member epoch; (I2) a member leaving UNREVOKED_PARTITIONS jumps to the target assignment epoch; (I3) an empty group has assignmentEpoch == groupEpoch; (I4) epoch 0 always means (re)join.
Siblings checked:
- **CAB `updateMemberEpoch(sameEpoch)` overwrites `previousMemberEpoch`.** `CGM:105-110` sets `previous = current` even when the new epoch equals the old one. Callers that pass the unchanged epoch are `CAB:342,354` (`updateCurrentAssignment`), `CAB:432` (UNREVOKED entered from `computeNextAssignment(member.memberEpoch())`, `CAB:229-234`) and `CAB:462`/`452` when target == current. After any of these, the lost-response tolerance in `GMM:1686-1693` (accept `previousMemberEpoch` plus owned ⊆ assigned) no longer covers the real previous epoch. A client that lost the bump response and then triggers one of these transitions gets FENCED_MEMBER_EPOCH on its next retry. This behaviour predates the window (same as the 2024 code), so it is a **liveness issue, not a safety issue. Verdict: looks fine for safety, but model `previousMemberEpoch` exactly**.
- **KAFKA-20442 completeness.** Every consumer-protocol path that empties a group goes through `consumerGroupFenceMembers` (`GMM:5040`): leave (`4841,4853`), session timeout (`5417`), rebalance timeout (`5574`), classic join/sync timeouts (`5665,5699`) and classic LeaveGroup (`9187`). The fix sits at `GMM:5100-5103`. A static `-2` leave (`GMM:4927-4946`) does not remove the member. **Looks fine.**
- **KAFKA-19233 + static rejoin.** An epoch-0 rejoin of a known member returns the existing state unchanged (`GMM:1680`, `3496-3503`). Reconciliation then runs with the client's owned set (normally empty), so pending revocations are treated as done (`CAB:218`). Safe only if a client that sends epoch 0 really owns nothing, which is the KIP-848 contract. **Looks fine. Modeling note: the model must give epoch-0 rejoin `owned = {}`; a buggy client that rejoins with owned partitions is outside the assumptions.**
- **Rebalance-timer identity after KAFKA-19974.** The timer guard compares `memberEpoch` (`GMM:5569`). Because UNREVOKED->UNREVOKED no longer bumps the epoch, the guard can no longer tell revocation rounds apart. `maybeReconcile` re-schedules (resets the deadline) on every changed UNREVOKED state (`GMM:4086-4092`) and cancels the timer otherwise (`4094`), so a stale timer cannot fence a later round. Side effect: a member that keeps changing its subscription while in UNREVOKED extends its own deadline. **Looks fine for safety, minor liveness.**

### 3.2 Replay/load consistency and the relaxed partition-epoch invariant (KAFKA-19862, KAFKA-19716, KAFKA-18672, KAFKA-21303)
KAFKA-19862 turned the ownership checks in `CG` into best-effort checks. This applies at runtime, not only during load:
- `removePartitionEpochs` now ignores mismatches, logging at DEBUG (`CG:1120-1148`).
- `addPartitionEpochs` overwrites any older epoch held by *another* member (`CG:1169-1170`) and throws only if `prev >= epoch` (`1172`).
- **Verified suspicious (as a detector, not as a bug):** `currentPartitionEpoch` is the only runtime guard against assigning a partition still held by another member (`CAB:404-410`, `CG:1379`). A double ownership in which the new owner has a higher epoch is now silently absorbed: the old owner's later removal at its own epoch is a no-op, so the map then says the old owner does not hold the partition even though its `assignedPartitions` still contains it. **For TLA+, state ownership invariants over members' `assignedPartitions ∪ partitionsPendingRevocation`, not over `currentPartitionEpoch`.**
- Remaining strict load checks are still present: the member tombstone requires a prior current-assignment tombstone (`GMM:6058-6065`), and the group tombstone requires the TA-metadata tombstone (`6176-6188`, `6275-6278`). A compaction race that drops only one of a pair of tombstones would still throw during load. **Unsure** (depends on how compaction interacts with the loader; KAFKA-19862 only addressed the partition-epoch case).
- KAFKA-21303 sibling: `onLoaded` (`GMM:6930-6945`) re-arms the session timer and the UNREVOKED rebalance timer. It does not re-arm the classic join/sync timers (`5655`, `5689`) for classic members in mixed groups. Pending JoinGroup/SyncGroup futures die with the old leader, and the session timer still bounds a dead member, so this **looks fine**.
- Legacy current-assignment records without per-partition epochs default to `max(memberEpoch, 0)` (`Utils.java:251-273`). That value is at least the true assignment epoch, so the offset-commit check becomes stricter, not looser. **Looks fine.**

### 3.3 Static member identity (3e5df2f08, KAFKA-20747, KAFKA-20292 [5/N], KAFKA-20601, KAFKA-20434)
Invariant: a member id never changes its instance id; an instance id maps to exactly one live member id.
- Joins are now enforced at `GMM:3485` (`throwIfMemberIdHasDifferentInstanceId`). Non-join heartbeats go through `staticMember(instanceId)` plus `throwIfInstanceIdIsFenced` (`3471-3478`), so the instance id cannot change on that path either. A dynamic-path heartbeat (instanceId null) with a static member's id keeps the instance id (`maybeUpdateInstanceId(Optional.empty())`, `2641`). **Looks fine.**
- `replaceMember` (`GMM:5170-5196`) does not cancel the old member's timers; they are keyed by the old member id, and the timer operation re-validates the member. **Looks fine.**
- A static `-2` leave resets partition assignment epochs to 0 (`GMM:4932-4938`), so the replacement inherits epoch-0 partitions. That widens the KIP-1251 commit window for the new member id only. **Looks fine** (the zombie uses a different member id).

### 3.4 Regex resolution freshness (KAFKA-19891, KAFKA-18916, KAFKA-18961, KAFKA-19720)
Invariant: every subscription change that can change a member's effective topic set eventually bumps the group epoch.
- **Verified suspicious (liveness, Medium, sibling of KAFKA-19891). Scenario:** M1 and M2 subscribe to regex A, which resolved to {t}. M1 switches to a new regex B that nobody else uses (`GMM:3789-3794`), so the status is `REGEX_UPDATED` with no bump (`2677-2687`). `hasSubscriptionChanged` is true, so `updateCurrentAssignment` treats the unresolved B as matching nothing (`CAB:494-496`) and revokes M1's t-partitions. When B resolves to the **empty** set, `handleRegularExpressionsResult` compares it with `EMPTY` (`GMM:3903-3907`): no change, so no bump. The metadata hash is also unchanged because t is still subscribed through A by M2 (`3927-3939`), and a metadata-expiry refresh only bumps on a hash change (`4272-4278`). Result: the target assignment still gives t-partitions to M1, whose subscription excludes t, so they are filtered out at `CAB:389-391` and **nobody consumes them until some unrelated membership change happens.** KAFKA-19891 fixed only the regex->"" case (`3802-3804`); the regex->unresolved-regex-that-resolves-to-nothing case is the unaudited sibling.
- Replay of `ConsumerGroupRegularExpression` updates groupsByTopics (`GMM:6353-6368`), so the KAFKA-18916 fix is present. **Looks fine.**
- Classic join clears the regex (`GMM:2840`) and runs `maybeUpdateRegularExpressions` (`2858-2866`); the bump there uses `regexUpdated()` (any update), which is stronger than the consumer path. **Looks fine.**

### 3.5 Deferred events, runtime revert and side effects (KAFKA-18484, KAFKA-19064, KAFKA-20845, KAFKA-20634, KAFKA-19760)
Invariants: a deferred response completes only after its records commit (Q3); a failed batch reverts exactly the state that never reaches the log.
- `failCurrentBatch` reverts to the batch base offset and fails *all* events in the batch (`CR:786-792`). A replay exception in one operation fails every other operation in the same batch (`CR:1020-1032`).
- **Verified suspicious (low probability): timer side effects are not reverted.** `consumerGroupFenceMembers` cancels the member's timers synchronously while it builds records (`GMM:5105-5107`). If the batch then fails for a reason that does not unload the coordinator (for example a replay exception from a later operation in the same batch, or RecordTooLarge; `NotLeaderOrFollower` leads to an unload, which is fine), the fenced member is restored by the snapshot revert but has no session or rebalance timer. A timer-driven fence is retried (`CTI:121-125`). A client-driven leave (`GMM:4841`) or classic LeaveGroup (`9187`) is not retried, so a member whose client has already gone stays in the group until a heartbeat with its id arrives, which never happens. The same pattern exists for `createGroupTombstoneRecordsAndCancelTimers` (`GMM:9343`).
- Operations that use `replayRecords=false` (downgrade `GMM:5060`, classic join downgrade `2968`) mutate state directly. KAFKA-20845 removed the one known path where state was reverted but the records were still written (`CR:963-964`). The remaining flush calls at `CR:934`, `969` and `998` run before the current operation's records are appended, so a failure there drops both the mutation and the records. **Looks fine.**
- The HWM gate (KAFKA-20634) lives in core (`CoordinatorPartitionWriter.scala`). `CR:1810-1846` applies whatever HWM arrives while the coordinator is ACTIVE, so the coordinator relies entirely on the writer's leadership gate. **For modeling: HWM delivery after a leadership change must be impossible, not merely ignored.**

### 3.6 Target-assignment lag and batching (KAFKA-20272, KAFKA-20273, KAFKA-20268, KAFKA-20442)
Invariant: when groupEpoch > assignmentEpoch, a new target assignment is eventually computed and every member is told to move to it.
- **Verified suspicious (liveness, Medium): no trigger for delayed assignment in classic-only consumer groups.** The consumer heartbeat computes the deferred assignment lazily (`GMM:4354-4366`); nothing schedules it for later (the only delayed-assignment timer is for streams, `GMM:2322`/`4778`). The classic heartbeat into a consumer group (`GMM:8761-8792`) neither calls `maybeUpdateTargetAssignment` nor tells members to rejoin when `groupEpoch > assignmentEpoch`; after KAFKA-20272 it checks only `memberEpoch < assignmentEpoch` (`8781`). Scenario: a consumer group whose members all use the classic protocol (for example after an upgrade, with the downgrade policy off), with `assignment.interval.ms > 0`. A classic member joins inside the interval (`2919-2926` returns the last target), and every later heartbeat returns NONE. The group stays in ASSIGNING (`CG:967`) and the new member keeps an empty assignment until another join or leave happens after the interval. The KAFKA-20272 fix removed the rejoin signal without adding a compute trigger.
- New members get `memberEpoch = assignmentEpoch < groupEpoch` with an empty assignment (`CAB:194-198`, `452`). This is why groups start at epoch 1 (`ModernGroup.java:126`, `TargetAssignmentMetadata.java:32`; KAFKA-20268). **Looks fine.**

### 3.7 Offload wiring (KAFKA-20292 series)
- `TARB:216-266` remaps static members and drops members that left, so that an asynchronous result can be applied "as if" concurrent leaves and replacements happened after it. **That path is dead code in production:** consumer and share groups always call `withUnchangedStaticMembers()` (`GMM:4412`, `4506`); `withChangedStaticMembers` and `withAssignorOffload` have no callers in `src/main` (grep), and `consumerGroupAssignorOffloadEnable` (`GMM:9915-9918`) is never read. **Unsure:** that logic has no production exercise; when it is wired, the `currentMemberIds.contains(oldMemberId)` guard (`TARB:242`) and the "new member gets EMPTY" fallback (`306-312`) are the parts to model.

## 4. Current semantics

### 4.1 Group epoch, assignment epoch, member epoch, per-partition assignment epoch (KIP-1251)
- **Group epoch** (`ModernGroup.groupEpoch`, initial value 1 at `ModernGroup.java:126`). It is bumped by subscription, resolved-regex or preferred-assignor changes (`GMM:2677-2687`), by a metadata-hash change (`4272-4282`), by regex results (`3941-3949`) and by every fence (`5091-5092`).
- **Assignment epoch** (`TargetAssignmentMetadata(assignmentEpoch, timestamp)`, initial value (1, 0) at `TargetAssignmentMetadata.java:32`). It is the group epoch for which the stored target assignment was computed. It is written as the last record of a target-assignment computation (`TARB:327-331`), or set equal to the group epoch when the group becomes empty (`GMM:5100-5103`). The tombstone sets it to -1 (`GMM:6279`). Group state is ASSIGNING iff groupEpoch > assignmentEpoch, RECONCILING iff some member is not (STABLE and epoch == assignmentEpoch) (`CG:963-979`).
- **Member epoch.** It moves to `targetAssignmentEpoch` whenever the member leaves UNREVOKED or reaches STABLE/UNRELEASED (`CAB:452,462,471`). It stays unchanged while entering or remaining in UNREVOKED (`CAB:432,342`; KAFKA-19974). A static leave sets it to -2 (`GMM:4935`); a tombstone replay sets it to -1 (`6322`). `previousMemberEpoch` is overwritten on every `updateMemberEpoch` (`CGM:105-110`).
- **Per-partition assignment epoch** (`assignedPartitions: Map<topic, Map<partition, epoch>>`). It is set to `targetAssignmentEpoch` when a partition is newly granted (`CAB:442-448`) and kept for retained partitions (`CAB:394-395`). It is reset to 0 on a static `-2` leave (`GMM:4937`) and copied on static replacement (`GMM:3521-3524`). Classic->consumer conversion uses the generation id (`CG:1265`).
- **OffsetCommit check** (`CG:680-697`, `869-906`): `clientEpoch == memberEpoch` is accepted with no per-partition check. `clientEpoch > memberEpoch` is rejected. `clientEpoch < memberEpoch` is accepted per partition iff the partition is in assigned or pending revocation and `assignmentEpoch(p) <= clientEpoch`.
- **Partition ownership map** `currentPartitionEpoch` stores the *member* epoch of the owner (`CG:1095-1096`) and is relaxed as described in §3.2.

### 4.2 Assignment interval and batching
- Per-group `consumer.assignment.interval.ms` overrides the dynamic broker config `group.consumer.assignment.interval.ms` (`GMM:9905-9908`).
- In the consumer heartbeat, a new target is computed only if `assignmentEpoch < groupEpoch` **and** `canComputeNextTargetAssignment(lastTimestamp, interval, now)` (`GMM:4354-4366`). That function returns true if timestamp == 0, if interval == 0, or if now >= timestamp + interval (`4312-4333`). Otherwise the heartbeat reconciles against the last target and `group.assignmentEpoch()` (`298-306`).
- The timestamp is `time.milliseconds()` at the end of the assignor run (`TargetAssignmentBuilder.java:152`). It is persisted in the TA-metadata record and reset to 0 for empty groups (`GMM:5101-5102`).
- Batching is **lazy**: no timer exists for consumer groups (§3.6), so progress depends on a consumer-protocol heartbeat or join after the interval has passed.

### 4.3 Offload
- Configs exist: `group.consumer.assignor.offload.enable` (`GroupCoordinatorConfig.java:1298`) and the group-level override (`GroupConfig.java:1111`), resolved by `GMM:9915-9918`.
- **Offload is not wired for consumer groups.** The resolver has no caller. The consumer path runs the assignor synchronously on the event thread (`GMM:4383-4398`) without `withAssignorOffload(true)`. `TargetAssignmentRecordsBuilder` is always used with `withUnchangedStaticMembers()` (`4412`), and no executor task computes assignments (the executor is used only for regex resolution, `3853`). The KAFKA-20292 [1..13/N] series and KAFKA-20268 (epoch 1 start) are preparation work. The model can treat target computation as atomic within one heartbeat event, plus optional lag from batching.

## 5. Follow-up-fix patterns (incomplete auditing)
- KAFKA-20066 (KIP-1251) needed b8692f315 and KAFKA-19974 (epoch handling in CAB), then KAFKA-20442 ×3 (consumer, share and streams fixed separately). Epoch bookkeeping is edited site by site.
- Batching (KAFKA-20291) needed KAFKA-20272 and KAFKA-20273 for the classic paths. §3.6 shows the classic heartbeat path is still not fully covered.
- Static identity needed KAFKA-20747 (ConsumerGroup and StreamsGroup), KAFKA-20292 [5/N] (view) and 3e5df2f08 (join), so the same mapping bug was fixed in three places.
- Regex freshness needed KAFKA-18916, KAFKA-18961, KAFKA-19891 and KAFKA-19720. Each fixed one transition kind; §3.4 shows another one is still open.
- Runtime error handling needed KAFKA-18484 [1/2], [2/2], KAFKA-19064, KAFKA-19857 and two MINOR timer fixes, after which KAFKA-20845 found a revert/direct-mutation divergence. Side-effect reversal (timers) is still not handled (§3.5).
