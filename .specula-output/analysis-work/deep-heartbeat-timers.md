# Deep analysis: consumer-group heartbeat path and timers (GroupMetadataManager)

Source: `kafka/` trunk @ 1224f3d4b2 (2026-10-27). Paths are abbreviated:
`GMM` = `group-coordinator/.../group/GroupMetadataManager.java`,
`CTI` = `coordinator-common/.../runtime/CoordinatorTimerImpl.java`,
`CR` = `coordinator-common/.../runtime/CoordinatorRuntime.java`,
`CAB` = `group-coordinator/.../modern/consumer/CurrentAssignmentBuilder.java`,
`CG` = `group-coordinator/.../modern/consumer/ConsumerGroup.java`.
Guidance priority questions addressed: Q1, Q3, Q4 (as they relate to GMM).

## 0. Execution model facts the analysis relies on

- A heartbeat is a write op: `generateRecordsAndResult` runs on the shard thread, records are
  replayed immediately into timeline state (uncommitted), appended to the current batch, and the
  response is a deferred event released at HWM (CR:1317-1345, CR:906-1030). Records are NOT
  replayed while the op runs: inside `consumerGroupHeartbeat`, `group.*` always reflects the
  state *before* this heartbeat (all previous ops, committed or not).
- Ops with zero records still wait for pending writes (`waitForPendingWrites`, CR:922-925), so a
  success response never reflects uncommitted state.
- An exception thrown during generation completes the future immediately (CR:1344-1346), i.e.
  **error responses are not deferred** and may be derived from uncommitted state (e.g.
  UNKNOWN_MEMBER_ID after an uncommitted fence). This errs on the safe side (client drops
  partitions).
- Batch failure (`failCurrentBatch`, CR:786-792) reverts *all* ops in the batch (linger batching),
  via `revertLastWrittenOffset`. Timer/executor side effects are **not** reverted.
- Write timeout (`OperationTimeout`, CR:1337, CR:1136-1140; default `offsets.commit.timeout.ms` =
  5000, GroupCoordinatorService:288) fails the future with `TimeoutException` but does **not**
  revert the records; they may still commit later.
- Timer semantics (CTI): one task per key; `schedule` = put+cancel previous (CTI:138-139);
  on expiry the op is enqueued as a write op and first checks `tasks.remove(key, this)`
  (CTI:93) so an overridden/cancelled task is a no-op; on failure other than
  RejectedExecution/NotCoordinator/LoadInProgress and `retry=true`, it **re-schedules the same
  closure with a 500 ms backoff via `schedule(key, ...)` (CTI:121-124), which overwrites whatever
  task currently holds the key**. All consumer timers use `retry=true`.
- Unload: `timer.cancelAll()` (CR:623); load: `onLoaded()` re-creates timers (GMM:6906-6950).

## 1. Per-heartbeat state transition (TLA+ action `ConsumerHeartbeat(m, req)`)

Entry: `consumerGroupHeartbeat(ctx, req)` (GMM:5725) dispatches epoch -1/-2 to
`consumerGroupLeave` (GMM:5729-5736), else the private `consumerGroupHeartbeat` (GMM:2589-2751).
Request validation in GroupCoordinatorService:470-509: epoch 0 => rebalanceTimeout set,
`topicPartitions` non-null AND empty, subscription non-null; epoch < -2 rejected.

State read: `groupEpoch G`, `assignmentEpoch A`, `assignmentTimestamp`, `targetAssignment`,
`members[m]` (epoch, prevEpoch, state, assigned, pendingRevocation, subscription, instanceId,
rebalanceTimeout, classic metadata), `currentPartitionEpoch`, static-member index, resolved regexes,
`metadataHash`, `metadataRefreshDeadline` (non-timeline, ModernGroup:117).

Steps (all records accumulate in one atomic list, CoordinatorResult default isAtomic=true):
1. Group: `createIfNotExists = (req.epoch == 0)` (GMM:2606). Missing group and epoch != 0 =>
   GROUP_ID_NOT_FOUND. New group is a detached object (GMM:887-888); materialised on replay.
   Size check (GMM:1606-1625).
2. Member lookup (guards; any failure = error response, no records, no timer change):
   - Dynamic (GMM:3378-3393): `getOrMaybeCreateMember` (CG:255-269, no mutation) then
     `throwIfConsumerGroupMemberEpochIsInvalid` (GMM:1673-1694):
     epoch 0 => always accepted (KAFKA-19233, GMM:1680, commit 384586e52);
     `req.epoch > m.epoch` => FENCED_MEMBER_EPOCH;
     `req.epoch < m.epoch` accepted only if `req.epoch == m.prevEpoch` and
     `owned ⊆ m.assigned` (isSubset GMM:1523-1538), else FENCED.
     Unknown member with epoch != 0 => UNKNOWN_MEMBER_ID.
   - Static, epoch != 0 (GMM:3470-3478): owner of instanceId must exist and have this memberId
     (FENCED_INSTANCE_ID otherwise), then same epoch check.
   - Static, epoch 0 (GMM:3480-3532): (a) known memberId must own this instanceId, else
     INVALID_REQUEST (GMM:3485, commit 3e5df2f08); if its epoch == -2 return copy with
     `epoch=0, prevEpoch=0` **in memory only** (GMM:3487-3495); else return it unchanged
     (GMM:3496-3502). (b) unknown memberId, instance unowned => fresh member. (c) unknown
     memberId, instance owned by M_old: consumer->consumer requires `M_old.epoch == -2`
     (GMM:3514-3518, UNRELEASED_INSTANCE_ID otherwise); copy M_old under new id with epoch 0/0 and
     emit `replaceMember` records: tombstones(currentAssignment, targetAssignment, member) for
     M_old, member record, target-assignment record (copy of M_old's target), current-assignment
     record for new member (GMM:5170-5195). **Old member's timers are deliberately NOT
     cancelled** (GMM:5178-5179).
3. Metadata merge (GMM:2640-2650) -> `updatedMember`; if `updatedMember != member` emit
   `ConsumerGroupMemberMetadata` record (GMM:3690-3706). `subscribedTopicNamesChanged` iff names
   differ.
4. Regex (GMM:3756-3874): may emit regex tombstone; may schedule async resolution on the executor
   (in-memory, not reverted); returns NO_CHANGE / REGEX_UPDATED / REGEX_UPDATED_AND_RESOLVED.
5. `bumpGroupEpoch = namesChanged ∨ regex==UPDATED_AND_RESOLVED ∨ preferredAssignorChanged`
   (GMM:2677-2687). If bump or metadata expired: `updateSubscriptionMetadata` (GMM:4235-4300):
   bump also if metadata hash changed; emits `ConsumerGroupMetadata(G+1, hash)`; side effect
   `group.setMetadataRefreshDeadline(now+interval, G')` (GMM:4287; non-timeline but guarded by
   `groupEpoch() < deadline.epoch` in `hasMetadataExpired`, ModernGroup:342-344).
   Local `G' ∈ {G, G+1}`.
6. Target (GMM:4346-4430): if `A >= G'` reuse old target (`fromLastTargetAssignment`,
   GMM:298-306, looks up by instanceId for static members). Else if
   `canComputeNextTargetAssignment(ts, interval, now)` is false (GMM:4312-4334: false iff
   ts != 0 ∧ interval != 0 ∧ now < ts+interval) also reuse old target at epoch A — the group stays
   with `G' > A` until some later heartbeat after the interval (no timer drives it). Else run the
   assignor over `members ∪ {updatedMember}` and emit target-assignment delta records +
   `TargetAssignmentMetadata(G', now)`; result epoch = G'.
7. Reconcile (GMM:4049-4111): skip if `¬subChanged ∧ state==STABLE ∧ epoch==T`
   (ModernGroupMember:159-161). Else `CurrentAssignmentBuilder` (CAB:187-262, 370-466). If result
   differs, emit `ConsumerGroupCurrentMemberAssignment` and, **only for consumer-protocol
   members** (GMM:4085), schedule `rebalanceTimeout(key, epoch=newEpoch)` if new state is
   UNREVOKED_PARTITIONS else cancel it (GMM:4086-4095). Note STABLE(E)->UNREVOKED keeps epoch E
   (`updateMemberEpoch(memberEpoch)`, CAB:430-432; also CAB:341-342).
8. `scheduleConsumerGroupSessionTimeout(m)` (GMM:2731) — during generation, before commit.
9. Response (GMM:2734-2750): `memberEpoch = updatedMember.epoch`, heartbeat interval; assignment
   included iff `req.epoch == 0` ∨ full request ∨ assigned partitions changed. Released at HWM.

Leave (GMM:4827-4856): no epoch validation at all. Dynamic or static with -1 =>
`consumerGroupFenceMembers` (GMM:5040-5110): tombstones ×3 (GMM:5332-5336), regex tombstones,
`ConsumerGroupMetadata(G+1, hash)`, and if the group becomes empty
`TargetAssignmentMetadata(G+1, ts=0)` (GMM:5100-5103); `cancelTimers` (session, rebalance, join,
sync; GMM:5344-5349) **during generation**. Static -2 (GMM:4927-4946): single current-assignment
record with `epoch=-2`, pendingRevocation={}, assigned epochs reset to 0; **state field unchanged,
no group-epoch bump, no target change, no timer change** (session timer from the last heartbeat
keeps running; rebalance timer stays but becomes a no-op because epoch != scheduled epoch).

TLA+ variables suggested: `groupEpoch, assignEpoch, assignTs, target[m], mEpoch[m], mPrev[m],
mState[m], assigned[m], pending[m], partEpoch[p]` (+ `uncommitted log`, `hwm`),
`sessTimer[m] ∈ {None} ∪ [closure: "fence m"]`, `rebTimer[m] ∈ {None} ∪ [epoch: Nat]`,
and `pendingTimerOp` (timer op enqueued/in-flight, for the retry path).

## 2. Path matrix (who bumps / writes / touches timers)

| Path | group epoch bump | records | timers |
|---|---|---|---|
| New dynamic join (ep 0) | if names/regex/assignor/hash change | member, maybe G, target, current | session sched; rebalance sched/cancel |
| Rejoin ep 0, member exists (19233) | same rule (usually none) | only if something changed | session resched; rebalance sched/cancel only if record changes |
| Static rejoin same id after -2 | same rule | current asg (epoch reset is in-memory until reconcile writes it) | as heartbeat |
| Static replace (new id) | same rule | 3 tombstones + 3 records for new + normal | **old id's timers left running** (GMM:5178) |
| Regular heartbeat | same rule | as needed | session resched; rebalance sched/cancel |
| Leave -1 (consumer) / admin LeaveGroup | always +1 | tombstones, G, maybe target-meta | cancel all 4, during generation |
| Static leave -2 | none | current asg only | none |
| Session/join/sync timeout | always +1 (via fence) | as leave | cancel all 4 |
| Rebalance timeout | +1 iff `m.epoch == scheduledEpoch` | as leave | cancel all 4 |
| Classic join in consumer group (GMM:2763-2972) | names/regex only (**no assignor rule**) | as heartbeat | session+sync scheduled **after commit** (appendFuture, GMM:2949-2961); maybeReconcile never touches rebalance timer |
| Classic sync (GMM:8521-8548) | none | none | after commit: cancel sync, resched session |
| Classic heartbeat (GMM:8761-8791) | none | none | session resched; join timer `scheduleIfAbsent` |
| onLoaded (GMM:6906-6950) | — | — | session for all members; rebalance for every UNREVOKED member (incl. classic) |

## 3. Answers to the priority questions (GMM scope)

**Q1 (overlap during revocation).** In GMM, a partition is handed to a new owner only via
`computeNextAssignment`, which treats a target partition as unreleased while
`currentPartitionEpoch(p) != -1` and p is not pending revocation of *this* member (CAB:404-409).
GMM feeds it `group::currentPartitionEpoch` from the pre-heartbeat state (GMM:2721). Paths that
free partitions without a client acknowledgement: (a) fence (leave/timeouts) — tombstone sets the
member epoch to -1 and clears assigned/pending on replay (GMM:6304-6327); (b) static -2 leave
clears `pendingRevocation` (GMM:4934-4938), freeing those partitions immediately; (c) epoch-0
rejoin of an *existing* member (KAFKA-19233): owned=[] is mandated (GSvc:489-491), so
`ownsRevokedPartitions` is false and all pending revocations are treated as acknowledged
(CAB:218-234). (a)/(b)/(c) are correct only under the client contract "a member that sends
leave/-2/epoch-0 has stopped using its partitions". The coordinator has no way to distinguish a
genuine epoch-0 rejoin from a delayed/duplicated earlier epoch-0 request (see F4). Also note
`addPartitionEpochs` silently overwrites a partition owned at a *lower* epoch (CG:1167-1170) and
`removePartitionEpochs` silently skips mismatches (CG:1128-1134): an overlap would not raise
IllegalStateException, so the TLA+ invariant must be stated over members' `assigned ∪ pending`,
not over `partEpoch`.

**Q3 (responses from uncommitted state).** Success responses are deferred to HWM (§0), including
0-record heartbeats; so a member never acts on an epoch/assignment that is later rolled back.
Notes: (i) error responses are immediate and may reflect uncommitted fences (safe direction); with
KAFKA-19233 the subsequent epoch-0 rejoin restores the member if the fence is later lost on
failover. (ii) On write timeout the client gets an error but the state may commit; the
previous-epoch tolerance (GMM:1686-1692) covers exactly one lost step, and the assignment is only
resent if the client sends a *full* request (GMM:2745-2748) — the client contract is that it does
so after errors. (iii) Non-reverted in-memory side effects of generation: session/rebalance timer
schedule/cancel (GMM:2731, 4086-4095, 5106), regex executor scheduling, and
`metadataRefreshDeadline` (benign because of its epoch guard). The timer side effects are the
real divergence (F2).

**Q4 (timers vs. heartbeats / incarnations).**
- Serialization: timer expiry runs as a write op on the shard thread; a heartbeat that reschedules
  or cancels the key before the queued op runs neutralises it (`tasks.remove(key,this)`, CTI:93).
  Fence-then-heartbeat yields UNKNOWN_MEMBER_ID; heartbeat-then-fence is a no-op timer. No path
  both fences and grants within one op.
- Session, join and sync timers check only "group exists and member id exists"
  (`consumerGroupFenceMemberOperation`, GMM:5406-5427): **no epoch/incarnation check**. Safety for
  incarnations relies entirely on key overwrite/cancel.
- Rebalance timer checks `member.memberEpoch() == scheduledEpoch` (GMM:5569) but not state or
  incarnation. Because STABLE(E)->UNREVOKED keeps epoch E, "epoch == E" does not imply
  "still UNREVOKED since scheduling" (relevant only when a schedule is reverted, F2).
- Fence cancels timers (GMM:5106); rejoin with the same id (the Java client keeps its id for life,
  AbstractMembershipManager:82, and rejoins without backoff after UNKNOWN_MEMBER_ID,
  AbstractHeartbeatRequestManager:451-457) reschedules the session key, so the normal path is safe.
- **Gap: the CTI retry path re-installs an old closure over a newer task (F1)**, and timer
  side effects of reverted batches persist (F2). F1 is a concrete way for a timer of an earlier
  incarnation to fence a later one.

## 4. Findings

### F1. Timed-out session-timeout fence is retried and overrides the new incarnation's session timer — spurious fence of a rejoined member [Model-checkable; Test-verifiable]
Path: session timer T1 for m fires; op runs (removes T1, CTI:93), fences m (GMM:5406-5417),
cancels m's timers (GMM:5106), records appended but HWM is stalled. Client heartbeats, gets
UNKNOWN_MEMBER_ID immediately (from uncommitted state), rejoins with the **same member id** and
epoch 0 -> new member m' (uncommitted) and session task T2 (GMM:2731). After `writeTimeout`
(5 s) the fence op's future fails with `TimeoutException` (CR:1139) — records are not reverted.
CTI's `exceptionally` handler: not RejectedExecution/NotCoordinator => `retry` =>
`schedule(key, 500ms, op)` (CTI:121-124) which `tasks.put`s T3 and **cancels T2** (CTI:138-139).
T3 fires 500 ms later and runs `consumerGroupFenceMemberOperation(groupId, m)` with no incarnation
check, fencing m' (bumping the group epoch again, freeing m''s partitions while m' may be consuming
until its next heartbeat). Only a heartbeat from m' within that 500 ms window prevents it; the
default heartbeat interval is 5 s, and m''s own join response is also stuck behind the HWM.
Same for classic join/sync timeouts (also no epoch check). Rebalance timeouts are protected by the
epoch check (new incarnation's epoch is >= the old target epoch > scheduled epoch).
Compensations checked: `tasks.remove(key,this)` only protects *before* the op runs; the retry
handler's `tasks.remove(key, this)` (CTI:107) is a no-op once T2 replaced it, and `schedule`
then overwrites T2 anyway. On timeout the fence records remain, so the retry is never needed for
the original member; it can only harm a successor. Trigger needs HWM stall > 5 s (e.g. follower
failure before ISR shrink, `replica.lag.time.max.ms`=30 s) coinciding with a session expiry and a
quick rejoin — plausible in production. Impact: liveness/availability (extra fence + rebalance,
a window where the fenced client still consumes partitions just reassigned); violates the
guidance's expected behaviour for Q4 ("a timer scheduled for an earlier incarnation never affects
a later one"). No test covers retry-overrides-newer-task (CoordinatorTimerImplTest:233 only checks
plain retry). History: retry semantics unchanged since 5f0e36825/06f7cb3e5.
Model: CTI as a map key->task with `retryOverwrite`, write timeout as a nondeterministic action
while HWM is stalled, plus client rejoin with the same id.

### F2. Timer side effects of a reverted batch are not rolled back [Model-checkable; Code-review]
Records of all ops in a failed batch are reverted (CR:786-792) but `cancelTimers` executed during
generation of a fence (GMM:5106) and schedule/cancel in `maybeReconcile` (GMM:4086-4095) are not.
Consequences: (a) leave/-1 or admin removal reverted -> member restored with **no session timer**;
a client that already left will never heartbeat, so it stays forever holding its assignment
(group never converges) until coordinator reload. (b) A reverted STABLE(E)->UNREVOKED(E)
transition leaves `rebalanceTimer(E)`; if the target computation was reverted too and nobody
re-drives it, the member is STABLE at E and reconciled (no further maybeReconcile change to
cancel), and the stale timer fences a healthy member (epoch check passes). `replaceMember`'s
comment (GMM:5178-5179) shows the authors know about revert hazards, yet fence paths cancel
eagerly. Triggerability is low: most append failures (NotLeaderOrFollower, storage errors, offset
mismatch -> FAILED/LOADING CR:719-733) are followed by unload/reload, whose `cancelAll` +
`onLoaded` recreates a consistent timer set. Remaining triggers: RecordTooLarge/unexpected append
errors, replay exceptions (CR:1020-1030).

### F3. Classic vs consumer path inconsistencies [Code-review-only]
(a) Consumer heartbeat schedules timers at generation time; classic join/sync schedule them only
after commit (GMM:2949-2961, 8537-8545). (b) Classic join's epoch-bump rule omits
`preferredServerAssignorChanged` (GMM:2866 vs 2677-2687). (c) `maybeReconcile` never schedules or
cancels rebalance timers for classic members (GMM:4085), but `onLoaded` schedules one for every
UNREVOKED member including classic ones (GMM:6937-6944) and does not restore join/sync timers;
after failover a classic member gets a different timeout regime (harmless because of the epoch
check, but the model should not assume symmetry). (d) A consumer heartbeat carrying a classic
member's id converts it in place (`setClassicMemberMetadata(null)`, GMM:2649) while its
join/sync timers (no epoch check) remain — requires a misbehaving client.

### F4. Epoch-0 is an unconditional "I released everything" signal (post KAFKA-19233) [Model-checkable]
Before 384586e52, epoch 0 for a member with epoch>0 was rejected unless it matched prevEpoch.
Now any epoch-0 request (GMM:1680) on an UNREVOKED member acknowledges all revocations
(owned=[] is mandatory). A delayed/duplicated epoch-0 request arriving after the member advanced
would let `computeNextAssignment` release partitions the live client still owns -> overlap (Q1).
Similarly a delayed leave (-1, no epoch check, GMM:4838-4841) fences a later incarnation.
Real triggerability is low (per-connection in-order processing; a client sends epoch 0 only after
dropping partitions), but the guidance asks to consider reordered/duplicated heartbeats, so a
network model with reordering will hit it. Classify any resulting counterexample carefully as a
client-contract assumption, not necessarily a coordinator bug.

### F5. Rebalance timeout is reset by every record change while UNREVOKED [Code-review-only]
`scheduleConsumerGroupRebalanceTimeout` is called whenever the reconciled member differs and is
UNREVOKED (GMM:4086-4092), and `schedule` replaces the deadline. Partial revocations, new targets,
or subscription changes (CAB:219-223) keep pushing the deadline; the epoch stays E. A slow or
subscription-toggling client can delay fencing beyond `rebalanceTimeoutMs`. Liveness only.

### F6. In-memory-only epoch edge cases [Test-verifiable, low]
(a) Static -2 rejoin sets epoch 0 only in memory (GMM:3492-3495); it is persisted only if
`maybeReconcile` writes. If the member is already "reconciled" (target epoch 0, i.e. a group
whose members subscribe to nothing so the group epoch never left 0) no record is written; the
response says epoch 0 while the log keeps -2, and the instance id remains "released"
(GMM:1738-1746), so another process can replace a live static member. (b) Static -2 leave keeps
the `state` field (e.g. UNREVOKED with empty pending set). Both degenerate; useful as harness
edge tests, not as model targets.

### Checked and found safe (with compensating mechanism)
- Queued timer vs heartbeat ordering: CTI:93 + reschedule/cancel (GMM:2731, 4094, 5106).
- Static replacement leaving old timers: old id can't come back (client ids are random per
  consumer; a same-id rejoin requires owning the instance id, GMM:3485), timers find no member.
- Conversion classic->consumer schedules session timers at generation; on failure they hit a
  group-type check (GMM:1489-1494).
- `metadataRefreshDeadline` non-timeline write: guarded by `groupEpoch() < deadline.epoch`.
- Assignment interval: group can sit at `G > A`; members reconcile toward the old target and
  report epoch A; recomputation happens on the next heartbeat after the interval (no timer).
  In-model: allow `assignEpoch < groupEpoch` as a stable state.
- Fence of the last member sets `assignEpoch = G+1, ts = 0` so the next join is not delayed.

## 5. Modeling recommendations
1. Model CTI faithfully: `tasks[key]`, enqueue-on-expiry, `tasks.remove(key,this)` check, and the
   failure handler that overwrites the key on retry. Make write timeout (no revert) and batch
   failure (revert, timers untouched) separate nondeterministic outcomes.
2. Member incarnations: same member id reused across fence/rejoin (client keeps the id); add a
   ghost `incarnation[m]` counter and an invariant: a fence produced by a timer scheduled for
   incarnation i never removes incarnation j > i (F1, F2).
3. Rebalance timer stores the epoch only; invariant "rebTimer fires on m ⇒ m was UNREVOKED at that
   epoch continuously since scheduling" (catches F2b).
4. Ownership invariant over `assigned ∪ pending` per member (not `partEpoch`); include client-side
   "consuming" sets to expose fence windows (F1) and reordered epoch-0/leave messages (F4) as an
   optional network mode.
5. Keep assignment-interval batching as a nondeterministic "can compute now" flag.
6. Leave -2 does not bump G and keeps target; static rejoin resets epoch to 0 and reconciles.
