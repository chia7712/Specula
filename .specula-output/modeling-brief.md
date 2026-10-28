# Modeling Brief: Apache Kafka KIP-848 Consumer Group Rebalance

Target: `./kafka` trunk @ `1224f3d4b`. Abbreviations: GMM = `group-coordinator/.../group/GroupMetadataManager.java`,
CG = `.../modern/consumer/ConsumerGroup.java`, CAB = `.../modern/consumer/CurrentAssignmentBuilder.java`,
CGM = `.../modern/consumer/ConsumerGroupMember.java`, CR = `coordinator-common/.../runtime/CoordinatorRuntime.java`,
CTI = `.../runtime/CoordinatorTimerImpl.java`, CLI = `.../runtime/CoordinatorLoaderImpl.java`.

## 1. System Overview

- Apache Kafka group coordinator (Java). Core logic is about 15k LOC across GMM, CR, CG, CAB, CGM and CTI.
- **Category A (Distributed / Message-Passing).** Its state is a replicated log (`__consumer_offsets` partition) that is replayed into in-memory timeline state. The main hazards are leader failover, truncation, commit gating by HWM, crashes and reloads, timers, and client retries. These are protocol and crash-window concerns, not lock-free memory concerns.
- Protocol: the KIP-848 incremental rebalance, plus KIP-1251 assignment epochs.
  - The group epoch is bumped by subscription or metadata changes.
  - The target assignment (assignment epoch) is computed synchronously in the heartbeat. Computation is gated by an assignment interval (batching, GMM:4354-4366). Offload is *not* wired for consumer groups.
  - Members reconcile through CAB through STABLE, UNREVOKED_PARTITIONS and UNRELEASED_PARTITIONS.
  - Since KAFKA-19974, the member epoch does not change while partitions are pending revocation, and jumps straight to the target epoch on exit.
- Concurrency model:
  - Each shard (partition) is serialized through `EventAccumulator` key locking (verified, CR:493-510). Timer expiry and executor completions are queued write events.
  - Only HWM and linger events jump to the front of the queue.
  - The loader runs on its own single thread, outside the event queue (CLI:77).
  - Every op follows the same pipeline: generate records → replay into timeline state → append to batch → flush to log → defer the response until HWM ≥ offset.
  - Failure reverts timeline state only. **Timers are not timeline state.**

## 2. Scenarios

### Scenario 1: Coordinator load/unload lifecycle is not fenced by leader epoch
**Mechanism**: runtime callbacks and queued events are matched to the *current* context by partition alone. They are never matched by context incarnation or leader epoch, so work from an earlier tenure can act on a later one.
**Evidence**:
- Historical: KAFKA-20634 `d559af58d` (HWM after the leader moved completed truncated writes, Crit); KAFKA-20845 `475e0a1fb` (state reverted while records were written); KAFKA-16030, KAFKA-16118, KAFKA-15910, KAFKA-19716, KAFKA-19862 (load path fixes); KAFKA-16973 (caught-up condition).
- Code:
  - CR:583-612 `CompleteLoad` does `coordinators.get(tp)` and checks only `state==LOADING`, with no epoch or identity check.
  - Unload removes the context (CR:2436). Reload creates a new one (CR:2364).
  - The loader is a FIFO, so an old load can complete into a new context.
  - Appends carry no expected leader epoch, so a stale ACTIVE context can append after re-election (deep-runtime §5 F2).
  - SC:140 can throw before the out-of-sync check.
  - The HWM listener is registered only on ACTIVE and gets no initial value (CR:552-556).
**Affected code paths**: `CoordinatorContext.load/unload/transitionTo`, `scheduleLoadOperation`, `scheduleUnloadOperation`, `CoordinatorWriteEvent.run`, `flushCurrentBatch`, `onHighWatermarkUpdated`.
**Suggested modeling approach**:
- Variables: per broker, `ctx` = [state, epoch, id], `log`, `hwm`, `queue` (a sequence with front-insert), `loaderJobs` (FIFO of [job, ctxId]), `dq` (deferred responses), `listener`.
- Actions: `LeaderChange(e)` (truncate followers, enqueue Unload/Load), `LoadStart`, `LoaderRun` (replays the log into the job's shard, success or failure), `CompleteLoad` (encode the code's check literally), `Unload`, `Append`, `Replicate`, `AdvanceHWM`, `HWMDeliver`.
- Granularity: keep Append, Replicate, AdvanceHWM and HWMDeliver as separate actions. Use 2 brokers and 3 leader epochs (e1→e2→e3, back on the same broker).
**Priority**: High.
**Rationale**: this is the most bug-dense historical area, two independent analyses found the CompleteLoad gap, and it answers guidance Q2 and Q3 directly.

### Scenario 2: Non-timeline side effects (timers) diverge from durable/reverted state
**Mechanism**: timers are scheduled or cancelled while records are generated. They are never rolled back when the batch is reverted, and they are retried by key without incarnation checks. Timer-driven fences validate too little: the session timer checks nothing, and the rebalance timer checks only the epoch.
**Evidence**:
- Historical: KAFKA-16663 (write-timeout timer cancel), 9bc48af1c (timer type checks), cad46800c (timer thread safety), KAFKA-19857 (executor cancelAll), KAFKA-21303 (classic session timeouts on reload), KAFKA-19974 (makes epoch-only guards weaker).
- Code:
  - CTI:121-124 re-schedules with `schedule(key, …)` after any non-NotCoordinator failure, for example a write timeout (CR:1138). That replaces the *new* incarnation's session timer.
  - GMM:5406-5427 fences by memberId alone.
  - GMM:5105 cancels timers before the fence records commit.
  - GMM:4087-4095 schedules the rebalance timer in memory.
  - GMM:5569 guards the rebalance fence with `memberEpoch==scheduled` only, without checking member state.
  - CR:783-792, 1115 revert timeline state only.
  - The comment at GMM:5174-5175 acknowledges that timer changes are not reverted.
**Affected code paths**: `scheduleConsumerGroupSessionTimeout`, `scheduleConsumerGroupRebalanceTimeout`, `consumerGroupFenceMemberOperation`, `consumerGroupFenceMembers`, `maybeReconcile`, `CoordinatorTimerImpl.schedule`, `failCurrentBatch`, `OperationTimeout`.
**Suggested modeling approach**:
- Variables: `timers` = set of [kind ∈ {session, rebalance}, member, epoch, taskId], stored outside the reverted state. The client keeps a stable `memberId` across rejoins, as the Java client does.
- Actions: `TimerFire`, `TimerRetry` (after `OpTimeout`/`FlushFail`, re-put under the same key), `FlushFail` (revert the group state, keep `timers`), and `OpTimeout` (the response fails, the state stays).
- Granularity: model a timer firing as a queued write that runs later.
**Priority**: High.
**Rationale**: concrete verified mechanisms. The consequences are a spurious fence of a healthy or rejoined member, or a member that is never expired (guidance Q4).

### Scenario 3: Partition ownership under epoch rules, client retries and fencing
**Mechanism**: exclusive ownership is an inductive property enforced by a single gate (CAB:404-410, `currentPartitionEpoch == -1` or the member's own pending partition). The epoch-validation shortcuts can feed that gate requests the server cannot tie to an incarnation: epoch 0 is always accepted, leave at -1 is unchecked, and the previous epoch is accepted with `owned ⊆ assigned`.
**Evidence**:
- Historical: KAFKA-19974 (two owners at the same epoch), KAFKA-19449, KAFKA-19233, b8692f315, KAFKA-16147 and 16249 (pre-2025), KIP-1251 [1-3/N], KAFKA-20442.
- Code:
  - CG:1120-1180: `addPartitionEpochs` overwrites a lower epoch and `removePartitionEpochs` skips mismatches (relaxed in KAFKA-19862). The map no longer detects overlap.
  - GMM:1673-1694 is the epoch validation.
  - GMM:1680: epoch 0 on an UNREVOKED member that reports owned={} acknowledges all pending revocations.
  - GMM:4838-4841: dynamic leave has no epoch check.
  - Static leave (-2) resets assignment epochs to 0 and drops the pending set (GMM:4927-4944).
**Affected code paths**: `consumerGroupHeartbeat`, `throwIfConsumerGroupMemberEpochIsInvalid`, CAB `build/computeNextAssignment/updateCurrentAssignment`, `consumerGroupLeave`, `consumerGroupStaticMemberGroupLeave`, CG `updateMember`, `add/removePartitionEpochs`.
**Suggested modeling approach**:
- Variables: `groupEpoch`, `assignEpoch`, `target[m]`, and per member `mEpoch`, `mPrev`, `mState`, `assigned[m]` (partition ↦ assignment epoch) and `pending[m]`. Keep `pEpoch` *derived*, with the code's overwrite and skip encoded literally. On the client side, model `clientOwned[m]` and a `net` bag of requests that can be duplicated or reordered.
- Actions: `Heartbeat(m, req)`, encoding the CAB transition table from deep-reconciliation §3 directly. Also `ClientRevoke`, `LoseResponse`, `Leave`, `StaticLeave`, `StaticRejoin`, `Rejoin(epoch 0)`, `ComputeTarget` (a nondeterministic disjoint assignor, gated by the interval) and `SubscriptionChange`.
**Priority**: High (core safety property, Q1).
**Rationale**: the code paths that change reconciliation semantics in 2025-26 are the densest in the history, and the internal safety net was weakened.

### Scenario 4: Subscription / regex changes that never trigger a target recompute
**Mechanism**: a group-epoch bump is the only trigger for recomputing the target assignment. Several transitions change a member's effective subscription without a bump, and the periodic refresh bumps only when the metadata hash changes.
**Evidence**:
- Historical: KAFKA-19891 (regex non-empty→empty, missing bump), KAFKA-18961 (time-based refresh), KAFKA-19720, KAFKA-17592/17593, KAFKA-20434 (assignor change missing bump), KAFKA-19546.
- Code:
  - GMM:2677-2687: no bump while the regex is unresolved.
  - GMM:3903-3907: a resolution of `{}` compares equal to EMPTY, so there is no bump.
  - `updateSubscriptionMetadata` bumps only on a hash change.
  - CAB:483-500 filters the target by the resolved subscription, so the member gives the partitions back but the target still holds them for it. Those partitions are unowned until an unrelated bump.
  - Batching: a classic heartbeat (GMM:8761-8792) does not compute a pending assignment, so a classic-only group can stay in ASSIGNING.
**Affected code paths**: `maybeUpdateRegularExpressions`, the regex resolution completion (~GMM:3870-3930), `updateSubscriptionMetadata`, `maybeUpdateTargetAssignment`, `canComputeNextTargetAssignment`, CAB `subscribedTopicIds`.
**Suggested modeling approach**:
- Variables: `regex[m]`, `resolved[r]` (with an "unresolved" marker), `metadataHash`, `assignmentTimestamp`.
- Actions: `ChangeRegex(m)`, `ResolveRegex(r)` (async: scheduled, then completed as a later write), `MetadataRefresh`, `TopicCreateDelete`.
- Check liveness under weak fairness on heartbeats and resolution.
**Priority**: Medium.
**Rationale**: verified liveness gaps (Q5), but no safety violation.

### Scenario 5 (secondary): Migration and static membership edge cases
**Mechanism**: conversions between the classic and consumer protocols, and static identity handoff, copy state from the other representation without checking its phase.
**Evidence**:
- Historical: KAFKA-16436/16554, 17733, 18150, 19546, 20273, 20640, 20662, 21303; static: 17228, 17317, 20601, 20747, 3e5df2f08.
- Code:
  - Upgrade takes each member's assignment from `member.assignment()` (CG:1217-1283) even while a classic rebalance has cleared assignments (GMM:7957-7958). The upgrade check ignores group state (GMM:1408-1430). Not re-verified by the lead analyst.
  - Static leave at -2 leaves the member UNREVOKED. `onLoaded` then arms a rebalance timer tagged -2, which fences the member after a reload (GMM:6937, 5568).
**Priority**: Low for TLA+; route to tests (§6.2).

## 3. Modeling Recommendations

### 3.1 Model
- **Runtime lifecycle with leader epochs** (Scenario 1): the per-shard state machine from deep-runtime §7, with an explicit loader process and queue, failover with truncation, and HWM delivery gated by the listener.
- **Timers as non-reverted state** (Scenario 2): timers keyed by (kind, member), retried after failure, with the fence operation encoded exactly as in the code (no epoch check for session, epoch-only for rebalance).
- **KIP-848 reconciliation** (Scenario 3): the CAB transition table, the epoch validation in GMM:1673-1694, previous-epoch tolerance, epoch 0 and -1 semantics, static leave, and the client's owned set with duplicated or reordered requests.
- **Group epoch / assignment epoch / batching** (Scenarios 3 and 4): `groupEpoch > assignEpoch` may persist (the interval gate), and the assignment epoch is set to the group epoch when the group empties (KAFKA-20442).
- **Regex as async resolution** (Scenario 4), as a separate small configuration.
- Suggested build order: (a) a runtime-only spec with opaque ops, for Scenario 1 and the Scenario 2 retry path; (b) add the group abstraction for Scenarios 2 and 3; (c) add the regex extension.

### 3.2 Do Not Model
- **Assignor internals** (uniform/range/uniform2): replace with a nondeterministic disjoint assignment over subscribed partitions. Optionally allow an overlapping variant to test that CAB does not depend on disjointness (deep-reconciliation N4).
- **Assignor offload / `TargetAssignmentRecordsBuilder` static remapping**: not wired for consumer groups (GMM:9915 has no callers).
- **Share and streams groups, offset commit semantics** (out of scope). The equal-epoch commit bypass goes to §6.3.
- **Network layer, KRaft and replication internals**: abstract to leader, leader epoch, follower truncation and HWM.
- **Transactions**: no consumer-group records are transactional. They matter only as a cause of batch failure, which FlushFail models.
- **Byte-level batching, compression, linger**: use one open batch of at most 2 ops, with a nondeterministic Flush.
- **Metrics, logging, configs, describe APIs.**
- **Compaction**: optional `Compact` action only if the Scenario 1 spec closes quickly; three analyses argue compaction preserves replay equivalence.

## 4. Proposed Extensions

| Extension | Variables | Purpose | Scenario |
|---|---|---|---|
| LeaderEpochFailover | leader, leaderEpoch, log[b], hwm | truncation, re-election on the same broker | 1 |
| ContextIncarnation | ctx[b].id, ctx[b].epoch, loaderJobs | stale CompleteLoad, stale ACTIVE append | 1 |
| DeferredResponses | dq, responses[r].status, listener | commit gating; failed-but-committed responses | 1, 3 |
| NonTimelineTimers | timers, taskId | timers not reverted, retry by key | 2 |
| BatchFailure | batch, snapshots, FlushFail, OpTimeout | revert without unload; timeouts | 2 |
| ClientIncarnation | memberId reuse, clientOwned, net (dup/reorder) | epoch 0 and -1 ambiguity, rejoin with the same id | 2, 3 |
| AssignmentInterval | assignEpoch < groupEpoch persisted | batching lag | 3, 4 |
| AsyncRegex | regex[m], resolved[r], metadataHash | missing epoch bumps | 4 |

## 5. Proposed Invariants

| Invariant | Type | Description | Targets |
|---|---|---|---|
| PartitionOwnershipExclusive | Safety | ∀m≠n live: (assigned∪pending)[m] ∩ (assigned∪pending)[n] = {} (on member records, not pEpoch) | Q1, Scenario 3 |
| ClientOwnershipExclusive | Safety | non-fenced clients' `clientOwned` sets are pairwise disjoint (fenced zombies tracked separately) | Q1, MC-5 |
| PartitionEpochMapConsistent | Safety | pEpoch[p] = epoch of the unique owner, else -1 | Scenario 3 (detects silent overwrite) |
| MemberEpochBounds | Safety | mEpoch ≤ assignEpoch ≤ groupEpoch; mEpoch' ≥ mEpoch unless reset to 0/-1/-2 | Scenario 3 |
| NoActiveWhileLoading | Safety | ctx ACTIVE ⇒ no pending loader job targets that ctx id | MC-1 |
| AppendFromLogState | Safety | every appended entry was generated from Replay(the log prefix it is appended after) | MC-1, MC-2, Q2 |
| ReloadEquivalence | Safety | after reload, the replayed state is reachable and consistent with every `ok` response | Q2, Q3 |
| AckedImpliesCommitted | Safety | a successful response's records are in the committed prefix | Q3 |
| TimerIncarnation / NoSpuriousFence | Safety | a fence fires only against the incarnation (and, for rebalance, the UNREVOKED state) it was scheduled for | MC-3, MC-4 |
| TimersCoverMembers | Safety | in an ACTIVE ctx every member has a session timer | MC-4 |
| CoordinatorEventuallyActive | Liveness | the leader eventually has an ACTIVE ctx (no stuck FAILED) | MC-1b |
| GroupConverges | Liveness | stable metadata and subscriptions ~> ∀m: STABLE ∧ mEpoch = assignEpoch ∧ assigned[m] = target[m] ∩ Sub(m), and every subscribed partition is assigned | MC-6, Q5 |

## 6. Findings Pending Verification

### 6.1 Model-Checkable
| ID | Description | Expected invariant violation | Scenario |
|---|---|---|---|
| MC-1 | On a leader flap e1→e2→e3 on the same broker during a long load, can the e1 load completion activate the e3 context while the e3 load is still replaying? Records generated from partial state would then be appended to the real log (group epoch regression, double ownership after reload). Variant: a failed e1 completion leaves the e3 context FAILED with no recovery until the next epoch. | NoActiveWhileLoading, AppendFromLogState, PartitionOwnershipExclusive after reload; CoordinatorEventuallyActive | 1 |
| MC-2 | Can events queued before Unload(e2) run on the stale ACTIVE context after the broker leads again at e3, appending records computed from a state that diverges from the truncated log? Is "written but reverted with ctx still ACTIVE" (SC:140 before the out-of-sync check) reachable? | AppendFromLogState, ReloadEquivalence | 1 |
| MC-3 | After a session-timeout fence's write times out (HWM stalled) or fails, the member rejoins with the same memberId. Does the timer retry replace the new incarnation's session timer and fence it 500 ms later? Join and sync timers have the same structure. | TimerIncarnation / NoSpuriousFence | 2 |
| MC-4 | When a batch is reverted without an unload (another op's failure, InvalidProducerEpoch, RecordTooLarge): (a) can a leftover rebalance timer for epoch E fence a member that is STABLE at E? (b) can a reverted leave or fence leave a member with no session timer forever? | NoSpuriousFence, TimersCoverMembers | 2 |
| MC-5 | With duplicated or reordered client requests (epoch-0 rejoin, -1 leave, previous-epoch retries) and same-id rejoin, can a partition be in two non-fenced clients' owned sets, or can a stale request fence a newer incarnation? | ClientOwnershipExclusive, NoSpuriousFence | 3 |
| MC-6 | Does the group converge when a member switches regex A→B and B resolves to no topics while others still subscribe via A, or when a classic-only group has an assignment delayed by the interval? | GroupConverges | 4 |

### 6.2 Test-Verifiable
| ID | Description | Suggested test approach |
|---|---|---|
| T-1 | Stale CompleteLoad (MC-1) at unit level | CoordinatorRuntimeTest with a loader that returns a manually completed future: load(e1), unload(e2), load(e3), complete the first future, assert ctx not ACTIVE / not FAILED |
| T-2 | Timer retry fences a rejoined incarnation (MC-3) | GroupMetadataManagerTest/CoordinatorTimerImpl test: fail the fence write with TimeoutException, rejoin the same memberId, advance 500 ms |
| T-3 | HWM listener registered without the current HWM; no-record heartbeats park until an unrelated write or timeout | CoordinatorRuntimeTest: HWM reaches the log end before ACTIVE, then a heartbeat that writes no records |
| T-4 | Upgrade classic→consumer while the classic group is in PREPARING/COMPLETING_REBALANCE with cleared assignments (M1): a new member is granted partitions a cooperative classic member still consumes | GroupMetadataManagerTest; then a real cooperative-sticky consumer plus a KIP-848 consumer |
| T-5 | Static leave (-2) followed by coordinator reload: `onLoaded` arms a rebalance timer tagged -2 and fences early when rebalanceTimeout < sessionTimeout | GroupMetadataManagerTest replay plus onLoaded |
| T-6 | Partial (non-full) previous-epoch retry receives the new epoch but no assignment; a static rejoin's epoch 0 is held only in memory | GroupMetadataManagerTest |

### 6.3 Code-Review-Only
| ID | Description | Suggested action |
|---|---|---|
| CR-1 | `addPartitionEpochs` silently overwrites a lower epoch and `removePartitionEpochs` silently skips mismatches on the live path (CG:1120-1180), so a future overlap would not be detected | Restrict the leniency to load, or log at WARN |
| CR-2 | Equal-epoch offset commit skips the per-partition check (CG:682); static leave resets assignment epochs to 0 | Review against KIP-1251 intent |
| CR-3 | Every reconcile change while UNREVOKED re-arms the full rebalance timeout (GMM:4086-4092), so the deadline can be pushed out indefinitely | Keep the original deadline |
| CR-4 | `onLoaded` arms new-protocol rebalance timers for classic members (live path does not) and does not restore join/sync timers | Align with the live path |
| CR-5 | Strict tombstone-order checks (GMM:6058-6065, 6176-6188) fail the whole partition load on violation | Confirm unreachable under compaction or relax |
| CR-6 | `updateMemberEpoch(same)` overwrites `previousMemberEpoch` (CGM:105-110), so recovery tolerates only one lost response | Confirm intent |
| CR-7 | Regex refresh gate uses the previous coordinator's wall-clock time (CG:493); one member's ACLs filter the whole group's resolution; unsynchronized image read in the resolver | Review |
| CR-8 | Dynamic leave carries no incarnation information, so a duplicated leave can fence a rejoined member (GMM:4838-4841) | Consider a protocol-level fix (overlaps MC-5) |

## 7. Reference Pointers
- Full audit trail: `.specula-output/analysis-report.md`. Detailed work files: `.specula-output/analysis-work/`:
  - `deep-runtime.md` §7 (runtime state machine and actions)
  - `deep-reconciliation.md` §3 (CAB transition table) and §7 (invariant definitions)
  - `deep-heartbeat-timers.md` §1-2 (heartbeat action and path matrix)
  - `deep-replay-regex-migration.md` §5 (record → in-memory field table)
  - `archaeology-*.md`
- Key source: CR:540-640 (transitions, load, unload), CR:700-810 (flush, revert), CR:1121-1140 (OperationTimeout), CR:2355-2440 (load/unload ops); CTI:80-140; GMM:1673-1694 (epoch validation), GMM:2589-2760 (heartbeat), GMM:3756-3930 (regex), GMM:4038-4200 (maybeReconcile), GMM:4345-4430 (target), GMM:4827-4945 (leave), GMM:5040-5110 (fence), GMM:5376-5700 (timers), GMM:5812-6330 (replay), GMM:6906-6980 (onLoaded); CAB:187-500; CG:318-361, 963-979, 1090-1180.
- Historical commits (reference only, already fixed): d559af58d KAFKA-20634, 475e0a1fb KAFKA-20845, 8ff7addce KAFKA-19974, 384586e52 KAFKA-19233, 83dc0d7ea KAFKA-19862, 02bd3420d KAFKA-19449, 82241e187 KAFKA-19891, ee550c4b7 KAFKA-16973, be17df6fd KAFKA-16374.
- Reference: KIP-848, KIP-1251. JIRA was unreachable from this environment, so issue discussions were not consulted.
