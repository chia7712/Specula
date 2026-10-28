# Deep analysis: replay / regex / migration (KIP-848 consumer groups)

Scope: guidance Q2 (replay after coordinator move) and Q5 (regex), plus migration and
static membership. Source: `kafka/` trunk at HEAD `1224f3d4b`.
Abbreviations: GMM = `group-coordinator/.../group/GroupMetadataManager.java`,
CG = `.../modern/consumer/ConsumerGroup.java`, MG = `.../modern/ModernGroup.java`,
CGM = `.../modern/consumer/ConsumerGroupMember.java`, CAB = `.../modern/consumer/CurrentAssignmentBuilder.java`,
RT = `coordinator-common/.../runtime/CoordinatorRuntime.java`, LDR = `.../runtime/CoordinatorLoaderImpl.java`,
EXE = `.../runtime/CoordinatorExecutorImpl.java`.

## 0. Summary of verdicts

| Id | Finding | Class | Severity guess |
|----|---------|-------|----------------|
| R1 | Atomicity holds for all consumer-group heartbeat/leave/fence/upgrade/downgrade writes (single batch); only `cleanupGroupMetadata` and `onTopicsDeleted` are non-atomic and can commit a prefix | Model-checkable (as a design fact) | none (benign prefix) |
| R2 | Reload schedules a rebalance timer for a static member that left with epoch -2 (state left at UNREVOKED_PARTITIONS); live path never arms one at epoch -2 | Model/Test-verifiable | low (divergence; early fence only if rebalanceTimeout < sessionTimeout) |
| R3 | Reload arms a consumer-protocol rebalance timer for classic-protocol members in UNREVOKED_PARTITIONS; live path never does (GMM:4085) | Test-verifiable | low (divergence) |
| R4 | Load replays up to LOG_END (above HW); fine by design but a model must allow "loaded state includes uncommitted tail" | Model-checkable | design fact |
| R5 | Replay `addPartitionEpochs` throws on overlap (CG:1158-1180); any overlap bug becomes an un-loadable partition (whole shard), not a per-group error | Model-checkable (invariant) | amplifier |
| R6 | Atomic write larger than max.message.bytes (after compression) fails forever, so the heartbeat that must write it fails forever | Test-verifiable | low/liveness |
| X1 | Regex: target assignment can transiently hold topics a member no longer matches; CAB filters them so current assignment never does; convergence relies on heartbeats + 10s batching | Model-checkable | none (safe) |
| X2 | Regex refresh gating uses persisted wall-clock timestamp of *another broker* (CG:493); clock skew after failover delays resolution of NEW regexes by skew+10s | Code-review-only | low |
| X3 | Regex resolution filtered by the ACLs of the member whose heartbeat triggered it, applied to the whole group | Code-review-only | design (KAFKA-18813) |
| X4 | Executor lambda reads non-volatile `metadataImage` field from another thread (GMM:537, 3854) | Code-review-only | benign (version self-reported) |
| M1 | **Upgrade classic→consumer uses `member.assignment()` only; after COMPLETING_REBALANCE→PREPARING_REBALANCE the classic assignments are reset to EMPTY (GMM:7957-7958, 8067) while cooperative clients still own partitions → converted group sees them as free and can hand them to a new consumer-protocol member immediately** | Test-verifiable | **medium candidate (ownership overlap, Q1)** |
| M2 | Static replacement with a classic joiner (or classic previous member) bypasses the "previous member must have left" check (GMM:3514-3518) | Code-review-only | by design (classic semantics) |

---

## 1. Write atomicity and record ordering (Q2 "can a prefix be committed?")

**Runtime mechanics.** `CoordinatorResult` defaults to `isAtomic=true` (CoordinatorResult.java:63-66, 88-94).
In `append()` (RT:906-1050) an atomic write first flushes the current batch if the new records would not fit
(RT:955-978), then appends *all* records of the op into one `MemoryRecordsBuilder`; the batch is the
unit appended by `partitionWriter.append` (RT:711-716), and a Kafka record batch is replicated and
committed all-or-nothing. Records are *replayed into the timeline state as they are appended*
(RT:1013-1020); on replay exception the whole current batch is failed and the snapshot reverted
(RT:1025-1036, `failCurrentBatch` RT:785-791). Flush failure also reverts to the batch base offset (RT:742-765).
Non-atomic writes check room per record and may flush mid-op (RT:984-1003), so a **prefix can commit**.

**Who is non-atomic?** grep of `CoordinatorResult<>(records, false)`:
- `GroupCoordinatorShard.cleanupGroupMetadata` (GroupCoordinatorShard.java:1136-1160): offset tombstones +
  `maybeDeleteGroup` tombstones for *many* groups.
- `GroupCoordinatorShard.onTopicsDeleted` (:1176-1192): offsets only.
- Everything on the consumer-group path is atomic: heartbeat (GMM:2750), fence (GMM:5108), static leave
  (GMM:4939), regex result (GMM:3956), downgrade (GMM:5060, `replayRecords=false` but atomic),
  classic join into consumer group (GMM:2968, `replayRecords=!downgrade`).
- Note the 4-arg constructor `(records, response, appendFuture, replayRecords)` is *atomic*; `false` there is
  `replayRecords`, not `isAtomic` (CoordinatorResult.java:102-120).

**Consequence of a prefix of `cleanupGroupMetadata`.** It only deletes *empty* groups (GMM:9758-9763). For a
consumer group the tombstone order is CA*, TA*, TA-metadata, MM*, regex*, subscription-metadata, epoch
(CG:755-775). An empty group has no member records, so a prefix commits e.g. TA-metadata tombstone
(assignmentEpoch := -1, GMM:6280-6286) and some regex tombstones but not the epoch tombstone. Reloaded state:
empty group, groupEpoch=N, assignmentEpoch=-1. A joining member bumps to N+1 and computes a fresh target
(`assignmentEpoch >= groupEpoch` false, GMM:4354) — no invariant broken; next cleanup cycle completes the
deletion. Benign. (The group tombstone check `assignmentEpoch != -1 → throw`, GMM:6187, is satisfied by order.)

**Atomic size limit (R6).** An atomic write whose *compressed* size exceeds `max.message.bytes` gets
`RecordTooLargeException` at flush (RT:751-756) and the op fails and is reverted. The heartbeat that would
emit a large target assignment (TargetAssignmentRecordsBuilder.java:296-331: one record per changed
member + metadata) will fail identically on every retry → group cannot advance its target epoch. Requires
very large groups; Test-verifiable; outside TLA+ abstraction except as "write may fail".

**Emission order inside one op (heartbeat, GMM:2600-2750).**
1. (upgrade only) classic GroupMetadata tombstone + `createConsumerGroupRecords` (CG:1290-1312: MM*, epoch, TA*, TA-meta, CA*).
2. static replacement `replaceMember` (GMM:5170-5205): CA-tomb(old), TA-tomb(old), MM-tomb(old), MM(new), TA(new := old target), CA(new).
3. MM(updated member) if changed (`hasMemberSubscriptionChanged`).
4. regex tombstone if last subscriber left the old regex (GMM:3778-3784).
5. epoch record (+ legacy subscription-metadata tombstone) from `updateSubscriptionMetadata` (GMM:4280-4294).
6. TA records for changed members + TA-metadata (TargetAssignmentRecordsBuilder.java:296-331).
7. CA(updated member) from `maybeReconcile` (GMM:4073-4074).
Leave/fence (GMM:5040-5108): per member CA-tomb, TA-tomb, MM-tomb (GMM:5332-5336), regex tombstones,
epoch(N+1, hash), and TA-metadata(N+1, ts=0) if the group becomes empty (GMM:5099-5102).
Static temporary leave: a single CA record with epoch -2 (GMM:4927-4944).
Note the fence path does *not* recompute the target: survivors still have targets that exclude the departed
member's partitions until the next heartbeat computes epoch N+1.

## 2. Replay: what it accepts that live never produces, and derived-state fidelity

### 2.1 Lenient handling (KAFKA-17306, commit 31f79055c)
- Tombstone for a missing group or member → no-op (GMM:6037-6041, 6050-6056, 6170-6174, 6241-6245,
  6270-6274, 6307-6318, 6347-6351). Rationale (commit msg): after compaction a partition may contain only
  tombstones for a group/member.
- Any record may create the group (`getOrMaybeCreatePersistedConsumerGroup(..., true)`, GMM:1044-1071),
  including replacing a *simple* classic group created by offset-commit replay (GMM:1060-1067). A non-simple
  classic group with the same id → `IllegalStateException` (GMM:1069) → load failure.
- A CA record for a member with no MM record creates a member with default subscription
  (`getOrMaybeCreateMember(memberId, true)` CG:255-268 + GMM:6300-6305). Live never produces a CA before MM
  for a new member, but compaction can order a member's latest CA before its latest MM.

### 2.2 Strict checks still present (these run live too, because append replays)
- MM tombstone requires the CA tombstone already replayed (`memberEpoch == -1`, GMM:6058-6061) and the TA
  tombstone already replayed (GMM:6062-6065).
- TA-metadata tombstone requires empty target map (GMM:6280-6284).
- Group (epoch) tombstone requires no members, empty target, assignmentEpoch == -1 (GMM:6176-6189).
- `addPartitionEpochs` throws if a partition is already owned at >= epoch (CG:1158-1180) — i.e. replay
  enforces "single owner" (R5). A violation live fails the batch (RT:1025-1036); at load it aborts the whole
  load (LDR:279-294 rethrow → LDR:168-170 completes load exceptionally) → *every* group of that
  `__consumer_offsets` partition becomes unavailable. A TLA+ spec should treat "replay throws" as a safety
  violation, not a no-op.
- Legacy CA records without `AssignmentEpochs` default per-partition epoch to `max(memberEpoch,0)`
  (Utils.java:245-279); the size-mismatch branch logs and uses the default.

### 2.3 Is compaction safe for these checks?
Log cleaning keeps, per key, a *suffix* of that key's history (older values removed when a later record for the
same key exists; final tombstones removed after `delete.retention.ms`). Hence, at any replay point *t*, each
key is either at its live value at *t* or absent. Absence only removes owners (no `addPartitionEpochs`
conflict), and MM/CA/TA tombstones of one member are adjacent in one batch/segment so they age out together.
Type transitions (consumer→classic→consumer) also stay consistent because the upgrade batch always has the
GroupMetadata tombstone *before* consumer values (GMM:1462) and downgrade has consumer tombstones *before*
the GroupMetadata value (GMM:1349-1392). Conclusion: no compaction counter-example found; worth a TLA+
check with a `Compact` action that drops a non-last record of a key.

### 2.4 Derived state: replay vs live
| Derived field (timeline unless noted) | Live maintenance | Replay maintenance | Same? |
|---|---|---|---|
| `currentPartitionEpoch` (CG:145) | `updateMember` → `maybeUpdatePartitionEpoch` (CG:318-330, 1090-1100) | same `updateMember` from CA replay (GMM:6300-6305) and CA tombstone (sets epoch -1, empties maps, GMM:6320-6326) | yes |
| `subscribedTopicNames` counts incl. regex (MG:84) | `updateMember`, `updateResolvedRegularExpression` (CG:465-488) | same calls from MM/regex replay | yes |
| `groupsByTopics` (GMM) | via replay of the same records | `updateGroupsByTopics` in MM/regex replay (GMM:6067, 6367) | yes |
| `staticMembers` (CG:123) | `updateStaticMember`/`removeStaticMember` (CG:337-366) | same | yes (KAFKA-20747 fix) |
| `subscribedRegularExpressions` (CG:150), `serverAssignors`, `numClassicProtocolMembers`, `classicProtocolMembersSupportedProtocols` | `updateMember` | same | yes |
| group `state` (CG:118) | `maybeUpdateGroupState` (CG:963) on every update | same | yes |
| `metadataRefreshDeadline` (MG:117, **not timeline**) | set in `updateSubscriptionMetadata` (GMM:4287) and regex result (GMM:3936-3939) | not persisted → EMPTY on load → first heartbeat recomputes hash | compensated: `hasMetadataExpired` also fires when `groupEpoch < deadline.epoch` (MG:342-344), covering reverted writes |
| `hasSubscriptionMetadataRecord` | n/a | partition-metadata replay (GMM:6202-6217) | yes |

Upgrade path note: `convertToConsumerGroup` builds a *detached* `ConsumerGroup` (CG:1217-1283) used to compute
the heartbeat result; `groups` receives a *different* object built by replaying the emitted records. Fidelity
therefore depends on `createConsumerGroupRecords` (CG:1290-1312) covering every field `fromClassicGroup`
sets (epoch, hash, targets, TA-meta(ts=0), MM incl. classicMemberMetadata, CA) — it does.

### 2.5 In-memory-only state and how onLoaded (GMM:6906-6980) rebuilds it
| State | Where | Rebuilt on load? |
|---|---|---|
| session timers | `CoordinatorTimer` | yes, every member; classic members use persisted `classicProtocolSessionTimeout` (KAFKA-21303, commit 349a647c4; GMM:6930-6935). Full timeout restarts (extends deadlines). |
| rebalance timers | timer | yes iff `state == UNREVOKED_PARTITIONS` (GMM:6937-6944), with full `rebalanceTimeoutMs` and the *persisted* `memberEpoch` |
| classic join/sync timers + futures | timer / ClassicGroup | no (futures are lost; members re-send) |
| regex executor task `<groupId>-regex` | EXE `tasks` | no; `unload()` cancels all (RT:623-624); next heartbeat re-schedules (GMM:3813-3857) |
| `lastMetadataImageWithNewTopics` | GMM:550 | `GroupCoordinatorShard.onLoaded` calls `onMetadataUpdate(emptyDelta, image)` first (GroupCoordinatorShard.java:1238-1243) → set to the *current* image version (GMM:6835-6837). Conservative: every persisted regex version < load version → refresh on first heartbeat after >10s |
| `topicHashCache` | GMM | lazily recomputed |
| `metadataRefreshDeadline` | MG:117 | EMPTY → expired |
| streams "refiner" caches | streams only | out of scope |

### 2.6 Stuck-after-reload analysis (Q2)
- UNREVOKED member: timer re-armed → either it revokes (heartbeat) or is fenced; fencing removes its
  partition epochs (CA tombstone) → UNRELEASED members progress. OK.
- UNRELEASED member: no timer needed; progresses when owners release (CAB:372-420 uses `currentPartitionEpoch == -1`).
- groupEpoch > assignmentEpoch (assignment interval or reverted write): next heartbeat of any member computes
  the target (GMM:4354-4366); `assignmentTimestamp` is persisted (GMM:6260-6263) so the interval is respected
  across failover (wall-clock of the old broker; interval=0 escape hatch, GMM:4325-4329).
- Lost response / previous epoch: covered by `previousMemberEpoch` in CA record.
No stuck state found for consumer-protocol members.

**R2 (divergence).** `consumerGroupStaticMemberGroupLeave` (GMM:4927-4944) builds the leaving member with
`Builder(member)` → it **keeps `state`** (CGM:192-195, 254, build at CGM:264-282) while setting epoch -2 and
clearing `partitionsPendingRevocation`. If the member was UNREVOKED_PARTITIONS, the persisted record is
(epoch=-2, state=UNREVOKED, pending=∅). Live: the already-armed rebalance timer carries the old epoch N and is
ignored at fire (`member.memberEpoch() == memberEpoch` check, GMM:5568-5575). After reload: onLoaded arms a
rebalance timer **with epoch -2** (GMM:6937-6944) → it fires and fences the static member (GMM:5568-5574)
after `rebalanceTimeoutMs` from load. Effects: if `rebalanceTimeoutMs < sessionTimeoutMs` (or remaining
session time), a temporarily-left static member is fenced earlier than on a coordinator that did not move;
its partitions (epoch -2 holds, CG:1094-1097) are released and the instance must rejoin from scratch
(new member id path). Rejoin with same id resets epoch to 0 (GMM:3487-3495) so the timer becomes a no-op.
Class: Test-verifiable (unit test: static member in UNREVOKED leaves with -2, reload via replay+onLoaded,
advance rebalanceTimeout < sessionTimeout, observe fence). Model-checkable if the spec tracks timers with
epoch tags. Severity low (contract: static member keeps its slot for session timeout).

**R3 (divergence).** Live arms the consumer rebalance timer only for non-classic members (GMM:4083-4096).
onLoaded has no `useClassicProtocol()` guard (GMM:6937). A classic member in a consumer group in
UNREVOKED_PARTITIONS therefore gets an extra fence deadline after reload. Since classic members reconcile via
JoinGroup within rebalance timeout anyway, effect is minor. Test-verifiable; low.

## 3. Regex subscriptions (Q5)

### 3.1 Flow
- `maybeUpdateRegularExpressions` (GMM:3756-3863): on regex change emits tombstone for the old regex if this
  member was its last subscriber (GMM:3778-3784); new regex → validate + `requireRefresh` if unseen
  (GMM:3787-3793), or `REGEX_UPDATED_AND_RESOLVED` if already resolved (GMM:3797-3799); clearing the regex →
  `REGEX_UPDATED_AND_RESOLVED` (GMM:3801-3803). Epoch is bumped only for `..._AND_RESOLVED` (GMM:2680-2688).
- Refresh gate: group has regexes; no task in flight (`executor.isScheduled(groupId-regex)`, GMM:3813-3816);
  `now > lastRefresh + 10s` (GMM:3820-3823, `REGEX_BATCH_REFRESH_MIN_INTERVAL_MS` GMM:447); and one of
  unresolved regex count mismatch (GMM:3826-3835), image has new topics (GMM:3838), or refresh interval
  elapsed (default 10 min, GroupCoordinatorConfig.java:267; KAFKA-18961 af012e1ec) (GMM:3841).
- Async: `executor.schedule` (GMM:3853-3857) → `TopicRegexResolver.resolveRegularExpressions` resolves all
  subscribed regexes against the image and filters by DESCRIBE ACL of the *triggering request context*
  (TopicRegexResolver.java resolve + filterTopicDescribeAuthorizedTopics) and stamps `version=image.version()`
  and `timestamp=start`.
- Result as a later *write op* (EXE `run` → `scheduleWriteOperation`; `complete()` removes the task first and
  rejects if replaced/cancelled). `handleRegularExpressionsResult` (GMM:3872-3949): skips regexes with no
  subscriber now (GMM:3895), writes a regex record for each remaining regex (even unchanged → refreshes
  version/timestamp), recomputes hash with adjusted counts, bumps epoch if any resolved set changed or hash
  changed (GMM:3901-3940). Group gone → no records (GMM:3941-3944).

### 3.2 Staleness cases
| Interleaving while resolution is in flight | Outcome |
|---|---|
| Member leaves / regex dropped | skipped via `numSubscribedMembers==0` (GMM:3895); tombstone already written; counts consistent |
| Regex dropped then re-added by another member | result applied with possibly older image; version tells; later refresh |
| New regex R2 subscribed | not in the in-flight set; `isScheduled` blocks a new task; R2 stays unresolved; next heartbeat ≥10s after the result timestamp triggers 3.1 path → resolved. Member with only R2 has empty subscription meanwhile (CAB:483-500 treats unresolved as matching nothing) |
| Topic created after resolution image | `lastMetadataImageWithNewTopics` > result version → refresh on next heartbeat after 10s (GMM:3838, 6844-6846) |
| Topic deleted | `onMetadataUpdate` requests refresh for groups in `groupsByTopics` (which includes regex topics because counts include regex) (GMM:6857-6866) → next heartbeat recomputes hash → epoch bump; the deleted name lingers in the resolved set until next resolution but is ignored (not in image) |
| Topic recreated with same name | treated as created → refresh |
| ACL change | no metadata delta; only the 10-min refresh (KAFKA-18961) |
| Coordinator unload | `executor.cancelAll` (RT:624); result rejected (`tasks.remove(key,this)` fails); reload starts fresh |
| Group deleted & recreated | key not cancelled by `createGroupTombstoneRecordsAndCancelTimers` (GMM:9358-9364 cancels timers only); result may apply to the new group, guarded by subscriber check; harmless |
| Resolution throws | empty result, no timestamp update → retried on each heartbeat (no 10s back-off) |
| Write of result fails/reverts | task already removed in `complete()`; `metadataRefreshDeadline` (non-timeline) set with the bumped epoch → `groupEpoch < deadline.epoch` forces a recompute (MG:342-344) |

### 3.3 Answer to Q5
- **Target can contain partitions of a topic the member no longer matches**, transiently: e.g. member switches
  regex A→B with B unresolved: no epoch bump (`REGEX_UPDATED` only), target still assigns A-topics to the member.
  `maybeReconcile` runs with `hasSubscriptionChanged=true` (GMM:2718-2728) and CAB filters target by
  `subscribedTopicIds()` (CAB:389-391, 483-500), so the member revokes A-topics and never re-acquires them; the
  A-partitions stay "assigned in target, owned by nobody" until the resolution result bumps the epoch. Safe;
  liveness delay = resolution latency (+10s batching if a task was in flight).
- **Omit a matching topic indefinitely?** Not found: every path converges given continued heartbeats. Bounded
  delays: 10s batching; until the 10-min refresh for ACL changes; X2 below.
- **X2** `lastResolvedRegularExpressionRefreshTimeMs` uses the persisted `timestamp` of the first entry
  (CG:493-500) written by the *previous* coordinator's wall clock. If that clock was ahead by Δ, after failover
  gate 2 (GMM:3820-3823) blocks *all* refreshes, including resolution of brand-new regexes, for Δ+10s.
  Code-review-only; liveness.
- The "they are always resolved together" assumption (CG:490-492) holds: every refresh resolves the full
  subscribed set, and a regex can only be absent (never stale) relative to others.
- **X3** ACL filter uses the triggering member's principal for the whole group (GMM:3854, TopicRegexResolver).
  Different principals in one group see each other's resolution. Design decision (KAFKA-18813); not a protocol bug.
- **X4** `metadataImage` (GMM:537, non-volatile) is read by the executor thread inside the lambda at run time
  (GMM:3854) — data race, but images are immutable and the version in the result reflects what was read.

## 4. Migration and static membership (secondary)

### 4.1 Upgrade classic → consumer (M1, candidate)
Path: consumer heartbeat with epoch 0 on a classic group → `validateOnlineUpgrade` (GMM:1408-1430: policy,
`usesConsumerGroupProtocol`, size — **no state check**) → `convertToConsumerGroup` (GMM:1454-1497).
`fromClassicGroup` (CG:1217-1283) sets every member STABLE with `memberEpoch = generationId` and
`assignedPartitions = member.assignment()`; target = same; TA-meta(generationId, ts=0). It ignores the
subscription's `ownedPartitions`.
In the classic state machine, `prepareRebalance` from COMPLETING_REBALANCE calls
`resetAndPropagateAssignmentWithError` which sets **every member's assignment to EMPTY** (GMM:7957-7958,
8061-8069). With the COOPERATIVE protocol, clients keep consuming their owned partitions across this rejoin
(only EAGER revokes in `onJoinPrepare`). Scenario:
1. Cooperative classic group stable at gen N; C owns p.
2. New classic member joins → PREPARING → join completes → COMPLETING_REBALANCE (gen N+1); before SyncGroup
   another join arrives → PREPARING_REBALANCE with all assignments reset to EMPTY. C still owns p.
3. Consumer-protocol member X heartbeats epoch 0 → upgrade: C converted with assigned=∅, partition epochs ∅.
4. X's heartbeat bumps epoch to N+2 and computes a target; p is assigned to X; `currentPartitionEpoch(p) == -1`
   so CAB gives p to X immediately (CAB:396-402) → **X and C both consume p** until C's next classic heartbeat
   gets REBALANCE_IN_PROGRESS and C rejoins (classic members are not asked to revoke because the coordinator
   believes C owns nothing). C's offset commits are validated against its member epoch (N+1), still current.
Existing test `testConsumerGroupHeartbeatToClassicGroupWithEmptyAssignmentMember` (GroupMetadataManagerTest.java:14211)
shows empty assignments during COMPLETING_REBALANCE are deliberately accepted (member never synced), but not
the reset-with-owned case. Comment at CG:1252-1255 assumes rejoin fixes things; it does not prevent the window.
Verification: Test-verifiable in GroupMetadataManagerTest (sequence above; assert X is assigned p while C's
subscription `ownedPartitions` contains p). Then confirm with a real cooperative-sticky classic consumer + new
consumer (integration). Compensating mechanism to check: whether the classic join of C (GMM:2898/2938,
`toTopicPartitions(subscription.ownedPartitions())`) would detect the conflict — it only feeds reconciliation of
C itself, after the overlap already happened.

### 4.2 Downgrade consumer → classic
Triggered when the last consumer-protocol member is fenced/leaves (`validateOnlineDowngradeWithFencedMembers`,
GMM:1292-1309) or replaced by a classic static member (GMM:1318-1335, 2822). `convertToClassicGroup`
(GMM:1349-1400): tombstones (with replaced id substitution CG:786-813) + classic GroupMetadata record, state
updated directly, records *not* replayed live (`replayRecords=false`, GMM:5060, 2968). Therefore the strict
replay checks of §2.2 are exercised **only at load**. I traced the replaced-member case: records are
replaceMember(old→new) then tombstones for {new, others}; replay order satisfies all checks; a duplicated regex
tombstone (from GMM:3778-3784 and CG:807-809) is a no-op. OK, but a TLA+ model should check "downgrade
records replay cleanly" since live never validates them.
Irrevocable side effects before commit: upgrade completes classic join/sync futures with REBALANCE_IN_PROGRESS
(GMM:1458-1459) and schedules session timers (GMM:1492-1494) even if the write later fails (timers then hit a
type check). Benign.

### 4.3 Static member replacement
- Consumer protocol both sides: replacement only if the previous member left with epoch -2
  (`throwIfInstanceIdIsUnreleased`, GMM:3514-3518). The -2 leave keeps assigned partitions with epochs reset
  to 0 and partition-epoch map at -2 (owned), drops pending revocations (GMM:4933-4937).
- `replaceMember` (GMM:5170-5205): in one atomic batch: CA/TA/MM tombstones of old, MM(new), TA(new := old
  target), CA(new = copy of old with epoch 0, previousEpoch 0, GMM:3521-3524). Replay: CA-tomb(old) removes
  partition epochs held at -2 (CG:1104-1110 expects equal epoch -2: matches), CA(new) adds them at epoch 0
  (CG:1158-1180, prev null). Ownership transfer is atomic; no window with two owners. The old timers are
  intentionally not cancelled (GMM:5178-5179); they find UnknownMember and no-op (GMM:5418-5422, 5583-5585).
- If either side uses the classic protocol, the previous member is replaced even while active (M2) —
  classic static semantics; the old process is presumed dead. A duplicated instance id (misconfiguration)
  gives two live clients the same partitions. Code-review-only.
- KAFKA-20747 (71faeec8b): a member id can no longer change its instance id (GMM:3485), and stale
  `staticMembers` entries are removed (CG:337-344). KAFKA-20601 (e6919341b): size check skipped for an
  existing instance id. KAFKA-20434 (0d9fe518b): assignor change bumps epoch (GMM:3718-3727, 2665-2688).
- `TargetAssignmentRecordsBuilder` remapping (TargetAssignmentRecordsBuilder.java:233-266) is built for
  offloaded assignment; the consumer path uses `withUnchangedStaticMembers()` (GMM:4412), i.e. synchronous.

## 5. Durable-log vs in-memory table (for the TLA+ spec)

| Record (key → value) | Emitted by | Replay rebuilds | Tombstone replay |
|---|---|---|---|
| ConsumerGroupMetadata(groupId) → {epoch, metadataHash} | `updateSubscriptionMetadata`, fence, regex result, upgrade | `groupEpoch`, `metadataHash` (GMM:6164-6166); creates group | requires no members, empty target, assignmentEpoch=-1, then `removeGroup` (GMM:6176-6191) |
| ConsumerGroupMemberMetadata(groupId, memberId) → {instanceId, rackId, clientId/Host, topics, regex, rebalanceTimeout, assignor, classicMemberMetadata} | MM change, join, replacement | member subscription fields (CGM:234-245) → `updateMember` derives `subscribedTopicNames`, `serverAssignors`, `subscribedRegularExpressions`, `staticMembers`, classic counters, `subscriptionType`, `state`; `groupsByTopics` | requires CA-tomb (epoch=-1) and TA-tomb first; `removeMember` reverses all derived maps (CG:350-361) |
| ConsumerGroupPartitionMetadata(groupId) (legacy) | none now (only tombstones) | `hasSubscriptionMetadataRecord=true` → next heartbeat writes a tombstone (GMM:4292-4294) | flag false |
| ConsumerGroupTargetAssignmentMember(groupId, memberId) → partitions | TA builder, replacement, upgrade | `targetAssignment[memberId]` | remove entry (no check) |
| ConsumerGroupTargetAssignmentMetadata(groupId) → {assignmentEpoch, assignmentTimestamp} | TA builder, fence-to-empty, upgrade | `targetAssignmentMetadata` (assignment epoch + ts) | requires empty target; sets (-1, 0) |
| ConsumerGroupCurrentMemberAssignment(groupId, memberId) → {memberEpoch, previousMemberEpoch, state, assigned(+per-partition epochs), pendingRevocation} | `maybeReconcile`, static leave (-2), replacement, upgrade | member epoch/state/assignment (CGM:247-262) → `currentPartitionEpoch` (throws on overlap), group `state` | member kept with epoch -1, empty maps (GMM:6320-6326) |
| ConsumerGroupRegularExpression(groupId, regex) → {topics, version, timestamp} | regex result | `resolvedRegularExpressions`, regex counts in `subscribedTopicNames`, `groupsByTopics` (GMM:6355-6367) | remove + decrement |
| GroupMetadata(groupId) (classic) | upgrade (tombstone), downgrade (value) | classic group | removes classic group |

In-memory only (lost on failover; see §2.5): timers (session/rebalance/join/sync) with epoch tags, executor
task per group, `metadataRefreshDeadline`, `lastMetadataImageWithNewTopics`, `topicHashCache`, deferred
responses (released on HW, RT `DeferredEventCollection`), classic join/sync futures.

## 6. Modeling recommendations
1. Model a write op as an atomic list of records appended to `log`; `committed` prefix by HW; `Load` replays
   `log` up to LOG_END (LDR:143) — include uncommitted tail, then truncation on leader change.
2. Model `Replay` with the strict checks of §2.2 as assertions; violation = bug (R5).
3. Model timers as (key, epochTag) sets; `Reload` rebuilds per onLoaded rules (§2.5) including the -2/state
   quirk (R2) to check "no fence earlier than session timeout for a -2 static member".
4. Regex: abstract the resolver as an async action producing (topics, version) from a snapshot of `Topics`;
   actions `CreateTopic`, `DeleteTopic`, `ResolveStart`, `ResolveComplete`; invariant: current assignment ⊆
   resolved subscription; liveness (weak fairness on heartbeats/resolution): eventually target matches
   resolved subscription of a stable topic set.
5. Optional `Compact` action (drop a non-last record of a key; drop a final tombstone) to check §2.3.
6. Migration (M1) is better confirmed by a unit test than by TLA+, since it hinges on classic client behavior.
