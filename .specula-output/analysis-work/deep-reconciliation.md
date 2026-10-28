# Deep analysis: KIP-848 member reconciliation & partition ownership (Q1)

Source: `kafka/` trunk @ 1224f3d4b. Paths abbreviated:
CAB = `group-coordinator/.../modern/consumer/CurrentAssignmentBuilder.java`,
CGM = `.../modern/consumer/ConsumerGroupMember.java`, CG = `.../modern/consumer/ConsumerGroup.java`,
MG = `.../modern/ModernGroup.java`, GMM = `.../group/GroupMetadataManager.java`,
TARB = `.../group/TargetAssignmentRecordsBuilder.java`, RT = `coordinator-common/.../CoordinatorRuntime.java`.

## 0. Bottom line for Q1

* Assuming clients behave (they report the partitions they still own) and the assignor's output is
  sane, I found **no path in the reconciliation code where two members hold the same partition**
  in `assignedPartitions ∪ partitionsPendingRevocation` at once. Exclusivity is an *inductive*
  property. One gate enforces it: CAB L404-410. That gate reads `ConsumerGroup.currentPartitionEpoch`,
  a map derived during replay.
* The map protects nothing by itself. `addPartitionEpochs` (CG L1158-1180) **silently
  overwrites** an entry that has a smaller epoch, and it throws only when the stored epoch is at least as large.
  `removePartitionEpochs` (CG L1120-1148) **silently skips** a mismatched epoch. If the gate
  ever let an overlap through, the map would absorb it and lose information. Nothing would detect it.
  So the TLA+ model must check exclusivity on the member records, not on the map.
* Overlap is allowed by design when a member is fenced (rebalance or session timeout, or an explicit leave):
  the member's records are tombstoned and its partitions are freed at once, while the client may still be
  consuming. That is the KIP-848 zombie model, and offset commits are fenced by epoch.
* Candidate issues (section 6): F1, where a rebalance-timeout timer survives a reverted write and can fence
  a STABLE member; F2, where a dynamic leave is not epoch-checked; F3, where an unresolved regex revokes
  everything without bumping the group epoch; plus several code-review notes.

## 1. State variables (code → TLA+)

| TLA+ var | Code | Notes |
|---|---|---|
| `groupEpoch` | `ModernGroup.groupEpoch` (MG; init 1) | replay `ConsumerGroupMetadataValue` GMM L6157-6170 |
| `assignEpoch` | `targetAssignmentMetadata.assignmentEpoch` (MG; INITIAL=1, TargetAssignmentMetadata L23) | replay GMM L6228-6250; tombstone → -1 |
| `target[m]` | `ModernGroup.targetAssignment` (Map<memberId, Assignment>) | replay GMM L6202-6222 |
| `mEpoch[m]`, `mPrev[m]`, `mState[m]` | `memberEpoch`, `previousMemberEpoch`, `state` | replay `CurrentMemberAssignment` GMM L6290-6330 |
| `assigned[m]` | `assignedPartitions: Map<topic, Map<part, assignmentEpoch>>` | per-partition assignment epoch (KIP-1251) |
| `pending[m]` | `partitionsPendingRevocation` (same shape) | |
| `pEpoch[p]` | `ConsumerGroup.currentPartitionEpoch` (CG L145) | **derived** from assigned∪pending at `memberEpoch`, never persisted |
| `owned[m]` (client) | `ownedTopicPartitions` in heartbeat (nullable) | null ⇒ "still owns everything" (CAB L273) |

Initial epochs: new member Builder `memberEpoch=0, previousMemberEpoch=-1, state=STABLE` (CGM L59-61).
Sentinels: `-1` LEAVE_GROUP_MEMBER_EPOCH (current-assignment tombstone, GMM L6318-6322), `-2` static
leave (GMM L4933-4938). `pEpoch[p] = -1` means *free* (CG L584-594).

## 2. Where `pEpoch` is mutated (answer: replay only)

* `ConsumerGroup.updateMember` (CG L318-332) → `maybeUpdatePartitionEpoch` (CG L1090-1097):
  remove old member's assigned∪pending at old epoch, then add new member's assigned∪pending at **new
  memberEpoch** (both sets use the member epoch, not the per-partition assignment epoch).
* `ConsumerGroup.removeMember` (CG L350-361) → `maybeRemovePartitionEpoch`.
* Callers of `updateMember`/`removeMember` for consumer groups: replay of MemberMetadata (GMM L6045, L6066),
  replay of CurrentMemberAssignment (GMM L6303, L6327), and `ConsumerGroup.fromClassicGroup` (CG L1275), which
  builds a *temporary* object during upgrade. The real group is then rebuilt from
  `createConsumerGroupRecords` (GMM L1488). No heartbeat or timer path mutates `pEpoch` directly: every
  change goes through records → replay. Within one heartbeat the handler reads the **pre-request** map
  (records are replayed only after the handler returns), and only one member reconciles per request.
* Load: the map is rebuilt record by record, so it is correct if the loaded record sequence is one
  the live coordinator could have replayed. With log compaction, each key keeps a suffix of its records.
  Argument: if A's surviving record R_A claims P and B's surviving record R_B claims P later, A's
  superseding record (between them) can only have been removed if a still-later A record exists in the
  cleaned range. That would also have removed R_A. So no overlap can appear on load. It only holds if live
  exclusivity holds. Partial-batch and HWM issues belong to Q2/Q3 and are not covered here.
* Uncommitted state: `revertLastWrittenOffset` (RT L788, L1115) rolls the snapshot registry back,
  including `pEpoch` and `members`. Timers are **not** rolled back (see F1).

## 3. Member state machine (CAB `build()` L187-262), suitable for direct encoding

Inputs per heartbeat: `m` (current member after metadata update), `T = target[m]` and `t = assignEpoch`
(from `maybeUpdateTargetAssignment` GMM L4346-4430 or `fromLastTargetAssignment` GMM L298-306),
`subChanged` (GMM L2673), `owned` (nullable), `pEpoch`, `Sub(m)` = `subscribedTopicIds()` CAB L483-500
(an unresolved regex matches **nothing**, L495).

Pre-guard (GMM L4060): if `¬subChanged ∧ mState=STABLE ∧ mEpoch=t` → no-op (`isReconciledTo`,
ModernGroupMember).

Helpers:
* `Owns(S) ≜ owned = null ∨ ∃p ∈ owned : p ∈ S` (CAB L270-287). Partitions in `owned` that are
  not in `S` are **ignored**.
* `CNA(e)` = `computeNextAssignment(e, assigned[m])` (CAB L368-476), per topic τ:
  `Tτ = IF τ ∈ Sub(m) THEN T[τ] ELSE {}` (L389-391);
  `keep = assigned[m][τ] ∩ Tτ` (assignment epochs preserved, L394-395);
  `revoke = assigned[m][τ] \ keep` (L398-399);
  `add = Tτ \ keep \ {p : pEpoch[p] ≠ -1 ∧ p ∉ pending[m]}` (L402-410, **the gate**, KAFKA-19449 exception);
  `unrel = ∃p ∈ Tτ \ keep : pEpoch[p] ≠ -1 ∧ p ∉ pending[m]`.
  Note that `revoke` is computed from `assigned` only. Old `pending[m]` is dropped from the record.
  1. `revoke ≠ {} ∧ Owns(revoke)` → **UNREVOKED**, epoch `e` (unchanged), `assigned:=keep`,
     `pending:=revoke` (L425-435).
  2. else `add ≠ {}` → `assigned := keep ∪ (add ↦ t)`, epoch `t`, pending `{}`, state
     `unrel ? UNRELEASED : STABLE` (L436-455).
  3. else `unrel` → **UNRELEASED**, epoch `t`, `assigned:=keep`, pending `{}` (L456-465).
  4. else **STABLE**, epoch `t`, `assigned:=keep`, pending `{}` (L466-475).
  When `revoke ≠ {}` but the client no longer owns any of it, the revocation is **skipped**
  (taken as already done) and the partitions are freed in the same step.
* `UCA(e)` = `updateCurrentAssignment` (CAB L297-358). It only removes topics not in `Sub(m)`:
  if `Sub(m)={} ∧ pending[m]={}`: move all assigned to pending (L307-310); otherwise move each
  unsubscribed topic of assigned into pending (merge, L314-331). If nothing was removed → unchanged (L334-337).
  If `newPending ≠ {} ∧ Owns(newPending)` → UNREVOKED at epoch e (L339-345). Otherwise the state is
  unchanged, assigned shrinks, and the **old** pending is kept (L353-356).
  Every `updateMemberEpoch(x)` sets `mPrev := mEpoch` even when `x = mEpoch` (CGM L105-110,
  commit b8692f315). This is intentional, so that retries carrying the earlier epoch are fenced.

Transitions:
| From | Guard | Result |
|---|---|---|
| STABLE | `mEpoch ≠ t` | `CNA(mEpoch)` (L194-198); covers mEpoch < t and, defensively, mEpoch > t |
| STABLE | `mEpoch = t ∧ subChanged` | `UCA(mEpoch)` (L199-203) |
| UNREVOKED | `Owns(pending) ∧ subChanged` | `UCA(mEpoch)` (L218-223) |
| UNREVOKED | `Owns(pending) ∧ ¬subChanged` | unchanged (L224-226) |
| UNREVOKED | `¬Owns(pending)` | `CNA(mEpoch)` (L231-234): **no +1 bump** (KAFKA-19974, 8ff7addce) |
| UNRELEASED | always | `CNA(mEpoch)` (L240-243) |
| UNKNOWN | `owned = null ∨ owned ≠ {}` | throw FENCED (L250-253) |
| UNKNOWN | `owned = {}` | `CNA(t)` (L255-258) |

Side effects in `maybeReconcile` (GMM L4049-4100): a record is written iff the result ≠ input. If the
new state is UNREVOKED, `scheduleConsumerGroupRebalanceTimeout(m, mEpoch)` replaces any existing timer
(key = member). Any other state cancels it. The response carries `assigned` only (never pending), and
only when `memberEpoch==0`, the request is full, or `assigned` changed (GMM L2745-2748).

Other member-record writers:
* Static leave (-2): `mEpoch:=-2`, `pending:={}`, assignment epochs reset to 0, **state and assigned
  kept** (GMM L4927-4945). The partitions stay owned (`pEpoch = -2 ≠ -1`) until rejoin or session timeout.
* Static rejoin, same id (epoch was -2): in memory `mEpoch:=0, mPrev:=0` (GMM L3487-3494), then CAB.
* Static replacement (new id): copy of the old member (state, assigned, **pending**) with epoch 0. Records:
  tombstones for the old id, then member + target + current records for the new id (GMM L5170-5193). Both
  consumer-protocol sides require the old member to be at -2 (L3519-3523). If either side is classic,
  replacement is unconditional.
* Fence/leave (`consumerGroupFenceMembers` GMM L5040-5110): tombstones for current → target → member
  (L5332-5336), `groupEpoch+1`, and `assignEpoch:=groupEpoch` when the group empties (KAFKA-20442,
  L5097-5102). **Dynamic leave is not epoch-validated** (GMM L4841-4844). See F2.
* Rebalance timeout (GMM L5557-5590): fences if `member.memberEpoch() == scheduledEpoch`. The state is **not** checked.

Epoch validation on heartbeat (GMM L1673-1694): epoch 0 is always accepted (an existing member keeps its
state). `recv > mEpoch` → FENCED. `recv < mEpoch` is accepted only if `recv = mPrev ∧ owned ⊆ assigned`
(isSubset L1523-1538 ignores pending; owned=null → fenced).

Group state (CG L963-979): EMPTY if no members; else ASSIGNING if `groupEpoch > assignEpoch`; else
RECONCILING if ∃m: ¬(STABLE ∧ mEpoch = assignEpoch); else STABLE.

## 4. Answers to the specific sub-questions

1. **UNRELEASED gate**: CAB L404-410 is the only place. `waitingOnUnreleasedPartition`
   (CG L1372-1386) is a read-only re-implementation used for classic heartbeats (GMM L8781-8783). It does
   **not** apply the `Sub(m)` filter (CAB L389) or the KAFKA-19449 exception. It is a liveness-only
   divergence (see N3).
2. **KAFKA-19449 (own pending)**: the exception only takes effect on the UNREVOKED→CNA exit, because
   in every other state `pending = {}`. The exit happens only when the client reports none of `pending`, so
   letting the same member take p back at epoch t is safe. Cost: while the client still holds p, a member
   whose new target gives p *back* must still revoke p first (UNREVOKED guard L218 sees p in pending).
   This is a liveness/churn issue, not a safety one.
3. **Topic deleted mid-revocation**: a deleted topic drops out of `Sub(m)` (TopicIds over the image), so CNA
   treats its target as empty. Pending partitions of that topic still hold the member in UNREVOKED while
   the client reports them. The client must drop them, or the rebalance timeout fences it. `pEpoch` entries
   keyed by the old topic id leave with the member record, and a recreated topic gets a new id, so the keys
   do not collide.
4. **Stale epoch by >1**: fenced (only `mPrev` is accepted). Because `updateMemberEpoch` clobbers `mPrev` on
   STABLE→UNREVOKED and on UNREVOKED→UNREVOKED, two lost responses in a row fence a healthy client.
   That is a liveness cost by design and is conservative for safety.
5. **Skipped epochs**: `CNA` jumps straight to `t` (with any number of target epochs skipped). Retained
   partitions keep their old assignment epoch, so KIP-1251 commit validation (CG L869-906) is unaffected.
6. **A→B→A**: B is UNRELEASED until A's revocation is replayed. If the target flips back to A while A is
   UNREVOKED holding p, A must still revoke p and then re-acquires it at epoch t (exception 2). B never
   gets p, because `pEpoch[p] = A's epoch`.
7. **Member leaves while B waits**: the tombstone frees p at once. B takes it at its next heartbeat (CNA in
   UNRELEASED). If A was fenced by a timeout rather than leaving, A's client may still consume p. That is
   the intended KIP-848 zombie window.
8. **Target change while UNREVOKED (KAFKA-19974)**: the exit is `CNA(mEpoch)`. If the new target revokes more
   partitions, the member stays UNREVOKED at the **same** epoch with a new `pending`, and the timer is
   rescheduled with a fresh deadline. If not, it jumps to `t`. The epoch never increases while partitions
   are pending. The intent (commit message) is that each partition is owned by a single member at any given
   epoch.
9. **mEpoch vs assignEpoch**: every write sets `mEpoch ∈ {unchanged, t, 0, -2}` with `t = assignEpoch`
   at that time. `assignEpoch` only changes to the current `groupEpoch` (GMM L4392, L5097-5102,
   CG L1227), and `groupEpoch` only increases (fence +1, subscription bump). So `mEpoch ≤ assignEpoch ≤ groupEpoch`
   should hold, and `mEpoch` never decreases except through the 0/-2 resets, -1 tombstones and UNKNOWN→t.
   I found no path to `mEpoch > assignEpoch`, so the defensive `mEpoch ≠ t` branch (CAB L194) is never
   reached in the decreasing direction. **Model-check this.** The code relies on it at GMM L2054 and
   L8781.
10. **Client-reported owned set**: an extra owned partition that is neither pending nor assigned is ignored
    (CAB L275-283). The previous-epoch path rejects it (isSubset). An omitted pending partition counts as
    revoked, so a misbehaving client can cause double consumption (outside the assumptions).
    `owned=null` is conservative everywhere.

## 5. Code-path inconsistencies between CNA branches / UCA

* C1. Branch 1 (UNREVOKED) and branches 2-4 differ: branch 1 keeps `mEpoch` and drops `add`, while 2-4
  jump to `t`. The `revoke ≠ {} ∧ ¬Owns(revoke)` case falls through to 2-4 **in the same step**, so the
  "revoked" partitions are freed (`pEpoch` removed) and the member can receive new partitions at `t`,
  without ever having been told to revoke. This is correct only if the client's `owned` list is truthful
  and complete at that instant.
* C2. UCA L353-356 keeps the *old* `pending` and the current state. It is reached only for STABLE (pending
  empty) or for UNREVOKED where `Owns(old pending)` holds, which implies `Owns(new pending)`, so L353 is
  unreachable for UNREVOKED. The comment at L350-352 agrees. Encode it as an assertion in the model.
* C3. UCA's `Sub(m)={} ∧ pending={}` branch (L307-310) moves *all* assigned to pending. The else-branch
  does the same topic by topic. They are equivalent, but the first can return `newAssigned = Map.of()`
  together with a non-empty pending set that the client does not own (L353 path), and so drops them silently.
* C4. A subscription change that unsubscribes topics makes CNA treat their target as empty, and UCA
  removes them. If an unresolved regex is the member's only subscription, *all* partitions are revoked
  and the group epoch is not bumped (GMM L2677-2687). See F3.

## 6. Findings

**F1 — Rebalance-timeout timer survives a reverted write and can fence a STABLE member.**
*Model-checkable (with a "batch write fails, coordinator stays loaded" action); Test-verifiable.*
`maybeReconcile` schedules the timer in memory when the member enters UNREVOKED (GMM L4087-4093).
`failCurrentBatch`/`revertLastWrittenOffset` (RT L783-790, L1115) roll back the member record but not the
timer. Only `unload()` calls `timer.cancelAll()` (RT L623). The timer's only guard is
`member.memberEpoch() == memberEpoch` (GMM L5569). STABLE→UNREVOKED keeps the epoch, so after a
revert the member is STABLE at the same epoch. If it stays reconciled (`isReconciledTo` skips CAB and
writes no record, so no cancel happens), the timer fires after `rebalanceTimeoutMs` and **fences a STABLE,
healthy member**, bumping the group epoch. KAFKA-19974 widened the window: before it, UNREVOKED exits
bumped the epoch, and `mEpoch` stays constant across more transitions now. Triggering it needs a
non-leadership batch failure (for example RecordTooLarge/RecordBatchTooLarge or a storage error, RT
L1100-1113). It also needs the trigger (a target change or a subscription change) not to be re-applied by
the retry. A batch holds the writes of *many* members, so a failure caused by member X reverts member M's
UNREVOKED entry too, and M's own request is failed and retried. The mirror case also exists: cancel, then
revert, leaves an UNREVOKED member with no rebalance timer (liveness only; the session timeout still applies).
GMM L5174-5175 shows that the authors know timers are not reverted ("in case the change is reverted").
Severity: availability (spurious fence and rebalance), not ownership overlap.

**F2 — Dynamic leave ignores the member epoch.** *Test-verifiable; Q4.* `consumerGroupLeave`
(GMM L4841-4844) fences `memberId` without comparing `request.memberEpoch` to anything. The new
consumer reuses its client-generated member id across leave/rejoin. A duplicated or retried leave
(epoch -1) that arrives after the same client rejoined (epoch 0 → e) fences the new incarnation.
Static leave checks only the instance id → member id mapping (L4849). Note: leave carries -1 by design,
so the server cannot tell incarnations apart.

**F3 — Unresolved regex treated as an empty subscription.** *Model-checkable; Q5.* On a regex
change that is `REGEX_UPDATED` but not yet resolved, `subChanged=true` but there is no group-epoch bump
(GMM L2677-2687). CAB L486-496 then makes `Sub(m)` = topic names only, so UCA revokes every regex-derived
partition and the member ends STABLE at `mEpoch = assignEpoch` with less than `target[m]`.
With `groupEpoch = assignEpoch` the group reports **STABLE** while target partitions sit unowned until the
async resolution bumps the epoch. This is by design ("conservative"), but if resolution is delayed or fails
the group shows STABLE with idle partitions. Good liveness property to check:
`GroupStable ⇒ ∀m: assigned[m] = target[m] ∩ Sub(m)`.

**F4 — `pEpoch` is not self-checking.** *Code-review only.* Overwrite-on-greater (CG L1169-1170) and
skip-on-mismatch (CG L1129-1134, debug log only). An overlap introduced by any future bug would be
absorbed silently. The only hard failure is an IllegalStateException when `prev ≥ epoch`, raised during
**replay**, which aborts the load. For example, two classic members with overlapping assignments at the
same generation, converted by `fromClassicGroup` (CG L1255-1275), would throw during the upgrade.

**F5 — Equal-epoch commit bypass.** *Code-review only; out of scope (offsets).* `validateOffsetCommit` returns NO_OP
when `recv == mEpoch` (CG L682-684), without checking which partitions are committed. An UNRELEASED member at
epoch t may commit offsets for partitions it does not yet own. A well-behaved client does not. KIP-1251's
per-partition check applies only to `recv < mEpoch`. Static leave resets assignment epochs to 0
(CGM L218-232), so after a same-id rejoin every older-epoch commit for retained partitions passes.

**N1** — Previous-epoch acceptance ignores `pending` (isSubset over `assigned` only, GMM L1689), which
is conservative. A partial (non-full) retry with `recv = mPrev` gets the new epoch but **no assignment**
when `assigned` did not change in this request (GMM L2745-2748). The client then advances its epoch
without learning about partitions it was granted in the lost response, and those partitions stall
(liveness). This relies on the client sending a full request after any error.
**N2** — `maybeUpdateTargetAssignment` is synchronous. The offload/"members changed during compute"
remapping in TARB (L119-199, `withChangedStaticMembers`) is **not wired** into the consumer path
(`withUnchangedStaticMembers`, GMM L4412). There is no target race today.
**N3** — `waitingOnUnreleasedPartition` ignores `Sub(m)` and the 19449 exception, so a classic member may be
told it is still waiting when CNA would not wait (liveness only).
**N4** — The target is not checked for disjointness (TargetAssignmentBuilder L93-110). Current-assignment
exclusivity does not depend on it: the gate still holds. But per-epoch uniqueness (I4) does. With
overlapping targets, p can move M→N at the same epoch after M unsubscribes.

## 7. Candidate TLA+ invariants (code variables)

Let `Own(m) ≜ DOMAIN-flatten(assigned[m]) ∪ DOMAIN-flatten(pending[m])` over `members`.

* **I1 PartitionOwnershipExclusive** (Q1): `∀ m≠n ∈ members: Own(m) ∩ Own(n) = {}`.
  Stronger client-side variant, which needs the client model: `∀p: |{m : p ∈ clientOwned[m]}| ≤ 1`, excluding
  fenced zombies (or tracking them separately as `ZombieOverlap`).
* **I2 PartitionEpochMapConsistent**: `∀p: pEpoch[p] = IF ∃m: p∈Own(m) THEN mEpoch[m] ELSE -1`,
  and `∀m: mEpoch[m] ≠ -1 ∨ Own(m) = {}`. Since -1 means free, a member at -1 that still owns
  partitions would silently free them.
* **I3 AssignedOnlyWhenFree** (action property): on a transition of m, for every `p ∈ assigned'[m] \ assigned[m]`:
  `pEpoch[p] = -1 ∨ p ∈ pending[m]`.
* **I4 OwnershipEpochUnique** (KAFKA-19974 intent): history `lastHeld[p] = <<m, maxEpoch>>`. When p
  is added to a member n ≠ prior owner with assignment epoch a: `a > lastHeld[p].maxEpoch`.
* **I5 MemberEpochLeAssignmentEpoch**: `∀m: mEpoch[m] ≤ assignEpoch` and `assignEpoch ≤ groupEpoch`.
* **I6 MemberEpochMonotonic** (action): `mEpoch' ≥ mEpoch ∨ mEpoch' ∈ {0,-1,-2}` (excluding the UNKNOWN path).
* **I7 PendingOnlyWhenUnrevoked**: `pending[m] ≠ {} ⇒ mState[m] = UNREVOKED`. Static leave
  breaks the converse (UNREVOKED with empty pending, GMM L4933-4938), so do not assert the converse.
* **I8 AssignedWithinTarget**: `mState[m] ∈ {STABLE, UNRELEASED} ∧ mEpoch[m] = assignEpoch ⇒
  Own(m) ⊆ target[m] ∩ Sub(m)`, plus `mState[m]=UNREVOKED ⇒ assigned[m] ⊆ target[m]` (as of the epoch it was computed against).
* **I9 AssignmentEpochBound**: `∀m, p ∈ assigned[m]: assigned[m][p] ≤ mEpoch[m]` (or 0 after a static reset).
* **I10 NoSpuriousRebalanceFence** (F1): a rebalance-timeout fence fires only when `mState[m] = UNREVOKED`.
* **L1 Progress** (liveness, weak fairness on heartbeats): `assignEpoch = groupEpoch ∧ stable metadata ~>
  ∀m: mState[m]=STABLE ∧ mEpoch[m]=assignEpoch`.

## 8. Modeling recommendations

* Model `pEpoch` as **derived** (I2 checks the code's incremental update). Encode
  `addPartitionEpochs`/`removePartitionEpochs` literally, including the overwrite and skip behaviour, so
  that F4 masking is visible.
* Actions: `Heartbeat(m, owned)` (CAB table + validation + response), `ClientRevoke(m)`,
  `ComputeTarget` (gated by the assignment interval: let `groupEpoch > assignEpoch` persist), `SubscriptionChange(m)`,
  `Fence(m)` / `Leave(m)` / `StaticLeave(m)` / `StaticRejoin(m)`, `RebalanceTimerFire(m)`, `BatchFail`
  (revert member and group vars, keep timers), `LoseResponse`, and optionally `RegexResolve`.
* Small bounds: 3 members, 2 topics × 2 partitions, an assignor that is a nondeterministic disjoint partition
  of the subscribed partitions (allow an overlapping variant to exercise N4/I4), epochs ≤ 5.
