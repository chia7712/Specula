------------------------------- MODULE base -------------------------------
(***************************************************************************)
(* Apache Kafka group coordinator: KIP-848 consumer group rebalance on top *)
(* of the coordinator runtime (one __consumer_offsets partition, one      *)
(* consumer group).                                                       *)
(*                                                                         *)
(* Derived from kafka trunk @ 1224f3d4b. Modeling brief:                   *)
(*   .specula-output/modeling-brief.md                                     *)
(* Scenarios: 1 runtime load/unload lifecycle vs leader epochs             *)
(*            2 timers as non-timeline state (not reverted, retried)       *)
(*            3 partition ownership under epoch rules and client retries   *)
(*            4 subscription / regex changes and rebalance triggers        *)
(*                                                                         *)
(* Source abbreviations used in annotations:                               *)
(*   GMM = group-coordinator/.../group/GroupMetadataManager.java           *)
(*   CG  = group-coordinator/.../modern/consumer/ConsumerGroup.java        *)
(*   CAB = group-coordinator/.../modern/consumer/CurrentAssignmentBuilder.java *)
(*   CGM = group-coordinator/.../modern/consumer/ConsumerGroupMember.java  *)
(*   MG  = group-coordinator/.../modern/ModernGroup.java                   *)
(*   CR  = coordinator-common/.../runtime/CoordinatorRuntime.java          *)
(*   CTI = coordinator-common/.../runtime/CoordinatorTimerImpl.java        *)
(*   CLI = coordinator-common/.../runtime/CoordinatorLoaderImpl.java       *)
(*   SC  = coordinator-common/.../runtime/SnapshottableCoordinator.java    *)
(*                                                                         *)
(* Abstractions (see modeling brief section 3.2):                          *)
(*  - one group, dynamic members only (no static members, no classic       *)
(*    protocol members, no share/streams groups);                          *)
(*  - partitions carry no per-partition assignment epoch (KIP-1251 epochs  *)
(*    only matter for offset commits, which are out of scope);             *)
(*  - the server-side assignor is a nondeterministic total, disjoint       *)
(*    assignment of the subscribed partitions to eligible members;         *)
(*  - one log entry == one record batch; offsets count batches;            *)
(*  - replication: followers copy the leader log (fetch with truncation);  *)
(*    HWM advances to a prefix present on all brokers; elections are clean.*)
(***************************************************************************)

EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Brokers,          \* brokers hosting replicas of the __consumer_offsets partition
    Members,          \* member ids (client generated, stable across rejoins)
    Partitions,       \* topic partitions of the subscribed topics
    Topics,           \* topic names
    TopicOf,          \* [Partitions -> Topics]
    Regexes,          \* subscribed regular expressions (may be {})
    RegexMatch,       \* [Regexes -> SUBSET Topics] resolution against the (static) metadata image
    Subscriptions,    \* set of subscriptions a client may switch to
    InitialSub,       \* [Members -> Subscriptions]
    NoRegex,          \* sentinel: no regex subscription
    Null,             \* sentinel
    AssignInterval,   \* consumer group assignment interval in ticks (0 disables batching)
    InitLeader        \* initial partition leader

ASSUME TopicOf \in [Partitions -> Topics]
ASSUME RegexMatch \in [Regexes -> SUBSET Topics]
ASSUME InitLeader \in Brokers

\* MemberState.java
STABLE     == "STABLE"
UNREVOKED  == "UNREVOKED_PARTITIONS"
UNRELEASED == "UNRELEASED_PARTITIONS"

\* CoordinatorRuntime.CoordinatorState (CLOSED contexts are removed: NONE)
NONE    == "NONE"
INITIAL == "INITIAL"
LOADING == "LOADING"
ACTIVE  == "ACTIVE"
FAILED  == "FAILED"

\* Errors surfaced by the model
NOERR            == "NONE"
NOT_COORDINATOR  == "NOT_COORDINATOR"
LOAD_IN_PROGRESS == "COORDINATOR_LOAD_IN_PROGRESS"
NOT_LEADER       == "NOT_LEADER_OR_FOLLOWER"
TIMEOUT          == "REQUEST_TIMED_OUT"
GENERIC          == "UNKNOWN_SERVER_ERROR"
GROUP_NOT_FOUND  == "GROUP_ID_NOT_FOUND"
UNKNOWN_MEMBER   == "UNKNOWN_MEMBER_ID"
FENCED_EPOCH     == "FENCED_MEMBER_EPOCH"

\* Member epochs (GMM LEAVE_GROUP_MEMBER_EPOCH)
LEAVE_EPOCH == -1

VARIABLES
    \* --- log layer (ReplicaManager / Partition, abstracted) ---
    logs,            \* [Brokers -> Seq(Entry)] local replica of the partition
    leader,          \* current partition leader
    leaderEpoch,     \* current partition leader epoch
    hwm,             \* high watermark (number of committed entries)
    hwmPending,      \* [Brokers -> BOOLEAN] an HWM update is waiting for the listener (CR:1815-1819)
    listenerActive,  \* [Brokers -> BOOLEAN] HWM listener registered and still active (KAFKA-20634 adapter)
    \* --- coordinator runtime (Scenario 1) ---
    ctx,             \* [Brokers -> Context] CoordinatorContext for the partition
    lq,              \* [Brokers -> Seq(LifecycleEvent)] Load/Unload/CompleteLoad events in FIFO order
    jobs,            \* [Brokers -> Seq(Job)] loader thread FIFO (CLI:77, single threaded)
    \* --- non-timeline state (Scenario 2) ---
    timers,          \* [Brokers -> SUBSET Timer] CoordinatorTimerImpl tasks map
    regexTask,       \* [Brokers -> Null \cup RegexTask] CoordinatorExecutor regex resolution task
    clock,           \* abstract wall clock (assignment interval / timestamps)
    \* --- clients and network (Scenario 3) ---
    client,          \* [Members -> ClientState] consumer membership manager (abstract)
    net,             \* SUBSET Request in flight
    respNet,         \* SUBSET Response in flight
    \* --- ghost / bookkeeping ---
    nextShard,       \* id of the next SnapshottableCoordinator (shard) created on LOADING
    nextUid,         \* id of the next appended log entry
    acked,           \* SUBSET uid: entries whose deferred responses were completed successfully
    flags            \* SUBSET FlagRecord: bad events observed (checked by invariants)

logVars    == <<logs, leader, leaderEpoch, hwm, hwmPending, listenerActive>>
rtVars     == <<ctx, lq, jobs>>
softVars   == <<timers, regexTask, clock>>
clientVars == <<client, net, respNet>>
ghostVars  == <<nextShard, nextUid, acked, flags>>
vars == <<logVars, rtVars, softVars, clientVars, ghostVars>>

-----------------------------------------------------------------------------
(* Generic helpers *)

Range(s) == {s[i] : i \in DOMAIN s}
IsPrefix(s, t) == Len(s) <= Len(t) /\ SubSeq(t, 1, Len(s)) = s
Max(a, b) == IF a > b THEN a ELSE b

NoSub == [names |-> {}, regex |-> NoRegex]

\* Topics a subscription resolves to.
\* CAB:483-500 subscribedTopicIds(): names, plus the resolved regex topics; an
\* unresolved regex matches nothing ("to be conservative", CAB:495).
EffTopics(sub, resolved) ==
    sub.names \cup (IF sub.regex # NoRegex /\ resolved[sub.regex] # Null
                    THEN resolved[sub.regex] ELSE {})

PartsOf(ts) == {p \in Partitions : TopicOf[p] \in ts}

-----------------------------------------------------------------------------
(* In-memory group state (timeline state of GroupMetadataManager/ConsumerGroup) *)

\* ConsumerGroupMember.Builder(memberId) defaults: CGM:59-61
\* inc: ghost incarnation (client join count) of the member record, set when
\* the member is created; used only by NoSpuriousFence.
NewMember == [epoch |-> 0, prev |-> -1, state |-> STABLE,
              assigned |-> {}, pending |-> {}, sub |-> NoSub, inc |-> 0]

\* new ConsumerGroup(...): MG:119-133 (group epoch 1, TargetAssignmentMetadata.INITIAL = (1, 0)).
\* refreshDue models ModernGroup.metadataRefreshDeadline, which starts expired
\* (DeadlineAndEpoch.EMPTY, MG:117) and is not persisted.
EmptyGS == [exists      |-> FALSE,
            groupEpoch  |-> 1,
            hash        |-> {},           \* metadata hash abstracted as the subscribed topic set
            assignEpoch |-> 1,
            assignTs    |-> 0,
            target      |-> [m \in Members |-> Null],
            members     |-> [m \in Members |-> Null],
            resolved    |-> [r \in Regexes |-> Null],
            pEpoch      |-> [p \in Partitions |-> -1],   \* CG.currentPartitionEpoch, -1 = absent
            refreshDue  |-> TRUE]

MembersOf(gs) == {m \in Members : gs.members[m] # Null}
NumSub(gs, r) == Cardinality({m \in MembersOf(gs) : gs.members[m].sub.regex = r})
TopicsOfSubs(subs, resolved) == UNION {EffTopics(s, resolved) : s \in subs}
SubscribedTopics(gs) == TopicsOfSubs({gs.members[m].sub : m \in MembersOf(gs)}, gs.resolved)

-----------------------------------------------------------------------------
(* Records written to __consumer_offsets (GroupCoordinatorRecordHelpers) *)

Rec(k, m, r, v) == [k |-> k, m |-> m, r |-> r, v |-> v]
EpochRec(e, h)    == Rec("Epoch", Null, Null, [epoch |-> e, hash |-> h])      \* ConsumerGroupMetadata
MemberRec(m, s, i) == Rec("Member", m, Null, [sub |-> s, inc |-> i])      \* ConsumerGroupMemberMetadata (+ ghost inc)
MemberTomb(m)     == Rec("Member", m, Null, Null)
TargetRec(m, ps)  == Rec("Target", m, Null, ps)                             \* ConsumerGroupTargetAssignmentMember
TargetTomb(m)     == Rec("Target", m, Null, Null)
TMetaRec(e, ts)   == Rec("TMeta", Null, Null, [epoch |-> e, ts |-> ts])     \* ConsumerGroupTargetAssignmentMetadata
CurrentRec(m, mm) == Rec("Current", m, Null,                                \* ConsumerGroupCurrentMemberAssignment
                        [epoch |-> mm.epoch, prev |-> mm.prev, state |-> mm.state,
                         assigned |-> mm.assigned, pending |-> mm.pending])
CurrentTomb(m)    == Rec("Current", m, Null, Null)
RegexRec(r, ts)   == Rec("Regex", Null, r, ts)                              \* ConsumerGroupRegularExpression
RegexTomb(r)      == Rec("Regex", Null, r, Null)

-----------------------------------------------------------------------------
(* Replay (GroupMetadataManager.replay overloads) *)

\* CG:1120-1148 removePartitionEpochs: remove only if the stored epoch matches,
\* otherwise silently skip (debug log).
RemovePE(pe, S, e) == [p \in Partitions |-> IF p \in S /\ pe[p] = e THEN -1 ELSE pe[p]]
\* CG:1158-1180 addPartitionEpochs: set if absent or smaller, else IllegalStateException.
AddPEOk(pe, S, e)  == \A p \in S : pe[p] < e
AddPE(pe, S, e)    == [p \in Partitions |-> IF p \in S THEN e ELSE pe[p]]

\* CG:318-332 updateMember -> CG:1090-1097 maybeUpdatePartitionEpoch
UpdateMemberRes(gs, m, new) ==
    LET old == gs.members[m]
        pe0 == IF old = Null THEN gs.pEpoch
               ELSE RemovePE(RemovePE(gs.pEpoch, old.assigned, old.epoch), old.pending, old.epoch)
        pe1 == AddPE(pe0, new.assigned, new.epoch)
        ok  == AddPEOk(pe0, new.assigned, new.epoch) /\ AddPEOk(pe1, new.pending, new.epoch)
    IN [g   |-> [gs EXCEPT !.exists = TRUE, !.members[m] = new,
                           !.pEpoch = AddPE(pe1, new.pending, new.epoch)],
        err |-> ~ok]

\* CG:350-361 removeMember -> maybeRemovePartitionEpoch
RemoveMember(gs, m) ==
    LET old == gs.members[m]
    IN [gs EXCEPT !.members[m] = Null,
                  !.pEpoch = RemovePE(RemovePE(gs.pEpoch, old.assigned, old.epoch), old.pending, old.epoch)]

OkRes(g) == [g |-> g, err |-> FALSE]

ReplayRecord(gs, r) ==
    CASE r.k = "Epoch" ->
            \* GMM:6157-6170 (value != null): setGroupEpoch, setMetadataHash
            OkRes([gs EXCEPT !.exists = TRUE, !.groupEpoch = r.v.epoch, !.hash = r.v.hash])
      [] r.k = "Member" /\ r.v # Null ->
            \* GMM:6044-6048: getOrMaybeCreateMember(memberId, true).updateWith(value)
            LET base == IF gs.members[r.m] = Null THEN [NewMember EXCEPT !.inc = r.v.inc] ELSE gs.members[r.m]
            IN UpdateMemberRes(gs, r.m, [base EXCEPT !.sub = r.v.sub])
      [] r.k = "Member" /\ r.v = Null ->
            \* GMM:6049-6067: ignore unknown group/member; strict tombstone-order checks
            IF ~gs.exists \/ gs.members[r.m] = Null THEN OkRes(gs)
            ELSE IF gs.members[r.m].epoch # LEAVE_EPOCH \/ gs.target[r.m] # Null
                 THEN [g |-> gs, err |-> TRUE]                    \* GMM:6058-6065 IllegalStateException
                 ELSE OkRes(RemoveMember(gs, r.m))
      [] r.k = "Target" ->
            \* GMM:6202-6222 updateTargetAssignment / removeTargetAssignment
            IF r.v = Null /\ ~gs.exists THEN OkRes(gs)
            ELSE OkRes([gs EXCEPT !.exists = TRUE, !.target[r.m] = r.v])
      [] r.k = "TMeta" ->
            \* GMM:6258-6268 setTargetAssignmentMetadata(epoch, timestamp)
            OkRes([gs EXCEPT !.exists = TRUE, !.assignEpoch = r.v.epoch, !.assignTs = r.v.ts])
      [] r.k = "Current" /\ r.v # Null ->
            \* GMM:6296-6304 getOrMaybeCreateMember(memberId, true).updateWith(value)
            LET base == IF gs.members[r.m] = Null THEN NewMember ELSE gs.members[r.m]
            IN UpdateMemberRes(gs, r.m,
                   [base EXCEPT !.epoch = r.v.epoch, !.prev = r.v.prev, !.state = r.v.state,
                                !.assigned = r.v.assigned, !.pending = r.v.pending])
      [] r.k = "Current" /\ r.v = Null ->
            \* GMM:6305-6328: member epoch := -1, partitions cleared
            IF ~gs.exists \/ gs.members[r.m] = Null THEN OkRes(gs)
            ELSE UpdateMemberRes(gs, r.m,
                   [gs.members[r.m] EXCEPT !.epoch = LEAVE_EPOCH, !.prev = LEAVE_EPOCH,
                                           !.assigned = {}, !.pending = {}])
      [] r.k = "Regex" ->
            \* GMM:6338-6370 resolved regular expression (tombstone removes it)
            IF r.v = Null /\ ~gs.exists THEN OkRes(gs)
            ELSE OkRes([gs EXCEPT !.exists = TRUE, !.resolved[r.r] = r.v])

RECURSIVE ReplaySeq(_, _)
ReplaySeq(res, s) ==
    IF s = <<>> \/ res.err THEN res
    ELSE ReplaySeq(ReplayRecord(res.g, Head(s)), Tail(s))

RECURSIVE ReplayEntries(_, _)
ReplayEntries(res, es) ==
    IF es = <<>> \/ res.err THEN res
    ELSE ReplayEntries(ReplaySeq(res, Head(es).recs), Tail(es))

ReplayLog(lg) == ReplayEntries(OkRes(EmptyGS), lg)

\* The persisted projection of the group state (refreshDue is not persisted).
Proj(gs) == [gs EXCEPT !.refreshDue = TRUE]

-----------------------------------------------------------------------------
(* Member epoch validation and CurrentAssignmentBuilder *)

\* GMM:1673-1694 throwIfConsumerGroupMemberEpochIsInvalid. Epoch 0 always valid
\* (KAFKA-19233); a smaller epoch is accepted only if it equals the previous
\* epoch and the owned partitions are a subset of the assigned ones (GMM:1523-1538,
\* isSubset(null) = false).
EpochValid(mem, e, owned) ==
    \/ e = 0
    \/ e = mem.epoch
    \/ /\ e < mem.epoch
       /\ e = mem.prev
       /\ owned # Null
       /\ owned \subseteq mem.assigned

\* CAB:270-287 ownsRevokedPartitions: null owned set means "still owns everything".
Owns(owned, S) == owned = Null \/ owned \cap S # {}

\* CGM:105-110 updateMemberEpoch(x): previousMemberEpoch := memberEpoch; memberEpoch := x
WithEpoch(mm, e) == [mm EXCEPT !.prev = mm.epoch, !.epoch = e]

\* CAB:368-476 computeNextAssignment(memberEpoch, assigned)
CNA(mm, e, t, T, eff, owned, pe) ==
    LET target   == {p \in T : TopicOf[p] \in eff}            \* CAB:388-391 unsubscribed topic => empty target
        keep     == mm.assigned \cap target                    \* CAB:393-395
        revoke   == mm.assigned \ keep                         \* CAB:397-399
        pendAsg0 == target \ keep                              \* CAB:401-403
        unrel    == {p \in pendAsg0 : pe[p] # -1 /\ p \notin mm.pending}   \* CAB:404-410 (KAFKA-19449)
        pendAsg  == pendAsg0 \ unrel
    IN IF revoke # {} /\ Owns(owned, revoke) THEN
          \* CAB:425-435 stay in current epoch, UNREVOKED_PARTITIONS
          [WithEpoch(mm, e) EXCEPT !.state = UNREVOKED, !.assigned = keep, !.pending = revoke]
       ELSE IF pendAsg # {} THEN
          \* CAB:436-455 assign and move to target epoch
          [WithEpoch(mm, t) EXCEPT !.state = IF unrel # {} THEN UNRELEASED ELSE STABLE,
                                   !.assigned = keep \cup pendAsg, !.pending = {}]
       ELSE IF unrel # {} THEN
          \* CAB:456-465
          [WithEpoch(mm, t) EXCEPT !.state = UNRELEASED, !.assigned = keep, !.pending = {}]
       ELSE
          \* CAB:466-475
          [WithEpoch(mm, t) EXCEPT !.state = STABLE, !.assigned = keep, !.pending = {}]

\* CAB:297-358 updateCurrentAssignment(memberEpoch, assigned).
\* Java reference-equality "no partitions removed" is modeled semantically.
UCA(mm, e, eff, owned) ==
    LET allBranch == eff = {} /\ mm.pending = {}                      \* CAB:307-310
        removed   == IF allBranch THEN mm.assigned
                     ELSE {p \in mm.assigned : TopicOf[p] \notin eff}  \* CAB:312-331
        newAsg    == mm.assigned \ removed
        newPend   == mm.pending \cup removed
    IN IF removed = {} THEN mm                                         \* CAB:334-337
       ELSE IF newPend # {} /\ Owns(owned, newPend) THEN               \* CAB:339-345
          [WithEpoch(mm, e) EXCEPT !.state = UNREVOKED, !.assigned = newAsg, !.pending = newPend]
       ELSE                                                            \* CAB:346-356 keep state & old pending
          [WithEpoch(mm, e) EXCEPT !.assigned = newAsg]

\* CAB:187-262 build()
CABBuild(mm, t, T, subChanged, owned, pe, resolved) ==
    LET eff == EffTopics(mm.sub, resolved) IN
    CASE mm.state = STABLE ->
            IF mm.epoch # t THEN CNA(mm, mm.epoch, t, T, eff, owned, pe)        \* CAB:194-198
            ELSE IF subChanged THEN UCA(mm, mm.epoch, eff, owned)               \* CAB:199-203
            ELSE mm
      [] mm.state = UNREVOKED ->
            IF Owns(owned, mm.pending) THEN                                     \* CAB:218-226
                 (IF subChanged THEN UCA(mm, mm.epoch, eff, owned) ELSE mm)
            ELSE CNA(mm, mm.epoch, t, T, eff, owned, pe)                        \* CAB:231-234 (KAFKA-19974: no +1)
      [] mm.state = UNRELEASED ->
            CNA(mm, mm.epoch, t, T, eff, owned, pe)                             \* CAB:240-243

\* ModernGroupMember.isReconciledTo (MGM:159-161)
IsReconciledTo(mm, t) == mm.state = STABLE /\ mm.epoch = t

-----------------------------------------------------------------------------
(* Target assignment (GMM:4345-4430, TargetAssignmentBuilder) *)

\* GMM:4312-4332 canComputeNextTargetAssignment
CanCompute(ts, now) == ts = 0 \/ AssignInterval = 0 \/ now >= ts + AssignInterval

\* Abstract assignor: every subscribed partition goes to exactly one member
\* subscribed to its topic. subsOf: [view -> Subscription].
AssignChoices(view, subsOf, resolved) ==
    LET eff == [x \in view |-> EffTopics(subsOf[x], resolved)]
        SP  == {p \in Partitions : \E x \in view : TopicOf[p] \in eff[x]}
    IN { [x \in Members |-> IF x \in view THEN {p \in SP : f[p] = x} ELSE Null] :
           f \in {g \in [SP -> view] : \A p \in SP : TopicOf[p] \in eff[g[p]]} }

\* TargetAssignmentRecordsBuilder: one record per member whose target changed.
RECURSIVE SeqOfSet(_)
SeqOfSet(S) == IF S = {} THEN <<>>
               ELSE LET x == CHOOSE y \in S : TRUE IN <<x>> \o SeqOfSet(S \ {x})

TargetRecs(gs, view, newT) ==
    LET changed == {x \in view : gs.target[x] # newT[x]}
        ms == SeqOfSet(changed)
    IN [i \in 1..Len(ms) |-> TargetRec(ms[i], newT[ms[i]])]

-----------------------------------------------------------------------------
(* Fencing (GMM:5040-5110 consumerGroupFenceMembers, single member) *)

FenceRecs(gs, m) ==
    LET mm        == gs.members[m]
        deleted   == IF mm.sub.regex # NoRegex /\ NumSub(gs, mm.sub.regex) = 1
                     THEN {mm.sub.regex} ELSE {}                        \* GMM:5066-5071 maybeDeleteResolvedRegularExpressions
        remaining == MembersOf(gs) \ {m}
        resolved2 == [r \in Regexes |-> IF r \in deleted THEN Null ELSE gs.resolved[r]]
        hash2     == TopicsOfSubs({gs.members[x].sub : x \in remaining}, resolved2)   \* GMM:5073-5084
        ge2       == gs.groupEpoch + 1                                  \* GMM:5087-5088 always bump
        regexTombs == IF deleted = {} THEN <<>> ELSE <<RegexTomb(CHOOSE r \in deleted : TRUE)>>
    IN <<CurrentTomb(m), TargetTomb(m), MemberTomb(m)>>                  \* GMM:5332-5336 removeMember order
       \o regexTombs
       \o <<EpochRec(ge2, hash2)>>
       \o (IF remaining = {} THEN <<TMetaRec(ge2, 0)>> ELSE <<>>)      \* GMM:5097-5102 (KAFKA-20442)

-----------------------------------------------------------------------------
(* Timers (CTI tasks map). One task per key <<kind, member>>.               *)
(* retry: task re-scheduled by CTI:121-124 after a failed timeout write     *)
(*        (fires after retryBackoff, 500 ms).                                *)
(* inc:   ghost, incarnation of the member the task was scheduled for        *)
(*        (Scenario 2, MC-3).                                                *)

TimerKey(t) == <<t.kind, t.m>>
SessionTimer(m, i, shard)      == [kind |-> "session",   m |-> m, epoch |-> 0, inc |-> i, shard |-> shard, retry |-> FALSE]
RebalanceTimer(m, e, i, shard) == [kind |-> "rebalance", m |-> m, epoch |-> e, inc |-> i, shard |-> shard, retry |-> FALSE]

\* CTI:139-141 schedule(): tasks.put(key, task) replaces (and cancels) the previous task.
Schedule(ts, t) == {x \in ts : TimerKey(x) # TimerKey(t)} \cup {t}
Cancel(ts, kind, m) == {x \in ts : TimerKey(x) # <<kind, m>>}
CancelMember(ts, m) == Cancel(Cancel(ts, "session", m), "rebalance", m)   \* GMM:5341 cancelTimers

\* GMM:6923-6937 onLoaded for consumer groups: session timer per member,
\* rebalance timer per member in UNREVOKED_PARTITIONS.
OnLoadedTimers(gs, shard) ==
    {SessionTimer(m, gs.members[m].inc, shard) : m \in MembersOf(gs)}
    \cup {RebalanceTimer(m, gs.members[m].epoch, gs.members[m].inc, shard) :
            m \in {x \in MembersOf(gs) : gs.members[x].state = UNREVOKED}}

-----------------------------------------------------------------------------
(* Heartbeat generation (GMM:5725-5752 dispatch)                            *)
(* Returns [err, recs, resp, timerFn-inputs, regex scheduling].              *)

HbErr(e) == [err |-> e, recs |-> <<>>, resp |-> Null, cab |-> Null, changed |-> FALSE,
             fenced |-> FALSE, schedRegex |-> Null, refreshed |-> FALSE, computed |-> FALSE, inc |-> 0]

\* consumerGroupLeave (GMM:4827-4845), dynamic member: fence without epoch check.
LeaveGen(gs, req) ==
    IF ~gs.exists THEN HbErr(GROUP_NOT_FOUND)                         \* consumerGroup(groupId) throws
    ELSE IF gs.members[req.m] = Null THEN HbErr(UNKNOWN_MEMBER)       \* CG:262-266
    ELSE [err |-> NOERR, recs |-> FenceRecs(gs, req.m),
          resp |-> [epoch |-> LEAVE_EPOCH, assign |-> Null],          \* response echoes epoch -1 (GMM:4834-4836)
          cab |-> Null, changed |-> FALSE, fenced |-> TRUE, schedRegex |-> Null, refreshed |-> FALSE,
          computed |-> FALSE, inc |-> 0]

\* consumerGroupHeartbeat (GMM:2589-2752) with assignor choice asg (Null when
\* no new target assignment is computed). taskBusy: executor already has the
\* group regex task scheduled (GMM:3820-3823).
HbGen(gs, req, now, asg, taskBusy) ==
    LET m       == req.m
        create  == req.epoch = 0                                      \* GMM:2606
        old     == gs.members[m]
        member  == IF old # Null THEN old ELSE NewMember              \* CG:255-268
        newSub  == IF req.sub = Null THEN member.sub ELSE req.sub     \* GMM:2637-2645 maybeUpdate*
        updated == [member EXCEPT !.sub = newSub]
        \* hasMemberSubscriptionChanged GMM:3690-3705
        metaChanged  == old = Null \/ newSub # member.sub
        namesChanged == metaChanged /\ newSub.names # member.sub.names
        inc          == IF old = Null THEN req.inc ELSE old.inc               \* ghost
        recMember    == IF metaChanged THEN <<MemberRec(m, newSub, inc)>> ELSE <<>>
        \* maybeUpdateRegularExpressions GMM:3756-3860
        oldR    == member.sub.regex
        newR    == newSub.regex
        rChanged == oldR # newR
        recRegexTomb == IF rChanged /\ oldR # NoRegex /\ NumSub(gs, oldR) = 1
                        THEN <<RegexTomb(oldR)>> ELSE <<>>            \* GMM:3780-3787
        status  == IF ~rChanged THEN "NO_CHANGE"
                   ELSE IF newR # NoRegex THEN
                          (IF NumSub(gs, newR) = 0 THEN "REGEX_UPDATED"            \* GMM:3790-3794
                           ELSE IF gs.resolved[newR] # Null THEN "RESOLVED"         \* GMM:3795-3800
                           ELSE "REGEX_UPDATED")
                   ELSE IF oldR # NoRegex THEN "RESOLVED"                          \* GMM:3802-3804 (KAFKA-19891)
                   ELSE "REGEX_UPDATED"
        requireRefresh0 == rChanged /\ newR # NoRegex /\ NumSub(gs, newR) = 0
        subRegexAfter == {r \in Regexes :
                            NumSub(gs, r) - (IF oldR = r /\ old # Null THEN 1 ELSE 0)
                                          + (IF newR = r THEN 1 ELSE 0) > 0}       \* GMM:3832-3838
        numResolved == Cardinality({r \in Regexes : gs.resolved[r] # Null})
        groupHasRegex == \E r \in Regexes : NumSub(gs, r) > 0
        requireRefresh == requireRefresh0 \/ Cardinality(subRegexAfter) # numResolved  \* GMM:3840 (3.2/3.3 time based omitted)
        schedRegex == IF (requireRefresh0 \/ groupHasRegex) /\ ~taskBusy           \* GMM:3816-3823
                         /\ requireRefresh /\ subRegexAfter # {}
                      THEN subRegexAfter ELSE Null                                \* GMM:3847-3853
        \* GMM:2671-2701
        subChanged == namesChanged \/ status \in {"REGEX_UPDATED", "RESOLVED"}
        bump0      == namesChanged \/ status = "RESOLVED"
        doUpdate   == bump0 \/ gs.refreshDue                                      \* GMM:2689 hasMetadataExpired
        view       == MembersOf(gs) \cup {m}
        subsOf     == [x \in view |-> IF x = m THEN newSub ELSE gs.members[x].sub]
        hash2      == TopicsOfSubs({subsOf[x] : x \in view}, gs.resolved)          \* updateSubscriptionMetadata
        bump       == doUpdate /\ (bump0 \/ hash2 # gs.hash)
        ge         == IF bump THEN gs.groupEpoch + 1 ELSE gs.groupEpoch
        recEpoch   == IF bump THEN <<EpochRec(ge, hash2)>> ELSE <<>>
        \* maybeUpdateTargetAssignment GMM:4345-4430
        upToDate   == gs.assignEpoch >= ge
        compute    == ~upToDate /\ CanCompute(gs.assignTs, now)
        lastT      == IF gs.target[m] = Null THEN {} ELSE gs.target[m]            \* fromLastTargetAssignment
        t          == IF compute THEN ge ELSE gs.assignEpoch
        T          == IF compute THEN asg[m] ELSE lastT
        recTarget  == IF compute THEN TargetRecs(gs, view, asg) \o <<TMetaRec(ge, now)>> ELSE <<>>
        \* maybeReconcile GMM:4049-4100
        skip       == ~subChanged /\ IsReconciledTo(updated, t)
        cab        == IF skip THEN updated
                      ELSE CABBuild(updated, t, T, subChanged, req.owned, gs.pEpoch, gs.resolved)
        changed    == cab # updated
        recCurrent == IF changed THEN <<CurrentRec(m, cab)>> ELSE <<>>
        \* response GMM:2733-2750
        sendAsg    == req.epoch = 0 \/ req.full \/ cab.assigned # member.assigned
    IN IF ~gs.exists /\ ~create THEN HbErr(GROUP_NOT_FOUND)           \* GMM:880-886
       ELSE IF old = Null /\ ~create THEN HbErr(UNKNOWN_MEMBER)        \* CG:262-266
       ELSE IF ~EpochValid(member, req.epoch, req.owned) THEN HbErr(FENCED_EPOCH)
       ELSE [err |-> NOERR,
             recs |-> recMember \o recRegexTomb \o recEpoch \o recTarget \o recCurrent,
             resp |-> [epoch |-> cab.epoch, assign |-> IF sendAsg THEN cab.assigned ELSE Null],
             cab |-> cab, changed |-> changed, fenced |-> FALSE, schedRegex |-> schedRegex,
             refreshed |-> doUpdate, computed |-> compute, inc |-> inc]

\* Inputs of the target assignment computation (mirrors HbGen's LET).
HbView(gs, req) ==
    LET member == IF gs.members[req.m] # Null THEN gs.members[req.m] ELSE NewMember
        newSub == IF req.sub = Null THEN member.sub ELSE req.sub
        view   == MembersOf(gs) \cup {req.m}
    IN [view |-> view, subsOf |-> [x \in view |-> IF x = req.m THEN newSub ELSE gs.members[x].sub]]

NullAsg == [x \in Members |-> {}]

\* Timer side effects of a heartbeat (GMM:4085-4095 and GMM:2731).
HbTimers(ts, req, res, shard) ==
    LET ts1 == IF res.changed
               THEN IF res.cab.state = UNREVOKED
                    THEN Schedule(ts, RebalanceTimer(req.m, res.cab.epoch, res.inc, shard))
                    ELSE Cancel(ts, "rebalance", req.m)
               ELSE ts
    IN Schedule(ts1, SessionTimer(req.m, res.inc, shard))

-----------------------------------------------------------------------------
(* Regex resolution result (GMM:3870-3930 handleRegularExpressionsResult) *)

RegexResultRecs(gs, regexes) ==
    IF ~gs.exists THEN <<>>
    ELSE
      LET live      == {r \in regexes : NumSub(gs, r) > 0}                     \* GMM:3893 skip
          changed   == {r \in live : (IF gs.resolved[r] = Null THEN {} ELSE gs.resolved[r]) # RegexMatch[r]}
          resolved2 == [r \in Regexes |-> IF r \in live THEN RegexMatch[r] ELSE gs.resolved[r]]
          hash2     == TopicsOfSubs({gs.members[x].sub : x \in MembersOf(gs)}, resolved2)
          bump      == changed # {} \/ hash2 # gs.hash                         \* GMM:3900-3935
          rs        == SeqOfSet(live)
      IN [i \in 1..Len(rs) |-> RegexRec(rs[i], RegexMatch[rs[i]])]
         \o (IF bump THEN <<EpochRec(gs.groupEpoch + 1, hash2)>> ELSE <<>>)

-----------------------------------------------------------------------------
(* Coordinator runtime: contexts, batches, deferred events                 *)

NoCtx == [st |-> NONE, epoch |-> -1, shard |-> 0, gs |-> EmptyGS,
          written |-> 0, committed |-> 0, batch |-> Null, dq |-> {}]

\* An operation outcome: op completed ok or with error; uid of the log entry
\* whose commit released it (0 if none).
Out(op, ok, err, uid) == [op |-> op, ok |-> ok, err |-> err, uid |-> uid]
FailOps(ops, err) == {Out(o, FALSE, err, 0) : o \in ops}

\* CR:885-1047 append() for one write operation.
\*  - no records: waitForPendingWrites CR:873-883
\*  - records: replay each into the timeline state, append to the open batch;
\*    a replay exception fails (and reverts) the whole batch, CR:1019-1033.
AppendRes(c, recs, op) ==
    IF recs = <<>> THEN
        IF c.batch # Null THEN [c |-> [c EXCEPT !.batch.ops = Append(@, op)], outs |-> {}]
        ELSE IF c.committed < c.written
             THEN [c |-> [c EXCEPT !.dq = @ \cup {[off |-> c.written, op |-> op]}], outs |-> {}]
             ELSE [c |-> c, outs |-> {Out(op, TRUE, NOERR, 0)}]
    ELSE
        LET b0 == IF c.batch = Null
                  THEN [base |-> c.written, baseGs |-> c.gs, recs |-> <<>>, ops |-> <<>>]   \* CR:805-870 maybeAllocateNewBatch
                  ELSE c.batch
            rr == ReplaySeq(OkRes(c.gs), recs)
        IN IF rr.err
           THEN [c |-> [c EXCEPT !.gs = b0.baseGs, !.batch = Null],                         \* failCurrentBatch CR:783-790
                 outs |-> FailOps(Range(Append(b0.ops, op)), GENERIC)]
           ELSE [c |-> [c EXCEPT !.gs = rr.g,
                                 !.batch = [b0 EXCEPT !.recs = @ \o recs, !.ops = Append(@, op)]],
                 outs |-> {}]

\* CR:617-635 unload(): deregister listener, cancel timers and executor tasks,
\* fail deferred events and the current batch with NOT_COORDINATOR.
UnloadOuts(c) ==
    FailOps({d.op : d \in c.dq} \cup (IF c.batch = Null THEN {} ELSE Range(c.batch.ops)), NOT_COORDINATOR)

\* Client responses for heartbeat outcomes.
HbResps(outs) ==
    {[m |-> o.op.m, reqId |-> o.op.reqId, ok |-> o.ok, err |-> o.err,
      epoch |-> IF o.ok THEN o.op.resp.epoch ELSE 0,
      assign |-> IF o.ok THEN o.op.resp.assign ELSE Null] : o \in {x \in outs : x.op.kind = "hb"}}

\* CTI:100-128 exceptionally(): retry unless NotCoordinator / LoadInProgress.
Retryable(err) == err \notin {NOT_COORDINATOR, LOAD_IN_PROGRESS}

RetryTimers(ts, outs, shard) ==
    LET rs == {o \in outs : o.op.kind = "timer" /\ ~o.ok /\ Retryable(o.err)}
        keys == {<<o.op.tkind, o.op.m>> : o \in rs}
        news == {[kind |-> o.op.tkind, m |-> o.op.m, epoch |-> o.op.tepoch, inc |-> o.op.tinc,
                  shard |-> shard, retry |-> TRUE] : o \in rs}
    IN {x \in ts : TimerKey(x) \notin keys} \cup news

\* Common post-processing of operation outcomes on broker b.
ApplyOuts(b, outs, ts, shard) ==
    /\ respNet' = respNet \cup HbResps(outs)
    /\ timers' = [timers EXCEPT ![b] = RetryTimers(ts, outs, shard)]
    /\ acked' = acked \cup {o.uid : o \in {x \in outs : x.ok /\ x.uid # 0}}

-----------------------------------------------------------------------------
(* Client side (abstract ConsumerMembershipManager)                          *)

Req(m, cl, e, owned, sub, full) ==
    [m |-> m, id |-> cl.next, inc |-> cl.inc, epoch |-> e, owned |-> owned, sub |-> sub, full |-> full]

FatalErr(e) == e \in {FENCED_EPOCH, UNKNOWN_MEMBER, GROUP_NOT_FOUND}

-----------------------------------------------------------------------------
(* Initial state *)

Init ==
    /\ logs = [b \in Brokers |-> <<>>]
    /\ leader = InitLeader
    /\ leaderEpoch = 1
    /\ hwm = 0
    /\ hwmPending = [b \in Brokers |-> FALSE]
    /\ listenerActive = [b \in Brokers |-> FALSE]
    /\ ctx = [b \in Brokers |-> NoCtx]
    /\ lq = [b \in Brokers |-> IF b = InitLeader THEN <<[type |-> "Load", epoch |-> 1]>> ELSE <<>>]
    /\ jobs = [b \in Brokers |-> <<>>]
    /\ timers = [b \in Brokers |-> {}]
    /\ regexTask = [b \in Brokers |-> Null]
    /\ clock = 1
    /\ client = [m \in Members |-> [st |-> "out", inc |-> 0, epoch |-> -1, owned |-> {}, assignment |-> {},
                                    sub |-> InitialSub[m], waiting |-> 0, next |-> 1, needFull |-> TRUE]]
    /\ net = {}
    /\ respNet = {}
    /\ nextShard = 1
    /\ nextUid = 1
    /\ acked = {}
    /\ flags = {}

-----------------------------------------------------------------------------
(* Log layer actions (Scenario 1)                                           *)

\* Follower fetch: truncate divergent tail and catch up with the leader.
Replicate(f) ==
    /\ f # leader
    /\ logs[f] # logs[leader]
    /\ logs' = [logs EXCEPT ![f] = logs[leader]]
    /\ UNCHANGED <<leader, leaderEpoch, hwm, hwmPending, listenerActive, rtVars, softVars, clientVars, ghostVars>>

\* HWM advance on the leader; the listener is notified only if registered
\* and active at that time (CR:1809-1819; no initial push on registration).
AdvanceHWM ==
    \E k \in (hwm + 1)..Len(logs[leader]) :
        /\ \A f \in Brokers \ {leader} : IsPrefix(SubSeq(logs[leader], 1, k), logs[f])
        /\ hwm' = k
        /\ hwmPending' = [hwmPending EXCEPT ![leader] = listenerActive[leader]]
        /\ UNCHANGED <<logs, leader, leaderEpoch, listenerActive, rtVars, softVars, clientVars, ghostVars>>

\* Clean leader election to an in-sync replica. The old leader becomes a
\* follower (listener adapter deactivated, KAFKA-20634) and is asked to unload;
\* the new leader is asked to load. Both requests are queued events.
LeaderChange(n) ==
    /\ n # leader
    /\ IsPrefix(logs[n], logs[leader])
    /\ Len(logs[n]) >= hwm
    /\ leader' = n
    /\ leaderEpoch' = leaderEpoch + 1
    /\ listenerActive' = [listenerActive EXCEPT ![leader] = FALSE]
    /\ hwmPending' = [hwmPending EXCEPT ![leader] = FALSE]
    /\ lq' = [lq EXCEPT ![leader] = Append(@, [type |-> "Unload", epoch |-> leaderEpoch + 1]),
                        ![n] = Append(@, [type |-> "Load", epoch |-> leaderEpoch + 1])]
    /\ UNCHANGED <<logs, hwm, ctx, jobs, softVars, clientVars, ghostVars>>

-----------------------------------------------------------------------------
(* Runtime lifecycle actions (Scenario 1)                                   *)

\* CR:2355-2392 scheduleLoadOperation event. maybeCreateContext + epoch check.
\* transitionTo(LOADING) CR:530-547: new shard, enqueue the loader job.
ProcessLoad(b) ==
    /\ lq[b] # <<>>
    /\ Head(lq[b]).type = "Load"
    /\ LET e  == Head(lq[b]).epoch
           c0 == IF ctx[b].st = NONE THEN [NoCtx EXCEPT !.st = INITIAL] ELSE ctx[b]
       IN IF c0.epoch < e /\ c0.st \in {INITIAL, FAILED}
          THEN /\ ctx' = [ctx EXCEPT ![b] = [NoCtx EXCEPT !.st = LOADING, !.epoch = e, !.shard = nextShard]]
               /\ jobs' = [jobs EXCEPT ![b] = Append(@, [shard |-> nextShard, pos |-> 0, err |-> FALSE])]
               /\ nextShard' = nextShard + 1
          ELSE /\ ctx' = [ctx EXCEPT ![b] = IF c0.epoch < e THEN [c0 EXCEPT !.epoch = e] ELSE c0]
               /\ UNCHANGED <<jobs, nextShard>>
    /\ lq' = [lq EXCEPT ![b] = Tail(@)]
    /\ UNCHANGED <<logVars, softVars, clientVars, nextUid, acked, flags>>

\* CR:2409-2445 scheduleUnloadOperation event: if context.epoch < epoch,
\* transition to CLOSED (unload) and remove the context.
ProcessUnload(b) ==
    /\ lq[b] # <<>>
    /\ Head(lq[b]).type = "Unload"
    /\ LET e == Head(lq[b]).epoch
           c == ctx[b]
       IN IF c.st # NONE /\ c.epoch < e
          THEN /\ ctx' = [ctx EXCEPT ![b] = NoCtx]
               /\ listenerActive' = [listenerActive EXCEPT ![b] = FALSE]
               /\ regexTask' = [regexTask EXCEPT ![b] = Null]
               /\ ApplyOuts(b, UnloadOuts(c), {}, 0)
          ELSE /\ UNCHANGED <<ctx, listenerActive, regexTask, respNet, timers, acked>>
    /\ lq' = [lq EXCEPT ![b] = Tail(@)]
    /\ UNCHANGED <<logs, leader, leaderEpoch, hwm, hwmPending, jobs, clock, client, net,
                   nextShard, nextUid, flags>>

\* CR:578-614 load(): completion of a loader job. The completion looks up
\* coordinators.get(tp) and checks only state == LOADING (no epoch / shard check).
ProcessCompleteLoad(b) ==
    /\ lq[b] # <<>>
    /\ Head(lq[b]).type = "CompleteLoad"
    /\ LET ev == Head(lq[b])
           c  == ctx[b]
       IN IF c.st = LOADING
          THEN IF ev.ok
               THEN \* transitionTo(ACTIVE) CR:549-554: register listener, onLoaded
                    /\ ctx' = [ctx EXCEPT ![b].st = ACTIVE]
                    /\ listenerActive' = [listenerActive EXCEPT ![b] = TRUE]
                    /\ hwmPending' = [hwmPending EXCEPT ![b] = FALSE]
                    /\ timers' = [timers EXCEPT ![b] = OnLoadedTimers(c.gs, c.shard)]
                    /\ flags' = IF ev.shard # c.shard
                                THEN flags \cup {[kind |-> "StaleLoadCompletion", b |-> b, shard |-> ev.shard]}
                                ELSE flags
                    /\ UNCHANGED <<regexTask, respNet, acked>>
               ELSE \* transitionTo(FAILED) CR:556-560: unload, context stays FAILED
                    /\ ctx' = [ctx EXCEPT ![b] = [NoCtx EXCEPT !.st = FAILED, !.epoch = c.epoch, !.shard = c.shard]]
                    /\ listenerActive' = [listenerActive EXCEPT ![b] = FALSE]
                    /\ regexTask' = [regexTask EXCEPT ![b] = Null]
                    /\ ApplyOuts(b, UnloadOuts(c), {}, 0)
                    /\ flags' = IF ev.shard # c.shard
                                THEN flags \cup {[kind |-> "StaleLoadCompletion", b |-> b, shard |-> ev.shard]}
                                ELSE flags
                    /\ UNCHANGED hwmPending
          ELSE UNCHANGED <<ctx, listenerActive, hwmPending, timers, regexTask, respNet, acked, flags>>
    /\ lq' = [lq EXCEPT ![b] = Tail(@)]
    /\ UNCHANGED <<logs, leader, leaderEpoch, hwm, jobs, clock, client, net, nextShard, nextUid>>

\* CLI:120-170 loader thread: replay one batch into the job's shard. The shard
\* is the context's coordinator if the context still uses it; replay happens
\* without the context lock (concurrently with event processing).
LoaderStep(b) ==
    /\ jobs[b] # <<>>
    /\ LET j == Head(jobs[b])
           c == ctx[b]
       IN /\ ~j.err
          /\ b = leader                              \* logEndOffset(tp) == -1 otherwise (CLI:179)
          /\ j.pos < Len(logs[b])                    \* shouldFetchNextBatch CLI:194
          /\ IF c.shard = j.shard /\ c.st # NONE
             THEN LET rr  == ReplaySeq(OkRes(c.gs), logs[b][j.pos + 1].recs)
                      off == j.pos + 1
                      bad == rr.err \/ off <= c.written                         \* SC:137-140 updateLastWrittenOffset
                  IN /\ ctx' = [ctx EXCEPT ![b] =
                                  IF bad THEN c
                                  ELSE [c EXCEPT !.gs = rr.g, !.written = off,
                                                 !.committed = IF off >= hwm THEN Max(c.committed, hwm)
                                                               ELSE c.committed]]   \* CLI:302-313
                     /\ jobs' = [jobs EXCEPT ![b][1] = [j EXCEPT !.pos = off, !.err = bad]]
                     /\ flags' = IF rr.err THEN flags \cup {[kind |-> "ReplayErrorOnLoad", b |-> b, shard |-> j.shard]}
                                 ELSE flags
             ELSE \* orphan shard: replay into a coordinator nobody uses any more
                  /\ jobs' = [jobs EXCEPT ![b][1] = [j EXCEPT !.pos = j.pos + 1]]
                  /\ UNCHANGED <<ctx, flags>>
    /\ UNCHANGED <<logVars, lq, softVars, clientVars, nextShard, nextUid, acked>>

\* CLI:163-170 load end: success iff still leader and no exception.
LoaderFinish(b) ==
    /\ jobs[b] # <<>>
    /\ LET j == Head(jobs[b]) IN
       /\ j.err \/ b # leader \/ j.pos >= Len(logs[b])
       /\ jobs' = [jobs EXCEPT ![b] = Tail(@)]
       /\ lq' = [lq EXCEPT ![b] = Append(@, [type |-> "CompleteLoad", shard |-> j.shard,
                                             ok |-> (b = leader /\ ~j.err)])]
    /\ UNCHANGED <<logVars, ctx, softVars, clientVars, ghostVars>>

-----------------------------------------------------------------------------
(* Write path (Scenarios 1-3)                                               *)

\* CR:690-760 flushCurrentBatch(). partitionWriter.append succeeds iff b is the
\* leader (no expected leader epoch). Ghost: the entry records whether the
\* batch was generated from the state obtained by replaying the log it is
\* appended to (AppendFromLogState).
Flush(b) ==
    LET c      == ctx[b]
        entry  == [recs |-> c.batch.recs, epoch |-> leaderEpoch, uid |-> nextUid,
                   consistent |-> (c.batch.base = Len(logs[b])
                                   /\ ReplayLog(logs[b]) = OkRes(Proj(c.batch.baseGs)))]
        offset == Len(logs[b]) + 1
    IN
    /\ c.st = ACTIVE
    /\ c.batch # Null
    /\ IF b # leader THEN
          \* NotLeaderOrFollowerException -> failCurrentBatch (CR:744-760)
          /\ ctx' = [ctx EXCEPT ![b] = [c EXCEPT !.gs = c.batch.baseGs, !.batch = Null]]
          /\ ApplyOuts(b, FailOps(Range(c.batch.ops), NOT_LEADER), timers[b], c.shard)
          /\ UNCHANGED <<logs, lq, jobs, listenerActive, regexTask, nextShard, nextUid>>
       ELSE
          /\ logs' = [logs EXCEPT ![b] = Append(@, entry)]
          /\ nextUid' = nextUid + 1
          /\ IF offset <= c.written THEN
                \* SC:137-140 throws before the out-of-sync check -> failCurrentBatch:
                \* records stay in the log, the state is reverted, context stays ACTIVE
                /\ ctx' = [ctx EXCEPT ![b] = [c EXCEPT !.gs = c.batch.baseGs, !.batch = Null]]
                /\ ApplyOuts(b, FailOps(Range(c.batch.ops), GENERIC), timers[b], c.shard)
                /\ UNCHANGED <<lq, jobs, listenerActive, regexTask, nextShard>>
             ELSE IF offset # c.batch.base + 1 THEN
                \* CR:720-735 out of sync: FAILED (unload) then LOADING (new shard)
                /\ ctx' = [ctx EXCEPT ![b] = [NoCtx EXCEPT !.st = LOADING, !.epoch = c.epoch, !.shard = nextShard]]
                /\ jobs' = [jobs EXCEPT ![b] = Append(@, [shard |-> nextShard, pos |-> 0, err |-> FALSE])]
                /\ nextShard' = nextShard + 1
                /\ listenerActive' = [listenerActive EXCEPT ![b] = FALSE]
                /\ regexTask' = [regexTask EXCEPT ![b] = Null]
                /\ ApplyOuts(b, UnloadOuts(c), {}, 0)
                /\ UNCHANGED lq
             ELSE
                \* CR:738-742 deferred until the HWM passes the offset
                /\ ctx' = [ctx EXCEPT ![b] = [c EXCEPT !.written = offset, !.batch = Null,
                                               !.dq = @ \cup {[off |-> offset, op |-> o] : o \in Range(c.batch.ops)}]]
                /\ UNCHANGED <<respNet, timers, acked, lq, jobs, listenerActive, regexTask, nextShard>>
    /\ UNCHANGED <<leader, leaderEpoch, hwm, hwmPending, clock, client, net, flags>>

\* Fault: append fails with a non-leadership error (RecordTooLarge, storage
\* error, InvalidProducerEpoch from a coalesced transactional write):
\* failCurrentBatch reverts the timeline state of every op in the batch,
\* but not timers (CR:751-760, CR:783-790).
FlushFail(b) ==
    LET c == ctx[b] IN
    /\ c.st = ACTIVE
    /\ c.batch # Null
    /\ ctx' = [ctx EXCEPT ![b] = [c EXCEPT !.gs = c.batch.baseGs, !.batch = Null]]
    /\ ApplyOuts(b, FailOps(Range(c.batch.ops), GENERIC), timers[b], c.shard)
    /\ UNCHANGED <<logVars, lq, jobs, regexTask, clock, client, net, nextShard, nextUid, flags>>

\* CR:1809-1835 HighWatermarkUpdate event (front of the queue): applies to
\* coordinators.get(tp) if ACTIVE; completes deferred events up to the HWM.
HWMDeliver(b) ==
    /\ hwmPending[b]
    /\ listenerActive[b]
    /\ hwmPending' = [hwmPending EXCEPT ![b] = FALSE]
    /\ LET c == ctx[b] IN
       IF c.st = ACTIVE /\ hwm >= c.committed /\ hwm <= c.written     \* SC:150-160 else IllegalStateException (logged)
       THEN LET rel == {d \in c.dq : d.off <= hwm}
            IN /\ ctx' = [ctx EXCEPT ![b] = [c EXCEPT !.committed = hwm, !.dq = @ \ rel]]
               /\ ApplyOuts(b, {Out(d.op, TRUE, NOERR, logs[b][d.off].uid) : d \in rel}, timers[b], c.shard)
       ELSE UNCHANGED <<ctx, respNet, timers, acked>>
    /\ UNCHANGED <<logs, leader, leaderEpoch, hwm, listenerActive, lq, jobs, regexTask, clock,
                   client, net, nextShard, nextUid, flags>>

\* CR:1121-1140 OperationTimeout: a deferred write fails with a timeout while
\* its records stay applied and written.
OpTimeout(b) ==
    LET c == ctx[b] IN
    /\ c.st = ACTIVE
    /\ \E d \in c.dq :
        /\ ctx' = [ctx EXCEPT ![b].dq = @ \ {d}]
        /\ ApplyOuts(b, {Out(d.op, FALSE, TIMEOUT, 0)}, timers[b], c.shard)
    /\ UNCHANGED <<logVars, lq, jobs, regexTask, clock, client, net, nextShard, nextUid, flags>>

-----------------------------------------------------------------------------
(* Request processing at broker b (CoordinatorWriteEvent.run CR:1317-1347)  *)

\* Errors returned before generating records (withActiveContextOrThrow or
\* ApiException from the handler) complete the request immediately.
ImmediateErr(req, e) ==
    [m |-> req.m, reqId |-> req.id, ok |-> FALSE, err |-> e, epoch |-> 0, assign |-> Null]

ConsumerGroupHeartbeat(b, req) ==
    LET c == ctx[b] IN
    /\ req \in net
    /\ net' = net \ {req}
    /\ IF c.st # ACTIVE THEN
          /\ respNet' = respNet \cup {ImmediateErr(req, IF c.st = LOADING THEN LOAD_IN_PROGRESS ELSE NOT_COORDINATOR)}
          /\ UNCHANGED <<ctx, timers, regexTask, acked>>
       ELSE IF req.epoch = LEAVE_EPOCH THEN
          LET res == LeaveGen(c.gs, req) IN
          IF res.err # NOERR THEN
             /\ respNet' = respNet \cup {ImmediateErr(req, res.err)}
             /\ UNCHANGED <<ctx, timers, regexTask, acked>>
          ELSE
             LET op == [kind |-> "hb", m |-> req.m, reqId |-> req.id, resp |-> res.resp]
                 ar == AppendRes(c, res.recs, op)
                 ts == CancelMember(timers[b], req.m)                              \* GMM:5104-5106
             IN /\ ctx' = [ctx EXCEPT ![b] = ar.c]
                /\ ApplyOuts(b, ar.outs, ts, c.shard)
                /\ UNCHANGED regexTask
       ELSE
          LET hv    == HbView(c.gs, req)
              cands == IF HbGen(c.gs, req, clock, NullAsg, TRUE).computed
                       THEN AssignChoices(hv.view, hv.subsOf, c.gs.resolved)
                       ELSE {NullAsg}
          IN \E asg \in cands :
             LET res == HbGen(c.gs, req, clock, asg, regexTask[b] # Null) IN
             IF res.err # NOERR THEN
                /\ respNet' = respNet \cup {ImmediateErr(req, res.err)}
                /\ UNCHANGED <<ctx, timers, regexTask, acked>>
             ELSE
                LET op == [kind |-> "hb", m |-> req.m, reqId |-> req.id, resp |-> res.resp]
                    ar == AppendRes(c, res.recs, op)
                    \* GMM:4290 updateSubscriptionMetadata resets the (non-reverted) refresh deadline
                    c2 == IF res.refreshed THEN [ar.c EXCEPT !.gs.refreshDue = FALSE] ELSE ar.c
                IN /\ ctx' = [ctx EXCEPT ![b] = c2]
                   /\ ApplyOuts(b, ar.outs, HbTimers(timers[b], req, res, c.shard), c.shard)
                   /\ regexTask' = IF res.schedRegex # Null
                                   THEN [regexTask EXCEPT ![b] = [shard |-> c.shard, regexes |-> res.schedRegex]]
                                   ELSE regexTask
    /\ UNCHANGED <<logVars, lq, jobs, clock, client, nextShard, nextUid, flags>>

PendingOps(c) == {d.op : d \in c.dq} \cup (IF c.batch = Null THEN {} ELSE Range(c.batch.ops))
PendingWriteOf(c, m) == \E o \in PendingOps(c) : o.kind = "hb" /\ o.m = m

\* CTI:80-128 timer expiry -> write event: tasks.remove(key, this) then the
\* timeout operation. Session: consumerGroupFenceMemberOperation (GMM:5406-5427,
\* no epoch check). Rebalance: fence iff member epoch == scheduled epoch (GMM:5557-5590).
TimerFire(b, t) ==
    LET c  == ctx[b]
        gs == c.gs
        m  == t.m
        doFence == gs.exists /\ gs.members[m] # Null
                   /\ (t.kind = "session" \/ gs.members[m].epoch = t.epoch)
        recs == IF doFence THEN FenceRecs(gs, m) ELSE <<>>
        op == [kind |-> "timer", tkind |-> t.kind, m |-> m, tepoch |-> t.epoch, tinc |-> t.inc]
        ar == AppendRes(c, recs, op)
        ts0 == timers[b] \ {t}
        ts == IF doFence THEN CancelMember(ts0, m) ELSE ts0
    IN
    /\ c.st = ACTIVE
    /\ t \in timers[b]
    \* Timing abstraction: a session (45 s) or rebalance timeout scheduled by a
    \* heartbeat cannot expire while a write of that member is still pending,
    \* since pending writes complete or time out first (write timeout, seconds).
    \* Retry tasks (500 ms backoff) are not constrained.
    /\ t.retry \/ ~PendingWriteOf(c, m)
    /\ ctx' = [ctx EXCEPT ![b] = ar.c]
    /\ ApplyOuts(b, ar.outs, ts, c.shard)
    /\ flags' = flags \cup
         (IF doFence /\ t.kind = "session" /\ gs.members[m].inc # t.inc
          THEN {[kind |-> "SessionFenceOtherIncarnation", b |-> b, m |-> m]} ELSE {})
         \cup
         (IF doFence /\ t.kind = "rebalance" /\ gs.members[m].state # UNREVOKED
          THEN {[kind |-> "RebalanceFenceNotUnrevoked", b |-> b, m |-> m]} ELSE {})
    /\ UNCHANGED <<logVars, lq, jobs, regexTask, clock, clientVars, nextShard, nextUid>>

\* CoordinatorExecutorImpl: the regex resolution task completes and its result
\* is applied as a write operation on the same shard.
RegexResolve(b) ==
    LET c  == ctx[b]
        tk == regexTask[b]
    IN
    /\ tk # Null
    /\ c.st = ACTIVE
    /\ tk.shard = c.shard
    /\ LET recs == RegexResultRecs(c.gs, tk.regexes)
           op   == [kind |-> "regex"]
           ar   == AppendRes(c, recs, op)
       IN /\ ctx' = [ctx EXCEPT ![b] = ar.c]
          /\ ApplyOuts(b, ar.outs, timers[b], c.shard)
    /\ regexTask' = [regexTask EXCEPT ![b] = Null]
    /\ UNCHANGED <<logVars, lq, jobs, clock, client, net, nextShard, nextUid, flags>>

\* ModernGroup.metadataRefreshDeadline elapses (METADATA_REFRESH_INTERVAL_MS).
MetadataRefreshDue(b) ==
    /\ ctx[b].st = ACTIVE
    /\ ~ctx[b].gs.refreshDue
    /\ ctx' = [ctx EXCEPT ![b].gs.refreshDue = TRUE]
    /\ UNCHANGED <<logVars, lq, jobs, softVars, clientVars, ghostVars>>

\* Wall clock advances (assignment interval).
Tick ==
    /\ clock' = clock + 1
    /\ UNCHANGED <<logVars, rtVars, timers, regexTask, clientVars, ghostVars>>

-----------------------------------------------------------------------------
(* Client actions (Scenario 3)                                              *)

Send(m, cl, e, owned, sub, full) ==
    /\ net' = net \cup {Req(m, cl, e, owned, sub, full)}
    /\ client' = [client EXCEPT ![m] = [cl EXCEPT !.waiting = cl.next, !.next = cl.next + 1]]

\* Join (or rejoin after fencing / leaving) with epoch 0 and the same member id.
ClientJoin(m) ==
    LET cl == client[m] IN
    /\ cl.st = "out"
    /\ cl.waiting = 0
    /\ LET cl2 == [cl EXCEPT !.st = "in", !.inc = @ + 1, !.epoch = 0, !.owned = {}, !.assignment = {},
                             !.needFull = TRUE]
       IN Send(m, cl2, 0, {}, cl2.sub, TRUE)
    /\ UNCHANGED <<logVars, rtVars, softVars, respNet, ghostVars>>

\* Regular heartbeat. A full request carries the subscription and owned set;
\* otherwise owned may be omitted (null = unchanged).
ClientHeartbeat(m) ==
    LET cl == client[m] IN
    /\ cl.st = "in"
    /\ cl.waiting = 0
    /\ IF cl.needFull
       THEN Send(m, cl, cl.epoch, cl.owned, cl.sub, TRUE)
       ELSE \E owned \in {cl.owned, Null} : Send(m, cl, cl.epoch, owned, Null, FALSE)
    /\ UNCHANGED <<logVars, rtVars, softVars, respNet, ghostVars>>

ClientChangeSubscription(m, s) ==
    /\ client[m].st = "in"
    /\ s # client[m].sub
    /\ client' = [client EXCEPT ![m].sub = s, ![m].needFull = TRUE]
    /\ UNCHANGED <<logVars, rtVars, softVars, net, respNet, ghostVars>>

\* onPartitionsRevoked completes: drop partitions no longer assigned.
ClientRevoke(m) ==
    /\ ~(client[m].owned \subseteq client[m].assignment)
    /\ client' = [client EXCEPT ![m].owned = @ \cap client[m].assignment]
    /\ UNCHANGED <<logVars, rtVars, softVars, net, respNet, ghostVars>>

\* onPartitionsAssigned: start consuming the new partitions after revocation.
ClientAssign(m) ==
    /\ client[m].owned \subseteq client[m].assignment
    /\ client[m].owned # client[m].assignment
    /\ client' = [client EXCEPT ![m].owned = client[m].assignment]
    /\ UNCHANGED <<logVars, rtVars, softVars, net, respNet, ghostVars>>

\* Leave: revoke everything, then send epoch -1 (best effort, not retried).
ClientLeave(m) ==
    LET cl == client[m] IN
    /\ cl.st = "in"
    /\ cl.waiting = 0
    /\ LET cl2 == [cl EXCEPT !.st = "leaving", !.owned = {}, !.assignment = {}]
       IN Send(m, cl2, LEAVE_EPOCH, Null, Null, FALSE)
    /\ UNCHANGED <<logVars, rtVars, softVars, respNet, ghostVars>>

\* Client-side request timeout: stop waiting; the request may still be delivered.
ClientRequestTimeout(m) ==
    /\ client[m].waiting # 0
    /\ client' = [client EXCEPT ![m].waiting = 0, ![m].needFull = TRUE,
                                ![m].st = IF @ = "leaving" THEN "out" ELSE @]
    /\ UNCHANGED <<logVars, rtVars, softVars, net, respNet, ghostVars>>

ClientReceive(m, r) ==
    LET cl == client[m] IN
    /\ r \in respNet
    /\ r.m = m
    /\ respNet' = respNet \ {r}
    /\ client' =
         IF r.reqId # cl.waiting THEN client                                 \* stale response: ignored
         ELSE IF cl.st = "leaving" THEN [client EXCEPT ![m] = [cl EXCEPT !.waiting = 0, !.st = "out", !.epoch = -1]]
         ELSE IF r.ok THEN
              [client EXCEPT ![m] = [cl EXCEPT !.waiting = 0, !.epoch = r.epoch, !.needFull = FALSE,
                                               !.assignment = IF r.assign = Null THEN @ ELSE r.assign]]
         ELSE IF FatalErr(r.err) THEN
              \* abandon all partitions and rejoin with epoch 0 (same member id)
              [client EXCEPT ![m] = [cl EXCEPT !.waiting = 0, !.st = "out", !.owned = {},
                                               !.assignment = {}, !.epoch = -1]]
         ELSE [client EXCEPT ![m] = [cl EXCEPT !.waiting = 0, !.needFull = TRUE]]   \* retriable
    /\ UNCHANGED <<logVars, rtVars, softVars, net, ghostVars>>

LoseRequest(req) ==
    /\ req \in net
    /\ net' = net \ {req}
    /\ UNCHANGED <<logVars, rtVars, softVars, client, respNet, ghostVars>>

LoseResponse(r) ==
    /\ r \in respNet
    /\ respNet' = respNet \ {r}
    /\ UNCHANGED <<logVars, rtVars, softVars, client, net, ghostVars>>


-----------------------------------------------------------------------------
(* Invariants                                                               *)

CommittedRes == ReplayLog(SubSeq(logs[leader], 1, hwm))
CommittedGS  == CommittedRes.g

Own(gs, m) == gs.members[m].assigned \cup gs.members[m].pending

OwnershipExclusive(gs) ==
    \A m, n \in MembersOf(gs) : m # n => Own(gs, m) \cap Own(gs, n) = {}

ActiveGS == {ctx[b].gs : b \in {x \in Brokers : ctx[x].st = ACTIVE}}

\* Q1 (brief section 5): a partition is in at most one member's assigned or
\* pending set, in every active coordinator and in the committed state.
\* Checked on member records, not on currentPartitionEpoch (CR-1).
PartitionOwnershipExclusive ==
    /\ \A g \in ActiveGS : OwnershipExclusive(g)
    /\ OwnershipExclusive(CommittedGS)

\* Q1 client view: clients that are live members of the committed group own
\* disjoint partitions (fenced zombies are allowed to overlap by design).
\* A client is live (not a zombie) if a heartbeat carrying its epoch and
\* owned set would be accepted by the committed group state (GMM:1673-1694).
LiveClient(g, m) ==
    /\ client[m].st = "in"
    /\ g.members[m] # Null
    /\ client[m].epoch > 0
    /\ EpochValid(g.members[m], client[m].epoch, client[m].owned)
ClientOwnershipExclusive ==
    LET g == CommittedGS IN
    \A m, n \in Members :
        (m # n /\ LiveClient(g, m) /\ LiveClient(g, n)) => client[m].owned \cap client[n].owned = {}

\* Q1 contract ("a partition moves to a new owner only after the old owner has
\* acknowledged revocation"): while the committed group still has member m,
\* every partition its client owns is still assigned to, or pending
\* revocation from, m. Fenced (removed) members are zombies and excluded.
OwnedImpliesAssigned ==
    LET g == CommittedGS IN
    \A m \in Members :
        (client[m].st = "in" /\ g.members[m] # Null) => client[m].owned \subseteq Own(g, m)

\* currentPartitionEpoch agrees with the member records.
PEConsistent(gs) ==
    \A p \in Partitions :
        IF \E m \in MembersOf(gs) : p \in Own(gs, m)
        THEN \E m \in MembersOf(gs) : p \in Own(gs, m) /\ gs.pEpoch[p] = gs.members[m].epoch
        ELSE gs.pEpoch[p] = -1
PartitionEpochMapConsistent == \A g \in ActiveGS : PEConsistent(g)

\* member epoch <= assignment epoch <= group epoch.
EpochBounds(gs) ==
    gs.exists =>
        /\ gs.assignEpoch <= gs.groupEpoch
        /\ \A m \in MembersOf(gs) : gs.members[m].epoch <= gs.assignEpoch
MemberEpochBounds ==
    /\ \A g \in ActiveGS : EpochBounds(g)
    /\ EpochBounds(CommittedGS)

\* MC-1: an ACTIVE context never has a loader job still replaying into its shard,
\* and a load completion is never applied to a context other than the one it loaded.
NoActiveWhileLoading ==
    \A b \in Brokers :
        ctx[b].st = ACTIVE => \A i \in DOMAIN jobs[b] : jobs[b][i].shard # ctx[b].shard
NoStaleLoadCompletion == \A f \in flags : f.kind # "StaleLoadCompletion"

\* MC-1/MC-2/Q2: every appended batch was generated from the state obtained by
\* replaying the log it was appended to.
AppendFromLogState == \A b \in Brokers : \A i \in DOMAIN logs[b] : logs[b][i].consistent

\* Q2: the committed log always replays without IllegalStateException.
CommittedReplayOk == ~CommittedRes.err
NoReplayErrorOnLoad == \A f \in flags : f.kind # "ReplayErrorOnLoad"

\* Q3: a successfully completed write is in the committed log.
AckedImpliesCommitted ==
    \A u \in acked : \E i \in 1..hwm : logs[leader][i].uid = u

\* MC-3/MC-4a: timer fences only hit the incarnation / state they were meant for.
NoSpuriousFence == \A f \in flags : f.kind \notin {"SessionFenceOtherIncarnation", "RebalanceFenceNotUnrevoked"}

\* MC-4b: every member of an active coordinator has a session timer.
TimersCoverMembers ==
    \A b \in Brokers :
        ctx[b].st = ACTIVE =>
            \A m \in MembersOf(ctx[b].gs) : \E t \in timers[b] : t.kind = "session" /\ t.m = m

\* MC-6 / Q5: a group that reports STABLE with all regexes resolved has every
\* subscribed partition assigned to some member.
GroupStable(gs) ==
    /\ gs.exists
    /\ MembersOf(gs) # {}
    /\ gs.groupEpoch <= gs.assignEpoch                                   \* CG:963-979
    /\ \A m \in MembersOf(gs) : IsReconciledTo(gs.members[m], gs.assignEpoch)
AllRegexResolved(gs) == \A r \in Regexes : NumSub(gs, r) > 0 => gs.resolved[r] # Null
FullyAssigned(gs) ==
    \A p \in PartsOf(SubscribedTopics(gs)) : \E m \in MembersOf(gs) : p \in gs.members[m].assigned
StableImpliesFullyAssigned ==
    \A b \in Brokers :
        LET g == ctx[b].gs IN
        (ctx[b].st = ACTIVE /\ regexTask[b] = Null /\ GroupStable(g) /\ AllRegexResolved(g))
            => FullyAssigned(g)

\* Structural
TypeOK ==
    /\ leader \in Brokers
    /\ hwm \in 0..Len(logs[leader])
    /\ \A b \in Brokers : ctx[b].st \in {NONE, INITIAL, LOADING, ACTIVE, FAILED}
    /\ \A b \in Brokers : ctx[b].committed <= ctx[b].written
    /\ \A m \in Members : client[m].st \in {"out", "in", "leaving"}

-----------------------------------------------------------------------------
Next ==
    \/ \E f \in Brokers : Replicate(f)
    \/ AdvanceHWM
    \/ \E n \in Brokers : LeaderChange(n)
    \/ \E b \in Brokers :
         \/ ProcessLoad(b) \/ ProcessUnload(b) \/ ProcessCompleteLoad(b)
         \/ LoaderStep(b) \/ LoaderFinish(b)
         \/ Flush(b) \/ FlushFail(b) \/ HWMDeliver(b) \/ OpTimeout(b)
         \/ \E req \in net : ConsumerGroupHeartbeat(b, req)
         \/ \E t \in timers[b] : TimerFire(b, t)
         \/ RegexResolve(b) \/ MetadataRefreshDue(b)
    \/ Tick
    \/ \E m \in Members :
         \/ ClientJoin(m) \/ ClientHeartbeat(m) \/ ClientRevoke(m) \/ ClientAssign(m)
         \/ ClientLeave(m) \/ ClientRequestTimeout(m)
         \/ \E s \in Subscriptions : ClientChangeSubscription(m, s)
         \/ \E r \in respNet : ClientReceive(m, r)
    \/ \E req \in net : LoseRequest(req)
    \/ \E r \in respNet : LoseResponse(r)

Spec == Init /\ [][Next]_vars

=============================================================================
