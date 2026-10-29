-------------------------------- MODULE MC --------------------------------
(***************************************************************************)
(* Model checking wrapper for base.tla (Kafka KIP-848 consumer groups on   *)
(* the coordinator runtime).                                               *)
(*                                                                         *)
(* Counter-bounded (they introduce nondeterminism):                         *)
(*   LeaderChange, FlushFail, OpTimeout, TimerFire, MetadataRefreshDue,    *)
(*   Tick, ClientJoin, ClientHeartbeat, ClientChangeSubscription,          *)
(*   ClientLeave, ClientRequestTimeout, LoseRequest, LoseResponse          *)
(* Unbounded (reactive):                                                    *)
(*   Replicate, AdvanceHWM, ProcessLoad/Unload/CompleteLoad, LoaderStep,   *)
(*   LoaderFinish, Flush, HWMDeliver, ConsumerGroupHeartbeat,              *)
(*   RegexResolve, ClientRevoke, ClientAssign, ClientReceive               *)
(***************************************************************************)

EXTENDS base

kip == INSTANCE base

CONSTANTS
    MaxLeaderChange,    \* leadership changes (Scenario 1; 2 = e1 -> e2 -> e3 flap)
    MaxFlushFail,       \* non-leadership append failures (Scenario 2)
    MaxOpTimeout,       \* deferred write timeouts (Scenario 2)
    MaxTimerFire,       \* session / rebalance timer expiries (Scenarios 2, 3)
    MaxRefresh,         \* metadata refresh deadline expiries (Scenario 4)
    MaxTick,            \* clock ticks (assignment interval)
    MaxJoin,            \* client (re)joins with epoch 0, all members
    MaxHeartbeat,       \* regular heartbeats, all members
    MaxSubChange,       \* subscription changes (Scenario 4)
    MaxLeave,           \* client leaves (Scenario 3)
    MaxReqTimeout,      \* client request timeouts -> late / duplicate delivery (Scenario 3)
    MaxLose,            \* lost requests or responses
    MaxLog              \* log length bound (state constraint)

VARIABLE fc

faultVars == <<fc>>

Bounded(k, lim) == fc[k] < lim
Inc(k) == fc' = [fc EXCEPT ![k] = @ + 1]

MCLeaderChange(n)            == Bounded("leader", MaxLeaderChange) /\ kip!LeaderChange(n) /\ Inc("leader")
MCFlushFail(b)               == Bounded("flushFail", MaxFlushFail) /\ kip!FlushFail(b) /\ Inc("flushFail")
MCOpTimeout(b)               == Bounded("opTimeout", MaxOpTimeout) /\ kip!OpTimeout(b) /\ Inc("opTimeout")
MCTimerFire(b, t)            == Bounded("timer", MaxTimerFire) /\ kip!TimerFire(b, t) /\ Inc("timer")
MCMetadataRefreshDue(b)      == Bounded("refresh", MaxRefresh) /\ kip!MetadataRefreshDue(b) /\ Inc("refresh")
MCTick                       == Bounded("tick", MaxTick) /\ kip!Tick /\ Inc("tick")
MCClientJoin(m)              == Bounded("join", MaxJoin) /\ kip!ClientJoin(m) /\ Inc("join")
MCClientHeartbeat(m)         == Bounded("hb", MaxHeartbeat) /\ kip!ClientHeartbeat(m) /\ Inc("hb")
MCClientChangeSubscription(m, s) ==
    Bounded("sub", MaxSubChange) /\ kip!ClientChangeSubscription(m, s) /\ Inc("sub")
MCClientLeave(m)             == Bounded("leave", MaxLeave) /\ kip!ClientLeave(m) /\ Inc("leave")
MCClientRequestTimeout(m)    == Bounded("reqTimeout", MaxReqTimeout) /\ kip!ClientRequestTimeout(m) /\ Inc("reqTimeout")
MCLoseRequest(r)             == Bounded("lose", MaxLose) /\ kip!LoseRequest(r) /\ Inc("lose")
MCLoseResponse(r)            == Bounded("lose", MaxLose) /\ kip!LoseResponse(r) /\ Inc("lose")

Reactive ==
    \/ \E f \in Brokers : kip!Replicate(f)
    \/ kip!AdvanceHWM
    \/ \E b \in Brokers :
         \/ kip!ProcessLoad(b) \/ kip!ProcessUnload(b) \/ kip!ProcessCompleteLoad(b)
         \/ kip!LoaderStep(b) \/ kip!LoaderFinish(b)
         \/ kip!Flush(b) \/ kip!HWMDeliver(b)
         \/ \E req \in net : kip!ConsumerGroupHeartbeat(b, req)
         \/ kip!RegexResolve(b)
    \/ \E m \in Members :
         \/ kip!ClientRevoke(m) \/ kip!ClientAssign(m)
         \/ \E r \in respNet : kip!ClientReceive(m, r)

Faults ==
    \/ \E n \in Brokers : MCLeaderChange(n)
    \/ \E b \in Brokers :
         \/ MCFlushFail(b) \/ MCOpTimeout(b) \/ MCMetadataRefreshDue(b)
         \/ \E t \in timers[b] : MCTimerFire(b, t)
    \/ MCTick
    \/ \E m \in Members :
         \/ MCClientJoin(m) \/ MCClientHeartbeat(m) \/ MCClientLeave(m) \/ MCClientRequestTimeout(m)
         \/ \E s \in Subscriptions : MCClientChangeSubscription(m, s)
    \/ \E r \in net : MCLoseRequest(r)
    \/ \E r \in respNet : MCLoseResponse(r)

MCInit ==
    /\ Init
    /\ fc = [k \in {"leader", "flushFail", "opTimeout", "timer", "refresh", "tick", "join", "hb",
                    "sub", "leave", "reqTimeout", "lose"} |-> 0]

MCNext ==
    \/ Reactive /\ UNCHANGED faultVars
    \/ Faults

MCSpec == MCInit /\ [][MCNext]_<<vars, faultVars>>

\* State constraint
MCConstraint == \A b \in Brokers : Len(logs[b]) <= MaxLog

\* Liveness (optional; only meaningful when the bounded counters do not
\* starve heartbeats): the partition leader eventually has an ACTIVE context.
CoordinatorEventuallyActive == <>(ctx[leader].st = ACTIVE)

-----------------------------------------------------------------------------
(* Concrete constants used by the cfg files *)

MCTopicOf2       == [p \in {"p1", "p2"} |-> IF p = "p1" THEN "t1" ELSE "t2"]
MCTopicOf1       == [p \in {"p1", "p2"} |-> "t1"]
MCRegexMatchNone == [r \in {} |-> {}]
MCRegexMatchAB   == [r \in {"A", "B"} |-> IF r = "A" THEN {"t1"} ELSE {}]

SubNames(ts) == [names |-> ts, regex |-> NoRegex]
SubRegex(r)  == [names |-> {}, regex |-> r]

MCSubsAll        == {SubNames({"t1", "t2"})}
MCSubsSplit      == {SubNames({"t1", "t2"}), SubNames({"t1"})}
MCInitialSubAll  == [m \in Members |-> SubNames({"t1", "t2"})]
MCSubsRegex      == {SubRegex("A"), SubRegex("B")}
MCInitialSubA    == [m \in Members |-> SubRegex("A")]

MCTopicOfP1      == [p \in {"p1"} |-> "t1"]
MCSubsT1         == {SubNames({"t1"})}
MCInitialSubT1   == [m \in Members |-> SubNames({"t1"})]

\* Symmetry over member ids (all members start with the same subscription).
MemberSymmetry == Permutations(Members)

=============================================================================
