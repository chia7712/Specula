------------------------------- MODULE Trace -------------------------------
(***************************************************************************)
(* Trace validation for base.tla (Category A: single linear NDJSON trace). *)
(*                                                                         *)
(* The harness (see instrumentation-spec.md) runs the real                 *)
(* GroupMetadataManager / CoordinatorRuntime inside a JUnit scenario with  *)
(* a mock partition writer and loader, and emits one event per spec        *)
(* action. Names are mapped by the harness: brokers "b1".., members        *)
(* "m1".., topics "t1".., partitions "p1".., regexes "A"..; null -> "null".*)
(*                                                                         *)
(* Event envelope: {"tag":"trace","event":{"name":..,"b":..,...,"post":{..}}}*)
(***************************************************************************)

EXTENDS base, Json, IOUtils, Sequences, TLC

JsonFile ==
    IF "JSON" \in DOMAIN IOEnv THEN IOEnv.JSON
    ELSE "../traces/trace.ndjson"

TraceLog == TLCEval(
    LET all == ndJsonDeserialize(JsonFile)
    IN SelectSeq(all, LAMBDA x : "tag" \in DOMAIN x /\ x.tag = "trace" /\ "event" \in DOMAIN x))

ASSUME Len(TraceLog) > 0

VARIABLE l
traceVars == <<l>>

ev == TraceLog[l].event

-----------------------------------------------------------------------------
(* Value mapping *)

\* Optional values are encoded by omitting the field (never a "null" string,
\* TLC cannot compare strings with records or sequences).
Has(r, f) == f \in DOMAIN r
SetOf(s)   == {s[i] : i \in DOMAIN s}
SubOf(x)   == [names |-> SetOf(x.names), regex |-> IF x.regex = "" THEN NoRegex ELSE x.regex]
OptSetF(r, f) == IF Has(r, f) THEN SetOf(r[f]) ELSE Null
OptSubF(r, f) == IF Has(r, f) THEN SubOf(r[f]) ELSE Null
StateOf(s) == CASE s = "STABLE" -> STABLE
                [] s = "UNREVOKED_PARTITIONS" -> UNREVOKED
                [] s = "UNRELEASED_PARTITIONS" -> UNRELEASED

IsEvent(name) == l <= Len(TraceLog) /\ ev.name = name

-----------------------------------------------------------------------------
(* Post-state validation (fields defined in instrumentation-spec.md S1)    *)

\* Context fields captured after every runtime event.
CtxOk(b, p) ==
    /\ ctx'[b].st = p.st
    /\ (Has(p, "epoch") => ctx'[b].epoch = p.epoch)
    /\ (Has(p, "written") => ctx'[b].written = p.written)
    /\ (Has(p, "committed") => ctx'[b].committed = p.committed)

\* Group fields of the in-memory (latest) state.
GroupOk(b, p) ==
    LET g == ctx'[b].gs IN
    /\ (Has(p, "groupEpoch") => g.groupEpoch = p.groupEpoch)
    /\ (Has(p, "assignEpoch") => g.assignEpoch = p.assignEpoch)
    /\ (Has(p, "members") => MembersOf(g) = SetOf(p.members))

\* Member fields of member m ("memberAbsent": true when the member is gone).
MemberOk(b, m, p) ==
    LET g == ctx'[b].gs IN
    /\ (Has(p, "memberAbsent") => g.members[m] = Null)
    /\ Has(p, "member") =>
             /\ g.members[m] # Null
             /\ g.members[m].epoch = p.member.epoch
             /\ g.members[m].prev = p.member.prev
             /\ g.members[m].state = StateOf(p.member.state)
             /\ g.members[m].assigned = SetOf(p.member.assigned)
             /\ g.members[m].pending = SetOf(p.member.pending)
             /\ (Has(p.member, "target") => g.target[m] = SetOf(p.member.target))
             /\ (Has(p.member, "targetAbsent") => g.target[m] = Null)

LogOk(p) ==
    /\ (Has(p, "leader") => leader' = p.leader)
    /\ (Has(p, "leaderEpoch") => leaderEpoch' = p.leaderEpoch)
    /\ (Has(p, "hwm") => hwm' = p.hwm)
    /\ (Has(p, "logLen") => \A b \in Brokers : Has(p.logLen, b) => Len(logs'[b]) = p.logLen[b])

-----------------------------------------------------------------------------
(* Action wrappers: match event -> base action -> validate -> advance       *)

TraceReplicate ==
    /\ IsEvent("Replicate")
    /\ Replicate(ev.b)
    /\ LogOk(ev.post)
    /\ l' = l + 1

TraceAdvanceHWM ==
    /\ IsEvent("AdvanceHWM")
    /\ AdvanceHWM
    /\ hwm' = ev.hwm
    /\ l' = l + 1

TraceLeaderChange ==
    /\ IsEvent("LeaderChange")
    /\ LeaderChange(ev.b)
    /\ LogOk(ev.post)
    /\ l' = l + 1

TraceProcessLoad ==
    /\ IsEvent("Load")
    /\ ProcessLoad(ev.b)
    /\ CtxOk(ev.b, ev.post)
    /\ l' = l + 1

TraceProcessUnload ==
    /\ IsEvent("Unload")
    /\ ProcessUnload(ev.b)
    /\ CtxOk(ev.b, ev.post)
    /\ l' = l + 1

TraceProcessCompleteLoad ==
    /\ IsEvent("CompleteLoad")
    /\ Head(lq[ev.b]).type = "CompleteLoad"
    /\ Head(lq[ev.b]).ok = ev.ok
    /\ ProcessCompleteLoad(ev.b)
    /\ CtxOk(ev.b, ev.post)
    /\ GroupOk(ev.b, ev.post)
    /\ l' = l + 1

TraceLoaderStep ==
    /\ IsEvent("LoaderStep")
    /\ LoaderStep(ev.b)
    /\ Head(jobs'[ev.b]).pos = ev.offset
    /\ l' = l + 1

Last(s) == s[Len(s)]

TraceLoaderFinish ==
    /\ IsEvent("LoaderFinish")
    /\ LoaderFinish(ev.b)
    /\ Last(lq'[ev.b]).ok = ev.ok
    /\ l' = l + 1

TraceFlush ==
    /\ IsEvent("Flush")
    /\ Flush(ev.b)
    /\ CtxOk(ev.b, ev.post)
    /\ LogOk(ev.post)
    /\ l' = l + 1

TraceFlushFail ==
    /\ IsEvent("FlushFail")
    /\ FlushFail(ev.b)
    /\ CtxOk(ev.b, ev.post)
    /\ GroupOk(ev.b, ev.post)
    /\ l' = l + 1

TraceHWMDeliver ==
    /\ IsEvent("HWMDeliver")
    /\ HWMDeliver(ev.b)
    /\ CtxOk(ev.b, ev.post)
    /\ l' = l + 1

TraceOpTimeout ==
    /\ IsEvent("OpTimeout")
    /\ OpTimeout(ev.b)
    /\ CtxOk(ev.b, ev.post)
    /\ l' = l + 1

\* The heartbeat event carries the request id, the outcome (error code or
\* response) and the post state of the member and group. The assignor's
\* choice is resolved by the captured target assignment ("targets").
TraceConsumerGroupHeartbeat ==
    /\ IsEvent("Heartbeat")
    /\ \E req \in net :
         /\ req.m = ev.m
         /\ req.id = ev.reqId
         /\ ConsumerGroupHeartbeat(ev.b, req)
    /\ (Has(ev, "targets") =>
          \A m \in Members : Has(ev.targets, m) => ctx'[ev.b].gs.target[m] = SetOf(ev.targets[m]))
    /\ (ev.err # "NONE" => \E r \in respNet' \ respNet : r.reqId = ev.reqId /\ r.m = ev.m /\ r.err = ev.err)
    /\ GroupOk(ev.b, ev.post)
    /\ MemberOk(ev.b, ev.m, ev.post)
    /\ l' = l + 1

TraceTimerFire ==
    /\ IsEvent("TimerFire")
    /\ \E t \in timers[ev.b] :
         /\ t.kind = ev.kind
         /\ t.m = ev.m
         /\ TimerFire(ev.b, t)
    /\ GroupOk(ev.b, ev.post)
    /\ MemberOk(ev.b, ev.m, ev.post)
    /\ l' = l + 1

TraceRegexResolve ==
    /\ IsEvent("RegexResolve")
    /\ RegexResolve(ev.b)
    /\ GroupOk(ev.b, ev.post)
    /\ (Has(ev.post, "resolved") =>
          \A r \in Regexes : Has(ev.post.resolved, r) => ctx'[ev.b].gs.resolved[r] = SetOf(ev.post.resolved[r]))
    /\ l' = l + 1

TraceTick ==
    /\ IsEvent("Tick")
    /\ Tick
    /\ l' = l + 1

TraceMetadataRefreshDue ==
    /\ IsEvent("MetadataRefreshDue")
    /\ MetadataRefreshDue(ev.b)
    /\ l' = l + 1

\* ---- client side (the JUnit scenario plays a well-behaved client) ----

ClientOk(m, p) ==
    /\ (Has(p, "epoch") => client'[m].epoch = p.epoch)
    /\ (Has(p, "owned") => client'[m].owned = SetOf(p.owned))
    /\ (Has(p, "assignment") => client'[m].assignment = SetOf(p.assignment))

\* The emitted request must be exactly the one the client model sends.
SentOk ==
    \E r \in net' \ net :
        /\ r.m = ev.m /\ r.id = ev.reqId /\ r.epoch = ev.epoch
        /\ r.owned = OptSetF(ev, "owned")
        /\ r.sub = OptSubF(ev, "sub")
        /\ r.full = ev.full

TraceClientJoin ==
    /\ IsEvent("ClientJoin")
    /\ ClientJoin(ev.m)
    /\ SentOk
    /\ l' = l + 1

TraceClientHeartbeat ==
    /\ IsEvent("ClientHeartbeat")
    /\ ClientHeartbeat(ev.m)
    /\ SentOk
    /\ l' = l + 1

TraceClientLeave ==
    /\ IsEvent("ClientLeave")
    /\ ClientLeave(ev.m)
    /\ SentOk
    /\ l' = l + 1

TraceClientChangeSubscription ==
    /\ IsEvent("ClientChangeSubscription")
    /\ ClientChangeSubscription(ev.m, SubOf(ev.sub))
    /\ l' = l + 1

TraceClientReceive ==
    /\ IsEvent("ClientReceive")
    /\ \E r \in respNet : r.m = ev.m /\ r.reqId = ev.reqId /\ ClientReceive(ev.m, r)
    /\ ClientOk(ev.m, ev.post)
    /\ l' = l + 1

TraceClientRevoke ==
    /\ IsEvent("ClientRevoke")
    /\ ClientRevoke(ev.m)
    /\ ClientOk(ev.m, ev.post)
    /\ l' = l + 1

TraceClientAssign ==
    /\ IsEvent("ClientAssign")
    /\ ClientAssign(ev.m)
    /\ ClientOk(ev.m, ev.post)
    /\ l' = l + 1

TraceClientRequestTimeout ==
    /\ IsEvent("ClientRequestTimeout")
    /\ ClientRequestTimeout(ev.m)
    /\ l' = l + 1

-----------------------------------------------------------------------------
(* Silent actions (no trace event), tightly constrained                    *)

\* The metadata refresh deadline is wall-clock based (MockTime in the harness);
\* allow it only right before a Heartbeat event, at most once in a row.
SilentMetadataRefreshDue ==
    /\ l <= Len(TraceLog)
    /\ ev.name = "Heartbeat"
    /\ MetadataRefreshDue(ev.b)
    /\ UNCHANGED l

-----------------------------------------------------------------------------
(* Constants for Trace.cfg (scenario configuration of the harness).        *)
(* Per-run overrides: the harness writes the same names; the subscription  *)
(* set and initial subscriptions are read from the first "Config" event.   *)

ConfigEv == TLCEval(CHOOSE i \in DOMAIN TraceLog : TraceLog[i].event.name = "Config")
TraceTopicOf == [p \in Partitions |-> TraceLog[ConfigEv].event.topicOf[p]]
TraceRegexMatch == [r \in Regexes |->
                      IF Has(TraceLog[ConfigEv].event.regexMatch, r)
                      THEN SetOf(TraceLog[ConfigEv].event.regexMatch[r]) ELSE {}]
TraceSubscriptions == {SubOf(TraceLog[ConfigEv].event.subscriptions[i]) :
                         i \in DOMAIN TraceLog[ConfigEv].event.subscriptions}
TraceInitialSub == [m \in Members |-> SubOf(TraceLog[ConfigEv].event.initialSub[m])]

TraceConfig ==
    /\ IsEvent("Config")
    /\ UNCHANGED vars
    /\ l' = l + 1

-----------------------------------------------------------------------------
TraceInit == Init /\ l = 1

TraceNext ==
    \/ TraceReplicate \/ TraceAdvanceHWM \/ TraceLeaderChange
    \/ TraceProcessLoad \/ TraceProcessUnload \/ TraceProcessCompleteLoad
    \/ TraceLoaderStep \/ TraceLoaderFinish
    \/ TraceFlush \/ TraceFlushFail \/ TraceHWMDeliver \/ TraceOpTimeout
    \/ TraceConsumerGroupHeartbeat \/ TraceTimerFire \/ TraceRegexResolve
    \/ TraceTick \/ TraceMetadataRefreshDue
    \/ TraceClientJoin \/ TraceClientHeartbeat \/ TraceClientLeave
    \/ TraceClientChangeSubscription \/ TraceClientReceive
    \/ TraceClientRevoke \/ TraceClientAssign \/ TraceClientRequestTimeout
    \/ SilentMetadataRefreshDue
    \/ TraceConfig

TraceSpec == TraceInit /\ [][TraceNext]_<<vars, l>> /\ WF_<<vars, l>>(TraceNext)

TraceMatched == <>(l > Len(TraceLog))

\* Debug helpers
TraceView == <<vars, l>>
TraceAlias == [l |-> l, event |-> IF l <= Len(TraceLog) THEN ev ELSE "done",
               ctx |-> ctx, client |-> client, net |-> net, respNet |-> respNet]

=============================================================================
