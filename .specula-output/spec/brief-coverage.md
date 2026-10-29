# Brief Coverage Self-Audit

Sources: `../modeling-brief.md` §2, §5 and §6.1, checked against the cfg files as they exist on disk
(`MC.cfg` and `MC_hunt_*.cfg`). Spec suite: `base.tla`, `MC.tla`, `Trace.tla` and the cfgs in this
directory.

## Scenarios (brief §2)

| Scenario | Hunt cfg | Key bounds | Notes |
|---|---|---|---|
| 1. Load/unload lifecycle not fenced by leader epoch | `MC_hunt_lifecycle.cfg` | 2 brokers, `MaxLeaderChange=3` (e1→e4, so a broker can unload and reload while an older load is still in flight), 1 member, 1 partition | |
| 2. Timers vs durable/reverted state | `MC_hunt_timers.cfg` | 1 broker, 2 members, `MaxFlushFail=1`, `MaxOpTimeout=1`, `MaxTimerFire=2`, `MaxLeave=1` | |
| 3. Partition ownership under epoch rules and client retries | `MC_hunt_ownership.cfg` | 1 broker, 2 members, 2 partitions, `MaxReqTimeout=2` (late/duplicate delivery), `MaxJoin=3`, `MaxLeave=1`, `MaxLose=1` | |
| 4. Missing rebalance triggers (regex) | `MC_hunt_regex.cfg` | 1 broker, 2 members on regex A (→t1), `MaxSubChange=1` to regex B (→{}), `MaxRefresh=1` | Covers only the regex part, not the classic-only batching part (see §6.1, MC-6). |
| 5. Migration / static membership (secondary) | none (deliberate) | | The brief routes it to tests (§6.2 T-4, T-5). Static and classic members are excluded from `base.tla`. |

## Invariants (brief §5)

| Invariant (brief name) | Defined (`base.tla`) | Wired | Enabled in |
|---|---|---|---|
| PartitionOwnershipExclusive | `PartitionOwnershipExclusive` (active contexts and committed state, on member records) | base | MC, lifecycle, timers, ownership, regex, Trace |
| ClientOwnershipExclusive | `ClientOwnershipExclusive` (clients whose epoch and owned set are still accepted by the committed state) | base | ownership |
| *(added for the Q1 contract)* | `OwnedImpliesAssigned` (a live member's client owns only partitions still assigned to it or pending revocation from it) | base | ownership |
| PartitionEpochMapConsistent | `PartitionEpochMapConsistent` | base | MC, ownership, Trace |
| MemberEpochBounds | `MemberEpochBounds` (active and committed) | base | MC, lifecycle, timers, ownership, regex, Trace |
| NoActiveWhileLoading | `NoActiveWhileLoading`, plus the `NoStaleLoadCompletion` ghost flag | base | lifecycle, Trace (NoActiveWhileLoading) |
| AppendFromLogState | `AppendFromLogState` (ghost `consistent` bit per entry, computed at `Flush`) | base | lifecycle, Trace |
| ReloadEquivalence | **Split** into `CommittedReplayOk` + `NoReplayErrorOnLoad` + ownership/epoch invariants evaluated on `CommittedGS` + `AckedImpliesCommitted` | base | lifecycle (all four), MC (CommittedReplayOk) |
| AckedImpliesCommitted | `AckedImpliesCommitted` | base | MC, lifecycle, timers, Trace |
| TimerIncarnation / NoSpuriousFence | `NoSpuriousFence` (ghost incarnation on members and timers; rebalance fence must hit an UNREVOKED member) | base | timers, ownership |
| TimersCoverMembers | `TimersCoverMembers` | base | timers |
| CoordinatorEventuallyActive (liveness) | `MC.tla` `CoordinatorEventuallyActive` | MC | not enabled (see below) |
| GroupConverges (liveness) | safety form `StableImpliesFullyAssigned` | base | regex |

Notes on the table:
- **ReloadEquivalence** was split because "a state the coordinator could have reached" is not
  directly expressible. The split checks the observable consequences: the committed log replays
  cleanly, the committed state satisfies ownership and epoch bounds, and every acknowledged write
  is committed.
- **CoordinatorEventuallyActive** is defined but not enabled. Its stuck-FAILED variant (MC-1b) shows
  up in the lifecycle hunt as `NoStaleLoadCompletion` (the flag is raised on both the ACTIVE and the
  FAILED branch). A real liveness check needs unbounded heartbeats, which the counter-bounded MC does not have.
- **GroupConverges** is encoded as a safety property: a group that reports STABLE with all regexes
  resolved and no resolution in flight must have every subscribed partition assigned. The
  implementation has no further trigger in that state, so a violation means non-convergence.

## Findings (brief §6.1)

| ID | Trigger mechanism in the spec | Expected violation | Hunt cfg | Reachable? |
|---|---|---|---|---|
| MC-1 | `LeaderChange`×2 on the same broker while its loader job is queued or running. `ProcessCompleteLoad` checks only `st = LOADING` (CR:583-612). | NoStaleLoadCompletion, NoActiveWhileLoading, AppendFromLogState; MC-1b: context left FAILED | lifecycle | Yes (random simulation hits NoStaleLoadCompletion within seconds) |
| MC-2 | events processed by a stale ACTIVE context after its broker leads again. `Flush` appends iff `b = leader`, with no epoch check, then the SC:140 branch. | AppendFromLogState, CommittedReplayOk | lifecycle | Enabled; not yet separately confirmed (its trigger needs a 3-change flap; the BFS run is part of Phase 3) |
| MC-3 | `OpTimeout` of a session-timeout fence, client rejoin with the same id, then `RetryTimers` replaces the new session task (CTI:121-124) and fences the new incarnation | NoSpuriousFence (`SessionFenceOtherIncarnation`) | timers | Enabled; not yet hit in simulation |
| MC-4 | `FlushFail` reverts timeline state but not `timers`. (a) leftover rebalance timer fences a STABLE member; (b) a reverted leave leaves the member without a session timer | NoSpuriousFence (`RebalanceFenceNotUnrevoked`), TimersCoverMembers | timers | (b) yes: simulation found a trace matching GMM:5104-5106 + CR:783-790. (a) not yet hit. |
| MC-5 | a late or duplicate epoch-0 request with `owned = {}` (KAFKA-19233 path) or an unchecked -1 leave delivered after newer state | OwnedImpliesAssigned, ClientOwnershipExclusive, NoSpuriousFence | ownership | Yes (OwnedImpliesAssigned hit in simulation) |
| MC-6 | regex A→B where B resolves to no topics, while another member still subscribes through A | StableImpliesFullyAssigned | regex | Yes (BFS, 28,872 distinct states) |

MC-6's second half (a classic-only group stalled by the assignment interval) is **not covered**:
classic-protocol members are outside `base.tla`. That half stays a test-verifiable item.

## Guidance priority questions

| Guidance question | Where it is represented |
|---|---|
| Q1: two members own the same partition | `PartitionOwnershipExclusive` (server side), `OwnedImpliesAssigned` / `ClientOwnershipExclusive` (client side). The CAB transition table is encoded literally (`CNA`, `UCA`, `CABBuild`), as are the ownership-map overwrite and skip semantics (`AddPE`/`RemovePE`). Hunt: ownership. |
| Q2: coordinator move mid-rebalance | log layer with truncation, a loader process with a FIFO of jobs, the lifecycle queue, replay of the uncommitted tail; `AppendFromLogState` and the committed-state invariants. Hunt: lifecycle. |
| Q3: responses from uncommitted state | deferred queue released only by `HWMDeliver`, `OpTimeout` (response fails, state stays), `AckedImpliesCommitted`. Hunts: lifecycle, timers. |
| Q4: timers vs heartbeats, same-id rejoin | timers keyed per (kind, member), retried by key, not reverted; ghost incarnation. Hunt: timers. |
| Q5: regex re-resolution | async `regexTask`, `RegexResolve` result handling, `refreshDue`. Hunt: regex. |
| Not covered | Static membership and classic↔consumer migration (Additional Exploration in the guidance). They stay test items (brief T-4, T-5). |

## Smoke-run observations (not triaged)

Short random simulations of the hunt cfgs, run only to check that each target is reachable and the
spec evaluates without errors. These are hunting results, so their meaning is decided in Phase 3,
after the spec has been validated against implementation traces:

| Hunt cfg | Result |
|---|---|
| lifecycle | NoStaleLoadCompletion violated |
| timers | TimersCoverMembers violated (MC-4b shape) |
| ownership | OwnedImpliesAssigned violated: a late duplicate epoch-0 request releases partitions the client still consumes (MC-5 shape) |
| regex | StableImpliesFullyAssigned violated (MC-6 shape) |
| MC.cfg | 20,000 random traces with no violation of the core invariants |

Two early hits were modeling artifacts and were fixed in the spec before these runs:
- **Session timer firing too early.** A heartbeat-scheduled timer could fire while that member's own
  write was still pending. Fixed with a timing guard in `TimerFire`.
- **Stale-timer ghost misfiring.** It flagged a correct re-fence after a reverted batch. It was
  replaced by the incarnation ghost.
