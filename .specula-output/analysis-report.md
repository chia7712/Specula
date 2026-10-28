# Analysis Report: Apache Kafka KIP-848 consumer group rebalance (Phase 1 audit trail)

Target: `./kafka` trunk @ `1224f3d4b` (2026-10-27). Modules: `group-coordinator`, `coordinator-common`
(~56k LOC main Java; core files: GroupMetadataManager 10.2k, CoordinatorRuntime 2.5k, ConsumerGroup 1.4k,
CurrentAssignmentBuilder 0.5k, ConsumerGroupMember 0.6k, CoordinatorTimerImpl 0.2k).
Guidance: `guidance/kafka-consumer-group-rebalance.md`.

## 1. Evidence sources and coverage

| Source | Coverage |
|---|---|
| Git history | Clone deepened to 2022-10 (8116 commits). 519 commits touch `group-coordinator/src/main` or `coordinator-common/src/main` (share/streams-titled excluded by title; 7 wrongly excluded ones recovered by hand). All 519 titles skimmed; ~146 messages read; ~25 diffs read in detail; 20 pickaxe (`git log -S`) searches. |
| In-scope bug fixes found | 74 (33 in 2022-11..2024-12, 41 in 2025-01..HEAD). Tables: `analysis-work/archaeology-pre2025.md` §2, `analysis-work/archaeology-2025plus.md` §2. |
| Excluded commits | ~67 feature scaffolding, ~45+60 refactor/MINOR, ~20+20 metrics/config/docs, ~70 streams, ~20 offsets/txn, 3 classic-only (details in the two archaeology files). |
| JIRA / GitHub issues & PRs | **Not available**: `issues.apache.org` and `api.github.com` are blocked by this environment's network policy (proxy 403). Issue discussions were NOT read; the 30+ deeply-read-issues requirement could not be met. Commit messages (which carry the KAFKA ticket title and PR description) were used instead. Open PRs were not reviewed. |
| Deep code reading | 4 parallel analyses, each reading its files in full: heartbeat path + timers (`analysis-work/deep-heartbeat-timers.md`), reconciliation/ownership (`deep-reconciliation.md`), CoordinatorRuntime write/HWM/load (`deep-runtime.md`), replay/regex/migration (`deep-replay-regex-migration.md`). |
| Developer signals | One TODO in scope area (GMM:4964, KAFKA-20680) — streams only, excluded. |
| Persistent findings registry | Initialized (`persistent-findings-context.json`, run `2026-10-28-kafka-cg`); no prior history to match against. |

## 2. Mechanism clusters from archaeology

| Mechanism | Representative fixes | Signal |
|---|---|---|
| Commit gating / HWM / load lifecycle | KAFKA-16973 caught-up condition, KAFKA-16374 HWM priority, KAFKA-15910/19716 snapshots while loading, KAFKA-16030 offline during load, KAFKA-16118 unload of deleted replica, KAFKA-20634 HWM after leader move (Crit), KAFKA-19862 load vs compaction, KAFKA-18484/19064 deferred-event exceptions | Most bug-dense area; 16+ fixes. |
| Runtime revert vs non-timeline side effects | KAFKA-20845 empty-batch revert while records written (Crit), 9bc48af1c timer type checks, KAFKA-16663 write-timeout timer cancel, cad46800c timer thread safety, KAFKA-19857 executor cancelAll | Each fix covered one more site; timers still not reverted. |
| Epoch bookkeeping in reconciliation | KAFKA-19974 two owners at same epoch (High), KAFKA-19449 own pending partitions, KAFKA-19233 rejoin with epoch 0, b8692f315 previous epoch on unsubscribe, KAFKA-16147 / 16249 (pre-2025), KIP-1251 [1-3/N] + KAFKA-20268/20442 | Core Q1 area; semantics changed repeatedly in 2025-2026. |
| Static member identity | KAFKA-14516, 17228, 17317, 20601, 20747, 20434, 3e5df2f08, KAFKA-20292 series | 8+ fixes, ongoing. |
| Regex / subscription freshness | KAFKA-17592/17593, 18961 time-based refresh, 19891 regex non-empty→empty bump, 19720 | Each fix covered one transition; siblings remain (Scenario 4). |
| Migration classic↔consumer | KAFKA-16436/16554/16313, 17733, 18134, 18150, 19546, 20273, 20640, 20662, 21303 | Continuous trickle of fixes. |

## 3. Verified findings (summary; details in work files)

Verified by the lead analyst re-reading code (beyond the sub-analyses):
- **R-F1 stale CompleteLoad** — `CoordinatorRuntime.java:583-612` callback re-looks-up `coordinators.get(tp)` and checks only `state == LOADING`; unload removes the context (`:2436`), reload re-creates it (`:2364`), loader is single-threaded FIFO (`CoordinatorLoaderImpl.java:77`). Independently found by two sub-analyses.
- **T-F1 timer retry overrides newer incarnation** — `CoordinatorTimerImpl.java:121-124` reschedules under the same key on any non-NotCoordinator failure (e.g. `OperationTimeout` `CoordinatorRuntime.java:1138-1139`), replacing whatever task currently holds the key; session-timeout op `GMM:5406-5427` fences by memberId with no epoch/incarnation check; session timer is `retry=true` (`GMM:5507-5513`).
- **G-F3 regex A→B (B resolves empty) never rebalances** — `GMM:2677-2687` no bump for unresolved regex; resolution result compares `{}` to `ResolvedRegularExpression.EMPTY` (`GMM:3903-3907`) → no bump; periodic refresh `updateSubscriptionMetadata` bumps only on hash change (topics still subscribed via A by others). The replay/regex sub-analysis judged Q5 "safe"; its argument relied on the periodic refresh, which does not bump here, so the disagreement is resolved in favour of "suspicious (liveness)", pending model checking.
- **Rebalance-timer epoch-only guard** — `GMM:5569` checks `memberEpoch` only, not member state; combined with KAFKA-19974 (epoch unchanged on STABLE→UNREVOKED) and non-reverted timers.
- **Dynamic leave not epoch-checked** — `GMM:4838-4841`.

Disagreements / unsure: tombstone strict checks under compaction during load (`GMM:6058-6065, 6176-6188`) — two analyses argue compaction cannot trigger them, one is unsure. M1 (upgrade during classic COMPLETING_REBALANCE) not re-verified by lead.

## 4. False positives / excluded after verification
- Timer task racing a heartbeat for the same member: safe — `tasks.remove(key, this)` identity check (`CoordinatorTimerImpl.java:93`) plus reschedule/cancel on heartbeat.
- Assignor offload remapping (`TargetAssignmentRecordsBuilder.java:216-266`): not wired for consumer groups (`consumerGroupAssignorOffloadEnable` has no callers) — no race today; excluded.
- Stale HWM event reaching new context; timers/executor of old incarnation after unload (`cancelAll`): benign.
- Prefix-commit of an operation's records: every consumer-group op is one atomic batch (`CoordinatorRuntime.java:955-978`); only empty-group cleanup is non-atomic (harmless).
- Regex late result for a regex no longer subscribed: skipped (`GMM:3893`).
- Static member replacement: atomic, partition epochs swapped in replay.
