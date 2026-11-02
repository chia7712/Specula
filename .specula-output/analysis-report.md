# Analysis Report: Kafka Share Groups (KIP-932)

Kafka revision: `8aa57006ff5d` (trunk, 2026-11-02). The clone is shallow, starting at 2025-09-01; the guidance asked for the last 12 months.
Guidance: `guidance/kafka-share-groups.md`. Handoff document: `modeling-brief.md`.

## Category

**Category A (Distributed / Message-Passing).** The delivery contract depends on:
- the async, retrying persister RPCs from the partition leader to the share coordinator;
- the coordinator's replicated log, which has snapshot/update/tombstone records, pruning and failover-by-replay;
- partition leader change, enforced by leader-epoch and state-epoch fencing.

There is also a local concurrency layer inside `SharePartition`: one RW lock, the timer thread for acquisition locks, and persister callback threads. It is modeled as interleaved actions.

## Coverage

| Item | Count |
|---|---|
| Commits since 2025-09-01 touching in-scope paths (`analysis/commit-list.txt`) | 141 |
| ...touching the SharePartition/fetch slice | 48 (15 bug fixes and 14 correctness-relevant features read in full) |
| ...touching the coordinator/persister/manager/session/DLQ slice | 95 (44 correctness-relevant read in full; 27 of them fixes) |
| Group-coordinator share commits scanned for the init/delete contract | ~70 (5 relevant) |
| Core files read in full by the deep-analysis passes | SharePartition, InFlightBatch, InFlightState, AcquisitionLockTimerTask, SharePartitionManager, DelayedShareFetch, ShareFetchUtils, ShareFetch, ShareSessionCache, ShareCoordinatorShard, ShareCoordinatorService, PersisterStateBatchCombiner, ShareCoordinatorOffsetsManager, PersisterStateManager (relevant parts), DefaultStatePersister |
| TODO/FIXME/HACK markers in scope | 0 |
| JIRA issues deeply read | **0. issues.apache.org was blocked by the session network policy**, and the GitHub API scope excludes apache/kafka. Kafka squash-merge commit messages, which carry the PR description, were used instead. |

Method: six parallel passes, each with notes under `analysis/`: two archaeology passes and four deep-analysis passes (state machine, persist/init, coordinator, manager/fetch). I re-verified the highest-impact claims against the code in the main context:
- Scenario 1: `close()`/`markFenced()` do not cancel timers; removal is by key; `onGroupEmpty` is broker-local.
- Scenario 2: the epoch maps are rebuilt from retained records; an equal-epoch read writes nothing.
- Scenario 3: last-result-wins on duplicate partitions (SCSvc:549, PSM:215-229).
- Scenario 4: the split drops the rollback state (IFB:225-249) and the `archiveRecords` callers.
- Scenario 5: the start-offset regression in the all-acked branch (SP:2645-2652, `canMoveStartOffset` floorEntry).

None of these is confirmed by a test or run. This phase is code reading only.

## Answers to the guidance priority questions (code reading)

1. **Timeout vs acknowledge (PQ1).** In a single live instance exactly one wins:
   - `hasExpired` is set before taking the lock, and the handler checks `isCancelled`;
   - the memberId and `state == ACQUIRED` checks reject the loser;
   - dc is incremented only in acquire.
   
   Exceptions: leaked timers after a batch split (Scenario 4), and timers on fenced instances (Scenario 1). The modeling baseline is MC-8.
2. **Leader change (PQ2).** For a *real* leader change (epoch bump), an ack the coordinator accepted is never lost: `readState` is serialized after pending writes and the old leader's later writes are fenced. In-flight records come back AVAILABLE or as gaps, and their dc is lost, because acquire is not persisted. Open risks:
   - replacement **without** an epoch bump (Scenario 1);
   - a lost fence after prune and failover (Scenario 2).
3. **Write failure (PQ3).** Ack/release only advance start/cache after success and roll back fully. Timeout, DLQ phase 2 and the LSO move are memory-first by design. An ambiguous outcome (applied but reported failed) and coalesced results for duplicate keys diverge the two states (Scenario 3).
4. **Archiving and max attempts (PQ4).**
   - No archiving happens while a record is held: the LSO and stale-batch archives touch only AVAILABLE; the aborted-transaction archive touches only records filtered out of the same response.
   - The per-offset/batch twins are consistent except for the archive split (Scenario 4) and rollback after expiry (ARCHIVED, bypassing the DLQ).
   - Several acquire-then-drop response paths burn delivery attempts without delivering (Scenario 6, T-7..T-9).
5. **Epoch fencing and compaction (PQ5).** Snapshot/update compaction is sound: replay equals write-time state, because both use the same combiner, and pruning keeps each key's latest snapshot. Fencing has gaps:
   - equal epochs are accepted;
   - the fence is rebuilt from retained records only (MC-3);
   - a tombstone resets the epochs (MC-4);
   - `deleteState` carries no epoch (CR-1).

Assumption check: per-key serialization holds. All ops for a key map to one `__share_group_state` partition (SCSvc:282-288), and reads run as write ops, so they wait for pending writes (CoordinatorRuntime:880-919).

## Historical mechanism groups (detail in the archaeology notes)

- Start offset / LSO / gap window: 55764f837, 18dbd5227, 25aae9903, 8fe1157ac.
- Lock timeout vs ack/release/renew: 25aae9903, dfa28c463, e658d545d, 7b017fadd.
- Batch vs per-offset split: dfa28c463, 43062a0ee, c205e6910, 18dbd5227.
- Re-initialization and stale instances: 08e287870, 8fe1157ac, f771a1e9b, 3eb0b8c1b.
- Reordered and duplicated persister requests: b77567478, 2225a16c7, 1df8219b3, 5dd015386, 46b4636d6.
- Epoch sentinels: 6665a1950, cde0d5687, be72089de.
- Session lifecycle: 70be719f6, c85a8c17a.
- DelayedShareFetch locks and wakeups: the KAFKA-20783 series, 8fc539cb3, 29254d681, ea6afaa46.
- DLQ write path: 93d0db451, bd5fcec64, a589c272c.

## Excluded / found safe

- A late ack after release and re-acquisition by another member: rejected by the memberId check under the partition lock.
- Coordinator update-record compaction dropping an update: not possible, since each key's latest snapshot is retained.
- Retried or reordered writes within one live instance: harmless, because the merge is commutative (max dc/state, max start).
- Purgatory starvation: bounded by maxWait; latency only.
- Durable startOffset moving backward: prevented by the max at SCS:670.
