# Instrumentation Spec: Kafka KIP-848 consumer groups on the coordinator runtime

This maps every `Trace.tla` wrapper to the Kafka code (trunk `1224f3d4b`) that has to emit it.
Category A: the trace is a single linear NDJSON file, written by one writer behind a mutex. All
coordinator events of one `__consumer_offsets` partition are serialized by the runtime, and the
JUnit scenario thread drives the client and log events.

Abbreviations follow `base.tla`: GMM, CG, CAB, CR, CTI, CLI and SC.

## 1. Trace Event Schema

### 1.1 Envelope

```json
{"tag": "trace", "event": {"name": "<SpecAction>", "b": "b1", "...": "event fields", "post": {"...": "post-state"}}}
```

Encoding rules:
- **No `"null"` strings.** An optional value is encoded by omitting its field (`owned`, `sub`,
  `target`). TLC cannot compare strings with records or sequences.
- **Sets are JSON arrays**, in any order.
- **Names are normalized.** Brokers become `b1`/`b2`, members `m1`/`m2`, topics `t1`/`t2`, partitions
  `p1`/`p2`, and regexes `A`/`B`. The harness keeps the maps (Uuid → name, `TopicIdPartition` → name)
  and writes a `Config` event first:

```json
{"tag":"trace","event":{"name":"Config","topicOf":{"p1":"t1","p2":"t1"},
  "regexMatch":{"A":["t1"],"B":[]},
  "subscriptions":[{"names":["t1"],"regex":""}],
  "initialSub":{"m1":{"names":["t1"],"regex":""},"m2":{"names":["t1"],"regex":""}}}}
```

- **Regex fields.** `regex` is `""` when the member has no regex subscription. `regexMatch` must match
  what `TopicRegexResolver` returns against the test `MetadataImage`.
- **Member states** use the `MemberState` enum names: `STABLE`, `UNREVOKED_PARTITIONS`, `UNRELEASED_PARTITIONS`.

### 1.2 Post-state fields

| Trace field | TLA+ | Implementation getter |
|---|---|---|
| `post.st` | `ctx[b].st` | `CoordinatorContext.state`. `CLOSED`/removed maps to `NONE`, and `INITIAL` stays `INITIAL`. |
| `post.epoch` | `ctx[b].epoch` | `CoordinatorContext.epoch` |
| `post.written` | `ctx[b].written` | `SnapshottableCoordinator.lastWrittenOffset()`, expressed in **batches** (see §3.2) |
| `post.committed` | `ctx[b].committed` | `SnapshottableCoordinator.lastCommittedOffset()`, in batches |
| `post.groupEpoch` | `gs.groupEpoch` | `ConsumerGroup.groupEpoch()` |
| `post.assignEpoch` | `gs.assignEpoch` | `ConsumerGroup.assignmentEpoch()` |
| `post.members` | `MembersOf(gs)` | `ConsumerGroup.members().keySet()` |
| `post.member.{epoch,prev,state}` | `gs.members[m].*` | `ConsumerGroupMember.memberEpoch()/previousMemberEpoch()/state()` |
| `post.member.assigned` / `pending` | `gs.members[m].assigned/pending` | `assignedPartitions()` / `partitionsPendingRevocation()` (key sets only; per-partition assignment epochs are not modeled) |
| `post.member.target` | `gs.target[m]` | `ConsumerGroup.targetAssignment(m).partitions()`. Omit when the member has no target; emit `targetAbsent: true` instead. |
| `post.memberAbsent` | `gs.members[m] = Null` | member not in `members()` |
| `post.resolved.{r}` | `gs.resolved[r]` | `ConsumerGroup.resolvedRegularExpression(r).topics()` |
| `post.leader`, `post.leaderEpoch`, `post.hwm`, `post.logLen.{b}` | log layer | harness bookkeeping in the replicated mock writer (§3.1) |
| client `post.epoch/owned/assignment` | `client[m].*` | the scenario's client model (§3.3) |

## 2. Action-to-Code Mapping

**Coordinator runtime** (`coordinator-common/.../runtime/CoordinatorRuntime.java`):

| Spec action / event | Code location | Trigger point | Fields |
|---|---|---|---|
| `ProcessLoad` / `Load` | CR:2364-2392, the internal op of `scheduleLoadOperation` | after the lambda body (context lock released) | `b`, `post.{st,epoch}` |
| `ProcessUnload` / `Unload` | CR:2416-2445, the internal op of `scheduleUnloadOperation` | after the lambda body, both when it unloads and when it ignores the event | `b`, `post.st` |
| `ProcessCompleteLoad` / `CompleteLoad` | CR:584-611, the `CompleteLoad` internal op in `load()` | after the lambda. Emit in **all** branches, including "ignored" and "context does not exist". | `b`, `ok` (exception == null), `post.{st,groupEpoch}` |
| `LoaderStep` / `LoaderStep` | CLI:150-155 loop body, after `processMemoryRecords` | after each fetched batch is replayed | `b`, `offset` (batches replayed so far) |
| `LoaderFinish` / `LoaderFinish` | CLI:163-170 | right before `future.complete` / `completeExceptionally` | `b`, `ok` |
| `Flush` / `Flush` | CR:690-742 `flushCurrentBatch` | after `freeCurrentBatch()`, or in the `catch` after `failCurrentBatch(t)` (out-of-sync, SC:140, NotLeader) | `b`, `post.{st,written,committed,logLen}` |
| `FlushFail` / `FlushFail` | CR:751-760 catch branch for a non-leadership error | after `failCurrentBatch(t)`. Use a fault hook in the mock writer that throws `RecordTooLargeException` (§3.1). | `b`, `post.{st,groupEpoch,...}` |
| `HWMDeliver` / `HWMDeliver` | CR:1819-1835 `HighWatermarkUpdate` internal event | after `deferredEventQueue.completeUpTo` (or after the "ignored" log) | `b`, `post.{st,committed}` |
| `OpTimeout` / `OpTimeout` | CR:1121-1140 `OperationTimeout`, the internal op | after `event.complete(TimeoutException)` | `b`, `post.st` |

**Group coordinator** (`group-coordinator/.../GroupMetadataManager.java`):

| Spec action / event | Code location | Trigger point | Fields |
|---|---|---|---|
| `ConsumerGroupHeartbeat` / `Heartbeat` | GMM:5725-5752 `consumerGroupHeartbeat(context, request)`, dispatching to `consumerGroupLeave` GMM:4827 or `consumerGroupHeartbeat` GMM:2589 | **after `CoordinatorWriteEvent.run` has appended and replayed the records** (CR:1317-1347). An `ApiException` thrown by the handler goes to `complete(t)`. | `b`, `m`, `reqId`, `err` (error name or `NONE`), `targets` (all members' target after the event, if a new target was computed), `post.{groupEpoch,assignEpoch,members,member}` |
| `TimerFire` / `TimerFire` | CTI:84-99 write op of the timer task, after `operation.generateRecords()`, plus its append | after the write event's append (same point as `Heartbeat`). Emit only when `tasks.remove(key, this)` succeeds. | `b`, `kind` (`session` from key prefix `consumer-group-session-timeout`, `rebalance` from `consumer-group-rebalance-timeout`), `m`, `post.{groupEpoch,members,member}` |
| `RegexResolve` / `RegexResolve` | GMM:3870-3930 `handleRegularExpressionsResult`, run as the executor's completion write op | after its append | `b`, `post.{groupEpoch,resolved}` |
| `MetadataRefreshDue` / `MetadataRefreshDue` | `ModernGroup.hasMetadataExpired` becomes true (MockTime advanced past `METADATA_REFRESH_INTERVAL_MS`) | emitted by the scenario when it advances time past the deadline. Otherwise left to the silent action. | `b` |

**Log layer and clock** (harness, §3.1):

| Spec action / event | Code location | Trigger point | Fields |
|---|---|---|---|
| `Tick` / `Tick` | `MockTime.sleep` in the scenario | after the sleep | none |
| `Replicate` / `Replicate` | harness `ReplicatedMockWriter.replicate(f)` | after the copy | `b` (follower), `post.logLen` |
| `AdvanceHWM` / `AdvanceHWM` | harness `ReplicatedMockWriter.commitUpTo(k)`, which calls the listeners | after setting the HWM, before calling the listener | `hwm` (in batches) |
| `LeaderChange` / `LeaderChange` | harness `electLeader(n)`, then `runtime.scheduleUnloadOperation(tp, epoch)` on the old leader and `scheduleLoadOperation(tp, epoch)` on the new one | after both are scheduled | `b` (new leader), `post.{leader,leaderEpoch}` |

**Client** (scenario thread, §3.3):

| Spec action / event | Code location | Trigger point | Fields |
|---|---|---|---|
| `ClientJoin` / `ClientJoin` | scenario: builds a `ConsumerGroupHeartbeatRequestData` with epoch 0 | before submitting | `m`, `reqId`, `epoch`, `owned`, `sub`, `full` |
| `ClientHeartbeat` / `ClientHeartbeat` | scenario: regular heartbeat | before submitting | same as above. Omit `owned`/`sub` when not set. |
| `ClientLeave` / `ClientLeave` | scenario: epoch -1 | before submitting | same as above |
| `ClientChangeSubscription` | scenario | when the client model changes its subscription | `m`, `sub` |
| `ClientReceive` | scenario: the response future completes | after the client model processes it | `m`, `reqId`, `post.{epoch,assignment}` |
| `ClientRevoke` / `ClientAssign` | scenario: simulated `onPartitionsRevoked` / `onPartitionsAssigned` | after updating the owned set | `m`, `post.owned` |
| `ClientRequestTimeout` | scenario: gives up on an outstanding request (the request may still be delivered later) | when it gives up | `m` |

## 3. Special Considerations

### 3.1 Harness topology
- **Runtime instance.** Use a real `CoordinatorRuntime` with the real `GroupCoordinatorShard` per
  simulated broker, two instances for Scenario 1. Each gets a `MockCoordinatorTimer`-free
  configuration, meaning the real `CoordinatorTimerImpl` and `CoordinatorExecutorImpl` driven by
  `MockTime` and a `MockTimer`, so `CTI:121-124` retries are exercised.
- **Replicated writer.** Implement it on top of `coordinator-common/src/test/.../MockPartitionWriter.java`
  (`InMemoryPartitionWriter`), one shared object with:
  - one record log per broker;
  - `append` that succeeds only on the current leader;
  - `replicate(f)`;
  - `commitUpTo(k)`, which notifies the registered listener of the leader;
  - the KAFKA-20634 deactivation of a listener when its broker becomes a follower;
  - a fault hook that makes the next `append` throw `RecordTooLargeException` (`FlushFail`).
- **Loader.** Use a controllable loader based on `MockCoordinatorLoader` that replays the current log
  of its broker batch by batch on a single loader thread. The scenario must be able to pause it
  between batches (`LoaderStep`) and before completing (`LoaderFinish`), so Scenario 1 interleavings
  can be produced deterministically.
- **Event processor.** Use a single-threaded `MultiThreadedEventProcessor` (1 thread), so every
  runtime event is serialized and the emission order equals the processing order.

### 3.2 Offsets are batches
The spec counts log entries (one batch each). The harness converts record offsets to batch counts.
`written` is the number of batches in the local log appended through this context, and `hwm` is the
number of committed batches. `CR:720` (out of sync) compares record offsets, which is equivalent
here because every batch has a known record count.

### 3.3 Client model
The scenario plays a well-behaved client that follows `base.tla`'s client actions:
- one request in flight at a time;
- a full request after join or any error;
- revoke before assign;
- leave only after revoking everything;
- rejoin with epoch 0 and the **same member id** after `FENCED_MEMBER_EPOCH` / `UNKNOWN_MEMBER_ID` /
  `GROUP_ID_NOT_FOUND`.

`reqId` is a per-member counter starting at 1. The request is submitted through
`runtime.scheduleWriteOperation("consumer-group-heartbeat", tp, Duration, coordinator -> coordinator.consumerGroupHeartbeat(ctx, request))`,
the same path `GroupCoordinatorService.consumerGroupHeartbeat` uses. The ghost incarnation `inc` is
not emitted: it is derived by the spec from `ClientJoin` events.

### 3.4 Assignor
Use the default `UniformAssignor` (or `MockPartitionAssignor` with a fixed plan). The spec's
assignor is nondeterministic, and the `targets` field of the `Heartbeat` event pins the choice. With
`AssignInterval = 0` (`group.consumer.assignment.interval.ms = 0`), every epoch bump computes a new
target immediately.

### 3.5 Timing
- **Event timeouts.** Set `group.consumer.session.timeout.ms` and the rebalance timeout well above
  the write timeout, matching the spec's timing guard in `TimerFire`. Fire timers by advancing
  `MockTime` and running the expired `MockTimer` tasks; the resulting write events emit `TimerFire`.
- **Regex refresh.** `REGEX_BATCH_REFRESH_MIN_INTERVAL_MS` (10 s) is not modeled. Advance time by
  more than 10 s between regex changes so the implementation never skips a refresh the spec performs.

### 3.6 What is not traced
- Static and classic members, share and streams groups.
- Offset commits, and per-partition assignment epochs (KIP-1251).
- The 10 s regex gate and the time-based regex refresh.

Scenarios must not use these features; otherwise the trace will not match.
