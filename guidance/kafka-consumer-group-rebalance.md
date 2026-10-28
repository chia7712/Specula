# Apache Kafka: Consumer Group Rebalance Protocol (KIP-848) Modeling Guidance

Target source: `./kafka` (Apache Kafka trunk, module `group-coordinator` with its
runtime in `coordinator-common`). Treat the checked-out implementation as ground truth.

## Required

### Goal

- Find safety bugs in the new consumer group protocol (KIP-848): partition
  ownership overlap, lost or rolled-back group state, and members that are
  fenced or stuck in ways the protocol contract forbids.

### Scope

- In scope: the `ConsumerGroup` state machine and its members
  (`modern/consumer/`), target and current assignment reconciliation
  (`TargetAssignmentBuilder`, `CurrentAssignmentBuilder`), epoch handling in
  `GroupMetadataManager` for the consumer-group heartbeat path, the record
  write/commit/replay cycle in `CoordinatorRuntime` (deferred events released
  on high watermark advance, loading on coordinator move), and the
  session/rebalance timers scheduled through `CoordinatorTimer`.
- Boundaries/exclusions: the classic (JoinGroup/SyncGroup) protocol except where
  it shares code with the new protocol via migration
  (`ConsumerGroupMigrationPolicy`); share groups and streams groups; offset
  commit semantics beyond what the rebalance path reads; the network layer,
  KRaft replication, and the client-side `ConsumerMembershipManager`. Read
  excluded code only to establish caller contracts.

### Priority Questions

1. When a member is being revoked a partition while a new target assignment
   is computed, can two members hold the same partition in their current
   assignment at the same time?
   Expected behavior: a partition is owned by at most one member's current
   assignment at any point; a partition moves to a new owner only after the old
   owner has acknowledged revocation (member epoch advanced).
2. When the coordinator moves to another broker in the middle of a rebalance,
   can replayed records produce a group whose group epoch, target assignment
   epoch, and member epochs are inconsistent, or whose members are stuck
   waiting for a revocation that will never be acknowledged?
   Expected behavior: the state rebuilt from committed records is one the
   coordinator could have reached in the original run, and every member can
   make progress from it with ordinary heartbeats.
3. When a heartbeat response is computed from state that is not yet committed
   (high watermark not advanced) and the write later fails or the coordinator
   is unloaded, can the member act on an epoch or assignment the coordinator
   never durably reached?
   Expected behavior: responses that depend on uncommitted state are deferred
   until commit, and a failed write never leaks an assignment to a member.
4. When a session timeout or rebalance timeout fires concurrently with a
   heartbeat from the same member, can the member be fenced and simultaneously
   granted a new epoch or assignment, or can a timer for a departed member
   fire against a member that rejoined with the same member id?
   Expected behavior: timer expiry and heartbeats for the same member are
   serialized through the coordinator's state machine; a timer scheduled for
   an earlier incarnation never affects a later one.
5. When a member subscribed by regular expression heartbeats before the regex
   is re-resolved after a topic change, can the target assignment include
   partitions of a topic the member is no longer subscribed to, or omit a
   matching topic indefinitely?
   Expected behavior: the group converges to an assignment that matches the
   resolved subscription once metadata is stable.

### Must-cover Interactions

- `GroupMetadataManager.consumerGroupHeartbeat` ↔ `ConsumerGroup` /
  `ConsumerGroupMember`: epoch bump, member join/leave/fencing, and the
  `UNREVOKED_PARTITIONS` → `UNRELEASED_PARTITIONS` → `STABLE` member state
  transitions computed by `CurrentAssignmentBuilder`.
- `TargetAssignmentBuilder` ↔ `CurrentAssignmentBuilder`: a new target
  assignment is installed while members may still be reconciling the previous
  one.
- `CoordinatorRuntime` write path ↔ `GroupMetadataManager.replay`: records
  appended, high watermark advance, deferred response release, and load from
  the `__consumer_offsets` partition after a leader change, including a
  partial batch at the tail.
- `CoordinatorTimer` session and rebalance timeouts ↔ heartbeat processing
  for the same member id; member rejoin with the same id after fencing.
- `TopicRegexResolver` and metadata image updates ↔ subscription metadata and
  group epoch bumps.
- Consider: coordinator unload mid-write, duplicated or reordered heartbeats
  from the same member (network retries), a member whose epoch is stale by
  more than one, and topics deleted while partitions are mid-revocation.

### Assumptions

- The coordinator processes events for one `__consumer_offsets` partition on a
  single thread at a time (per-shard serialization by `CoordinatorRuntime`);
  verify this against the code rather than assuming it.
- Clients are well-behaved: they acknowledge revocations by heartbeating with
  the reduced owned set, but they may be slow, may retry, and may crash at any
  point.

## Optional

### Known Incidents and References

- KIP-848 (The Next Generation of the Consumer Rebalance Protocol): the
  intended contract for epochs, revocation and assignment.
- Kafka JIRA components `group-coordinator` and `consumer`: search for fixed
  bugs mentioning "stuck", "epoch", "revocation", "duplicate assignment",
  "fenced" to seed bug archaeology.

### Suggested Starting Points

- `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/GroupMetadataManager.java`
  (`consumerGroupHeartbeat`, `replay`, timer scheduling helpers)
- `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/consumer/`
  (`ConsumerGroup`, `ConsumerGroupMember`, `CurrentAssignmentBuilder`)
- `group-coordinator/src/main/java/org/apache/kafka/coordinator/group/modern/TargetAssignmentBuilder.java`
- `coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorRuntime.java`
  (`scheduleWriteOperation`, `onHighWatermarkUpdated`, `scheduleLoadOperation`, `DeferredEventCollection`)
- `coordinator-common/src/main/java/org/apache/kafka/coordinator/common/runtime/CoordinatorTimerImpl.java`
- Existing tests for scenarios and harness ideas:
  `group-coordinator/src/test/java/org/apache/kafka/coordinator/group/GroupMetadataManagerTest.java`
  and `GroupMetadataManagerTestContext.java`.

### Additional Exploration

- Classic-to-consumer group migration (`ConsumerGroupMigrationPolicy`) when a
  classic member and a new-protocol member are in the same group.
- Static membership (`group.instance.id`) with a member rejoining under a new
  member id before the old one is fenced.
