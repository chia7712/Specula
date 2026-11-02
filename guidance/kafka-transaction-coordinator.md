# Apache Kafka: Transaction Coordinator Modeling Guidance

Target source: `./kafka`. The coordinator logic is split between the Scala classes in
`core/src/main/scala/kafka/coordinator/transaction/` and the Java data model in
`transaction-coordinator/`. Treat the checked-out implementation as ground truth.

## Required

### Goal

- Find bugs that break the exactly-once guarantee of KIP-98 / KIP-890: a transaction's
  records becoming visible as committed when it was aborted (or the reverse), two producer
  incarnations both making progress on the same transactional id, a transaction stuck
  forever in PrepareCommit / PrepareAbort, or markers written to partitions that were not
  part of the transaction.

### Scope

- In scope: `TransactionCoordinator` request handlers (`handleInitProducerId`,
  `handleAddPartitionsToTransaction`, `handleVerifyPartitionsInTransaction`,
  `handleEndTransaction`), the `TransactionMetadata` / `TxnTransitMetadata` state machine
  (`TransactionState`: Empty, Ongoing, PrepareCommit, PrepareAbort, CompleteCommit,
  CompleteAbort, Dead, PrepareEpochFence), `TransactionStateManager` (append to
  `__transaction_state`, coordinator epoch, load / unload on partition leader change,
  transactional id expiration), `TransactionMarkerChannelManager` and
  `TransactionMarkerRequestCompletionHandler` (writing markers to partition leaders, retries,
  coordinator epoch in markers), and the KIP-890 transaction version 2 paths (implicit
  AddPartitions via verification, epoch bump on every EndTxn).
- Boundaries/exclusions: the producer client state machine, the partition leader's handling
  of `WriteTxnMarkers` beyond the coordinator epoch check, `ProducerStateManager`, and the
  group coordinator's `TxnOffsetCommit` path. Read them only to establish contracts.

### Priority Questions

1. When the coordinator moves to another broker while a transaction is in PrepareCommit and
   some markers were already written, can the new coordinator abort the transaction, or write
   markers with a lower coordinator epoch that partition leaders accept, leaving some
   partitions committed and others aborted?
   Expected behavior: once PrepareCommit is durably written, the outcome is commit on every
   partition; markers from a stale coordinator epoch are rejected.
2. When a producer calls `InitProducerId` while its previous incarnation still has an
   Ongoing transaction (or one in PrepareCommit), can both incarnations append to the
   transaction log or add partitions, or can the old incarnation's EndTxn complete after the
   epoch was bumped?
   Expected behavior: the fence is total; after the bump, every request carrying the old epoch
   is rejected, and the old transaction is either aborted or completed exactly once.
3. With transaction version 2, when a partition is added implicitly through verification
   and the verification response races with an EndTxn or a coordinator failover, can a
   partition end up with records of the transaction but never receive a marker, or receive a
   marker for a transaction it has no records of?
   Expected behavior: the set of partitions in the committed transaction metadata is exactly
   the set that receives markers.
4. When the `__transaction_state` append fails (NotEnoughReplicas, timeout, leader change)
   after the in-memory `pendingState` was set, can the transactional id be stuck so that every
   later request gets `CONCURRENT_TRANSACTIONS` forever, or can the pending transition be
   applied without being durable?
   Expected behavior: a failed append clears the pending state and the metadata reflects only
   durable transitions.
5. When transactional id expiration (`transactional.id.expiration.ms`) runs concurrently with
   a late `InitProducerId` or `EndTxn` for the same id, can the id be deleted while a
   transaction is open, or resurrected with a stale epoch?
   Expected behavior: expiration only removes ids with no open transaction, and a request
   racing with expiration observes either the old metadata or `UNKNOWN_PRODUCER_ID`, never a
   mix.

### Must-cover Interactions

- `TransactionCoordinator.handleEndTransaction` → `TransactionStateManager.appendTransactionToLog`
  → `TransactionMarkerChannelManager.addTxnMarkersToSend` → completion handler →
  second append (CompleteCommit / CompleteAbort). Each hop can fail or be delayed.
- `TransactionStateManager.loadTransactionsForTxnTopicPartition` /
  `removeTransactionsForTxnTopicPartition` ↔ in-flight requests and in-flight markers for the
  same partition; `coordinatorEpoch` checks in `TransactionMarkerRequestCompletionHandler`.
- `TransactionMetadata.prepareTransitionTo` / `completeTransitionTo` and `pendingState` ↔
  concurrent requests for the same transactional id under the per-id lock.
- `handleInitProducerId` epoch bump and `PrepareEpochFence` ↔ an Ongoing transaction and
  its markers.
- `handleVerifyPartitionsInTransaction` (TV2) ↔ `handleAddPartitionsToTransaction` and
  `handleEndTransaction` ordering.
- Consider: duplicate or reordered client requests (same epoch), marker send timeout with
  retry after coordinator move, partition leader change during marker write, and a
  `__transaction_state` leader change between the two appends of an EndTxn.

### Assumptions

- Partition leaders reject `WriteTxnMarkers` with a coordinator epoch lower than the one
  they last saw for that transactional id; verify this in `ReplicaManager` rather than
  assuming it.
- Requests for one transactional id are serialized by the coordinator's per-id lock, but
  marker completions and log append callbacks run on other threads.

## Optional

### Known Incidents and References

- KIP-98, KIP-360 (producer epoch bump on retriable errors), KIP-890 (transaction version 2).
- Archaeology: `git log` for "TransactionCoordinator", "PrepareEpochFence",
  "CONCURRENT_TRANSACTIONS", "TransactionMarkerChannelManager", "pendingState",
  "transactional.id.expiration".

### Suggested Starting Points

- `core/src/main/scala/kafka/coordinator/transaction/TransactionCoordinator.scala`
- `core/src/main/scala/kafka/coordinator/transaction/TransactionStateManager.scala`
- `core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerChannelManager.scala`
- `core/src/main/scala/kafka/coordinator/transaction/TransactionMarkerRequestCompletionHandler.scala`
- `transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TransactionMetadata.java`
- `transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TransactionState.java`
- `transaction-coordinator/src/main/java/org/apache/kafka/coordinator/transaction/TxnTransitMetadata.java`
- Tests: `core/src/test/scala/unit/kafka/coordinator/transaction/TransactionCoordinatorTest.scala`,
  `TransactionStateManagerTest.scala`, `TransactionMarkerChannelManagerTest.scala`.

### Additional Exploration

- The planned migration of this coordinator onto `CoordinatorRuntime` (the group coordinator's
  runtime): findings here are useful input for that design.
- Interaction with `ProducerIdManager` / `RPCProducerIdManager` when the controller is slow
  to allocate a producer id block.
