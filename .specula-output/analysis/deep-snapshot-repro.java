package org.apache.kafka.raft.internals;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.KRaftVersionRecord;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.internal.MemoryRecords;
import org.apache.kafka.common.utils.internals.BufferSupplier;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.raft.ExternalKRaftMetrics;
import org.apache.kafka.raft.MockLog;
import org.apache.kafka.raft.VoterSet;
import org.apache.kafka.raft.VoterSetTest;
import org.apache.kafka.server.common.KRaftVersion;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.snapshot.RecordsSnapshotWriter;


import java.util.Optional;
import java.util.stream.IntStream;


public final class SpeculaNextOffsetRepro {
    static void assertEquals(Object a, Object b) { assertEquals(a, b, ""); }
    static void assertEquals(Object a, Object b, String m) { if (!java.util.Objects.equals(a, b)) System.out.println("  ASSERT FAILED: " + m + " expected=" + a + " actual=" + b); else System.out.println("  ok"); }
    static final ExternalKRaftMetrics M = x -> { };
    public static void main(String[] args) {
        new SpeculaNextOffsetRepro().voterRecordBelowStaleNextOffsetIsSkippedAfterTruncation();
        new SpeculaNextOffsetRepro().snapshotFreezeTrimThenTruncateAtHwLosesVoterSet();
        new SpeculaNextOffsetRepro().installedSnapshotBelowStaleNextOffsetIsNotReloaded();
    }
    void voterRecordBelowStaleNextOffsetIsSkippedAfterTruncation() {
        MockLog log = new MockLog(new TopicPartition("partition", 0), Uuid.randomUuid(), new LogContext());
        BufferSupplier bs = BufferSupplier.NO_CACHING;
        VoterSet staticVoters = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(1, 2, 3), true));
        KRaftControlRecordStateMachine ps = new KRaftControlRecordStateMachine(
            staticVoters, log, 1024, new LogContext(),
            new KafkaRaftMetrics(new Metrics(), "raft"), M);

        VoterSet v1 = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));
        VoterSet v2 = v1.addVoter(VoterSetTest.voterNode(7, true)).get();

        // epoch 1: V1 at offset 0, then 9 uncommitted kraft.version control batches (offsets 1..9)
        log.appendAsLeader(MemoryRecords.withVotersRecord(0, 0, 1, bs.get(300), v1.toVotersRecord((short) 0)), 1);
        for (int i = 1; i < 10; i++) {
            log.appendAsLeader(MemoryRecords.withKRaftVersionRecord(i, 0, 1, bs.get(300),
                new KRaftVersionRecord().setKRaftVersion((short) 1)), 1);
        }
        ps.updateState();
        assertEquals(v1, ps.lastVoterSet());

        // Diverging-epoch truncation to offset 1 (as KafkaRaftClient.handleFetchResponse does)
        log.truncateTo(1);
        ps.truncateNewEntries(1);

        // New leader (epoch 2) log: V2 at offset 1
        log.appendAsLeader(MemoryRecords.withVotersRecord(1, 0, 2, bs.get(300), v2.toVotersRecord((short) 0)), 2);
        ps.updateState();

        System.out.println("SPECULA lastVoterSet=" + ps.lastVoterSet() + " offset=" + ps.lastVoterSetOffset()
            + " logEnd=" + log.endOffset().offset() + " voterSetAtOffset(1)=" + ps.voterSetAtOffset(1));
        assertEquals(Optional.of(v2), ps.voterSetAtOffset(1), "voter set appended after truncation was skipped");
        assertEquals(v2, ps.lastVoterSet(), "voter set appended after truncation was skipped");
    }

    void snapshotFreezeTrimThenTruncateAtHwLosesVoterSet() {
        MockLog log = new MockLog(new TopicPartition("partition", 0), Uuid.randomUuid(), new LogContext());
        BufferSupplier bs = BufferSupplier.NO_CACHING;
        KRaftControlRecordStateMachine ps = new KRaftControlRecordStateMachine(
            VoterSet.empty(), log, 1024, new LogContext(),
            new KafkaRaftMetrics(new Metrics(), "raft"), M);

        VoterSet v1 = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));
        VoterSet v2 = v1.addVoter(VoterSetTest.voterNode(7, true)).get();

        log.appendAsLeader(MemoryRecords.withKRaftVersionRecord(0, 0, 1, bs.get(300),
            new KRaftVersionRecord().setKRaftVersion((short) 1)), 1);
        log.appendAsLeader(MemoryRecords.withVotersRecord(1, 0, 1, bs.get(300), v1.toVotersRecord((short) 0)), 1);
        for (int i = 2; i < 5; i++) {
            log.appendAsLeader(MemoryRecords.withKRaftVersionRecord(i, 0, 1, bs.get(300),
                new KRaftVersionRecord().setKRaftVersion((short) 1)), 1);
        }
        // uncommitted AddVoter record at offset 5 == HW
        log.appendAsLeader(MemoryRecords.withVotersRecord(5, 0, 1, bs.get(300), v2.toVotersRecord((short) 0)), 1);
        ps.updateState();
        assertEquals(v2, ps.lastVoterSet());

        // snapshot id offset 5 (<= HW 5) is frozen -> KafkaRaftClient.createSnapshot callback
        ps.truncateOldEntries(5);
        // diverging-epoch truncation to the HW (allowed: truncation offset == HW)
        log.truncateTo(5);
        ps.truncateNewEntries(5);

        System.out.println("SPECULA2 lastVoterSet=" + ps.lastVoterSet() + " entry=" + ps.lastVoterSetEntry()
            + " voterSetAtOffset(4)=" + ps.voterSetAtOffset(4));
        assertEquals(v1, ps.lastVoterSet(), "voter set at the snapshot offset was lost");
    }

    void installedSnapshotBelowStaleNextOffsetIsNotReloaded() {
        MockLog log = new MockLog(new TopicPartition("partition", 0), Uuid.randomUuid(), new LogContext());
        BufferSupplier bs = BufferSupplier.NO_CACHING;
        KRaftControlRecordStateMachine ps = new KRaftControlRecordStateMachine(
            VoterSet.empty(), log, 1024, new LogContext(),
            new KafkaRaftMetrics(new Metrics(), "raft"), M);

        VoterSet v1 = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(4, 5, 6), true));
        VoterSet bad = VoterSetTest.voterSet(VoterSetTest.voterMap(IntStream.of(8, 9), true));

        // divergent epoch-3 log of length 10 with an uncommitted voter set at offset 7
        log.appendAsLeader(MemoryRecords.withVotersRecord(0, 0, 3, bs.get(300), v1.toVotersRecord((short) 0)), 3);
        for (int i = 1; i < 10; i++) {
            if (i == 7) {
                log.appendAsLeader(MemoryRecords.withVotersRecord(i, 0, 3, bs.get(300), bad.toVotersRecord((short) 0)), 3);
            } else {
                log.appendAsLeader(MemoryRecords.withKRaftVersionRecord(i, 0, 3, bs.get(300),
                    new KRaftVersionRecord().setKRaftVersion((short) 1)), 3);
            }
        }
        ps.updateState();
        assertEquals(bad, ps.lastVoterSet());

        // follower downloads leader snapshot (5, 4) (higher epoch, lower offset than LEO 10)
        RecordsSnapshotWriter.Builder builder = new RecordsSnapshotWriter.Builder()
            .setRawSnapshotWriter(log.createNewSnapshotUnchecked(new OffsetAndEpoch(5, 4)).get())
            .setKraftVersion(KRaftVersion.KRAFT_VERSION_1)
            .setVoterSet(Optional.of(v1));
        try (RecordsSnapshotWriter<?> writer = builder.build(new StringSerde())) {
            writer.freeze();
        }
        assertEquals(true, log.truncateToLatestSnapshot());
        ps.updateState(); // KafkaRaftClient.handleFetchSnapshotResponse comment: "will always reload the snapshot"

        System.out.println("SPECULA3 logStart=" + log.startOffset() + " logEnd=" + log.endOffset().offset()
            + " lastVoterSet=" + ps.lastVoterSet() + " entry=" + ps.lastVoterSetEntry());
        assertEquals(v1, ps.lastVoterSet(), "snapshot was not reloaded; divergent voter set retained");
    }
}
