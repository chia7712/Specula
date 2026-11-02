package org.apache.kafka.raft;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpeculaElectionProbeTest {
    @Test
    public void testUnattachedWithLeaderReceivesSuccessfulFetchFromLeader() throws Exception {
        ReplicaKey local = replicaKey(100, true);
        ReplicaKey voter2 = replicaKey(101, true);
        int leaderId = 102; // leader not in the local voter set
        int epoch = 3;

        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, voter2));
        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withBootstrapSnapshot(Optional.of(voters))
            .withElectedLeader(epoch, leaderId)
            .withKip853Rpc(true)
            .build();

        assertTrue(context.client.quorum().isUnattached(), "expected unattached with leader");
        context.pollUntilRequest();
        RaftRequest.Outbound fetch = context.assertSentFetchRequest();

        Endpoints leaderEndpoints = Endpoints.fromInetSocketAddresses(
            Map.of(context.channel.listenerName(), InetSocketAddress.createUnresolved("localhost", 9990 + leaderId))
        );
        var response = RaftUtil.singletonFetchResponse(
            context.channel.listenerName(),
            context.raftProtocol.fetchRpcVersion(),
            context.metadataPartition,
            context.metadataTopicId,
            Errors.NONE,
            leaderId,
            leaderEndpoints,
            partitionData -> {
                partitionData.setRecords(MemoryRecords.EMPTY).setErrorCode(Errors.NONE.code()).setHighWatermark(0L);
                partitionData.currentLeader().setLeaderEpoch(epoch).setLeaderId(leaderId);
            }
        );
        context.deliverResponse(fetch.correlationId(), fetch.destination(), response);
        Throwable thrown = null;
        try {
            context.poll();
        } catch (Throwable t) {
            thrown = t;
        }
        System.out.println("SPECULA_PROBE state=" + context.client.quorum().leaderAndEpoch()
            + " isFollower=" + context.client.quorum().isFollower() + " thrown=" + thrown);
        if (thrown != null) thrown.printStackTrace(System.out);
    }
}
