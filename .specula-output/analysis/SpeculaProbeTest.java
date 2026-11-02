package org.apache.kafka.raft;

import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.MemoryRecords;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.kafka.common.message.BeginQuorumEpochResponseData;
import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SpeculaProbeTest {
    @Test
    public void testUnattachedWithLeaderReceivesFetchResponseFromLeader() throws Exception {
        int localId = 10;
        int voterId = 11;
        int leaderId = 12; // leader of the epoch but not in the locally known voter set
        int epoch = 5;
        List<InetSocketAddress> bootstrapServers = List.of(RaftClientTestContext.mockAddress(leaderId));

        RaftClientTestContext context = new RaftClientTestContext.Builder(localId, Set.of(voterId))
            .withBootstrapServers(Optional.of(bootstrapServers))
            .withKip853Rpc(true)
            .withElectedLeader(epoch, leaderId)
            .build();

        assertTrue(context.client.quorum().isUnattached());
        context.pollUntilRequest();
        RaftRequest.Outbound fetchRequest = context.assertSentFetchRequest();

        context.deliverResponse(
            fetchRequest.correlationId(),
            fetchRequest.destination(),
            context.fetchResponse(epoch, leaderId, MemoryRecords.EMPTY, 0L, Errors.NONE)
        );
        assertDoesNotThrow(context::poll);
    }

    @Test
    public void testLateBeginQuorumEpochResponseFromRemovedVoter() throws Exception {
        ReplicaKey local = replicaKey(100, true);
        ReplicaKey follower1 = replicaKey(101, true);
        ReplicaKey follower2 = replicaKey(102, true);
        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();

        context.unattachedToCandidate();
        int epoch = context.currentEpoch();
        context.expectAndGrantVotes(epoch);
        context.pollUntilRequest();
        RaftRequest.Outbound pendingToFollower2 = null;
        for (RaftRequest.Outbound request : context.collectBeginEpochRequests(epoch)) {
            if (request.destination().id() == follower2.id()) {
                pendingToFollower2 = request;
            } else {
                context.deliverResponse(request.correlationId(), request.destination(),
                    context.beginEpochResponse(epoch, local.id()));
                context.poll();
            }
        }
        assertTrue(pendingToFollower2 != null);
        assertTrue(context.client.quorum().isLeader());

        context.deliverRequest(context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0));
        context.pollUntilResponse();

        context.deliverRequest(context.removeVoterRequest(follower2));
        context.poll();
        context.poll();
        assertTrue(!context.client.quorum().isVoter(follower2));

        BeginQuorumEpochResponseData late = context.beginEpochResponse(epoch, local.id());
        context.deliverResponse(pendingToFollower2.correlationId(), pendingToFollower2.destination(), late);
        assertDoesNotThrow(context::poll);
    }
}
