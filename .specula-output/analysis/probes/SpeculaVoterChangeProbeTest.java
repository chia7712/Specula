/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft;

import org.apache.kafka.common.protocol.Errors;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static org.apache.kafka.raft.KafkaRaftClientTest.replicaKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Temporary probe tests written by Specula code analysis; not meant to be committed upstream. */
public class SpeculaVoterChangeProbeTest {

    // Probe 1: a BeginQuorumEpoch response from a voter that was just removed makes
    // LeaderState.addAcknowledgementFrom throw IllegalArgumentException on the raft IO thread.
    @Test
    void beginQuorumEpochResponseFromRemovedVoterThrows() throws Exception {
        ReplicaKey local = replicaKey(5, true);
        ReplicaKey follower1 = replicaKey(6, true);
        ReplicaKey follower2 = replicaKey(7, true);
        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();
        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        // follower1 fetches and establishes the HWM
        context.deliverRequest(context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        // follower2 never fetches: after beginQuorumEpochTimeoutMs the leader re-sends BeginQuorumEpoch to it
        context.time.sleep(context.beginQuorumEpochTimeoutMs);
        context.deliverRequest(context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0));
        context.pollUntilResponse();
        context.client.poll();
        List<RaftRequest.Outbound> begin = context.collectBeginEpochRequests(epoch);
        RaftRequest.Outbound toFollower2 = begin.stream()
            .filter(r -> r.destination().id() == follower2.id()).findFirst().orElseThrow();

        // Remove follower2 and append the VotersRecord
        context.deliverRequest(context.removeVoterRequest(follower2));
        context.poll();
        context.poll();
        assertFalse(context.client.quorum().isVoter(follower2));

        // Now follower2's BeginQuorumEpoch response arrives
        context.deliverResponse(
            toFollower2.correlationId(),
            toFollower2.destination(),
            context.beginEpochResponse(epoch, local.id())
        );
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> context.client.poll());
        assertTrue(e.getMessage().contains("non-voter"), e.getMessage());
    }

    // Probe 2: the leader removes itself, the RemoveVoter RPC times out before the VotersRecord
    // commits, then it commits. The leader never resigns and keeps leading while not a voter.
    @Test
    void leaderRemovesItselfCommitAfterTimeoutNeverResigns() throws Exception {
        ReplicaKey local = replicaKey(5, true);
        ReplicaKey follower1 = replicaKey(6, true);
        ReplicaKey follower2 = replicaKey(7, true);
        VoterSet voters = VoterSetTest.voterSet(Stream.of(local, follower1, follower2));

        RaftClientTestContext context = new RaftClientTestContext.Builder(local.id(), local.directoryId().get())
            .withKip853Rpc(true)
            .withBootstrapSnapshot(Optional.of(voters))
            .withUnknownLeader(3)
            .build();
        context.unattachedToLeader();
        int epoch = context.currentEpoch();

        context.deliverRequest(context.fetchRequest(epoch, follower1, context.log.endOffset().offset(), epoch, 0));
        context.pollUntilResponse();
        context.assertSentFetchPartitionResponse(Errors.NONE, epoch, OptionalInt.of(local.id()));

        context.deliverRequest(context.removeVoterRequest(local));
        context.poll();
        context.poll();
        assertFalse(context.client.quorum().isVoter(local));

        // Let the RemoveVoter RPC time out before the record commits
        context.time.sleep(context.requestTimeoutMs());
        context.pollUntilResponse();
        context.assertSentRemoveVoterResponse(Errors.REQUEST_TIMED_OUT);

        // Now both remaining voters fetch to the LEO: the VotersRecord commits
        long leo = context.log.endOffset().offset();
        long hwBefore = context.client.highWatermark().orElse(-1);
        for (int i = 0; i < 6; i++) {
            context.deliverRequest(context.fetchRequest(epoch, follower1, leo, epoch, 0));
            context.pollUntilResponse();
            context.deliverRequest(context.fetchRequest(epoch, follower2, leo, epoch, 0));
            context.pollUntilResponse();
            context.time.sleep(context.fetchTimeoutMs / 2);
            context.client.poll();
        }
        assertTrue(context.client.highWatermark().orElse(-1) >= leo, "HW " + context.client.highWatermark() + " before " + hwBefore);
        assertFalse(context.client.quorum().isVoter(local));
        // The removed leader is still leader of the same epoch
        assertTrue(context.client.quorum().isLeader());
        assertEquals(epoch, context.currentEpoch());
    }
}
