package com.ds.replicationlog.statemachine;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

public class Master {
    private final Map<Long, ReplicationState> acknowledges = new ConcurrentHashMap<>();
    private final DataRepository repository;
    private final Duration minAcknowledgmentsWaitTime;
    private final PeerClient<DataElement, Void> peerClient;

    public Master(DataRepository repository, Duration minAcknowledgmentsWaitTime,
                  PeerClient<DataElement, Void> peerClient) {
        this.repository = requireNonNull(repository);
        if (!minAcknowledgmentsWaitTime.isPositive()) {
            throw new IllegalArgumentException("Acknowledgments wait time must be positive");
        }
        this.minAcknowledgmentsWaitTime = requireNonNull(minAcknowledgmentsWaitTime);
        this.peerClient = requireNonNull(peerClient);
    }

    public List<DataElement> getData() {
        return repository.getDataElements();
    }

    public void appendData(int minAcknowledgments, String data) throws InterruptedException, TimeoutException {
        if (minAcknowledgments < 0) {
            throw new IllegalArgumentException("Min acknowledgments must be positive");
        }
        requireNonNull(data);
        var seqNum = repository.appendData(data);
        replicateAndWaitAcknowledgements(seqNum, minAcknowledgments, new DataElement(data, seqNum));
    }

    private void replicateAndWaitAcknowledgements(long seqNum, int minAcknowledgments, DataElement dataElement)
            throws InterruptedException, TimeoutException {
        var replicationState = new ReplicationState(minAcknowledgments, minAcknowledgmentsWaitTime);
        acknowledges.put(seqNum, replicationState);
        try {
            peerClient.sendNotification(dataElement);
            if (minAcknowledgments > 0) {
                replicationState.waitAcknowledgments();
            }
        } finally {
            acknowledges.remove(seqNum);
        }
    }

    public void acknowledgeReception(Acknowledgement acknowledgement) {
        requireNonNull(acknowledgement);
        acknowledges.computeIfPresent(acknowledgement.sequenceNum(), (k, rs) -> {
            rs.markAcknowledgement(acknowledgement.replicaId());
            return rs;
        });
    }

    private static class ReplicationState {
        private final CountDownLatch latchLatch;
        private final Set<String> replicasIds = new HashSet<>();
        private final Duration minAcknowledgmentsWaitTime;

        private ReplicationState(int minAcknowledgments, Duration minAcknowledgmentsWaitTime) {
            this.latchLatch = new CountDownLatch(minAcknowledgments);
            this.minAcknowledgmentsWaitTime = minAcknowledgmentsWaitTime;
        }

        private void markAcknowledgement(String replicaId) {
            if (replicasIds.add(replicaId)) {
                latchLatch.countDown();
            }
        }

        private void waitAcknowledgments() throws InterruptedException, TimeoutException {
            if (!latchLatch.await(minAcknowledgmentsWaitTime.toMillis(), TimeUnit.MILLISECONDS)) {
                throw new TimeoutException("");
            }
        }
    }
}
