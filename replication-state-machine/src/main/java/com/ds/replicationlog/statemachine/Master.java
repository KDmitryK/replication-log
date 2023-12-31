package com.ds.replicationlog.statemachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Map<Long, ReplicationState> acknowledges = new ConcurrentHashMap<>();
    private final DataRepository repository;
    private final Duration minAcknowledgmentsWaitTime;
    private final SlavesClient slavesClient;

    public Master(DataRepository repository, Duration minAcknowledgmentsWaitTime, SlavesClient slavesClient) {
        this.repository = requireNonNull(repository);
        if (!minAcknowledgmentsWaitTime.isPositive()) {
            throw new IllegalArgumentException("Acknowledgments wait time must be positive");
        }
        this.minAcknowledgmentsWaitTime = requireNonNull(minAcknowledgmentsWaitTime);
        this.slavesClient = requireNonNull(slavesClient);
    }

    public List<DataElement> getData(long fromSeqNum) {
        return repository.getDataElements(fromSeqNum);
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
            updateReplicas(dataElement);
            if (minAcknowledgments > 0) {
                replicationState.waitAcknowledgments();
            }
        } finally {
            acknowledges.remove(seqNum);
        }
    }

    private void updateReplicas(DataElement dataElement) {
        try {
            slavesClient.appendData(dataElement);
        } catch (RuntimeException e) {
            logger.warn("Failed to send updates to replicas", e);
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
                throw new TimeoutException("Waiting for acknowledgments has exceeded the timeout");
            }
        }
    }
}
