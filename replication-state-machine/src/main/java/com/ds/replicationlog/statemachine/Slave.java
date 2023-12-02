package com.ds.replicationlog.statemachine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Slave {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private static final long QUEUE_POLL_WAIT_MS = 1_000;
    private static final long AFTER_FAILURE_WAIT_MS = 1_000;
    private final Thread replicationThread;
    private final PriorityBlockingQueue<DataElement> replicationQueue;
    private final MasterClient masterClient;
    private final DataRepository repository;
    private final String replicaId;

    private volatile long appliedSeqNum = 0;

    public Slave(DataRepository repository, MasterClient masterClient, int replicationQueueCapacity, String replicaId) {
        this.repository = requireNonNull(repository);
        this.masterClient = requireNonNull(masterClient);
        if (replicationQueueCapacity < 1) {
            throw new IllegalArgumentException("Replication queue capacity must be positive");
        }
        this.replicationQueue = new PriorityBlockingQueue<>(replicationQueueCapacity, Comparator
                .comparingLong(DataElement::sequenceNum));
        this.replicaId = requireNonNull(replicaId);

        this.replicationThread = new Thread(() -> {
            var initialReplicationRequired = true;
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    if (initialReplicationRequired) {
                        initialReplicationRequired = !replicateBacklog(appliedSeqNum + 1, () -> {});
                    }
                    appendQueuedData();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    public List<DataElement> getData(long fromSeqNum) {
        return repository.getDataElements(fromSeqNum);
    }

    public void appendData(DataElement dataElement) {
        requireNonNull(dataElement);
        replicationQueue.add(dataElement);
    }

    private void appendQueuedData() throws InterruptedException {
        var dataElement = replicationQueue.poll(QUEUE_POLL_WAIT_MS, TimeUnit.MILLISECONDS);
        if (dataElement != null) {
            var successfulSave = true;
            if (appliedSeqNum >= dataElement.sequenceNum()) {
                acknowledge(new Acknowledgement(replicaId, dataElement.sequenceNum()));
            } else if (appliedSeqNum + 1 == dataElement.sequenceNum()) {
                successfulSave = appendDataAndAcknowledge(dataElement, replicationQueue::put);
            } else {
                successfulSave = replicateBacklog(appliedSeqNum + 1, () -> replicationQueue.put(dataElement));
            }
            if (!successfulSave) {
                Thread.sleep(AFTER_FAILURE_WAIT_MS);
            }
        }
    }

    private boolean replicateBacklog(long fromSeqNum, Runnable onError) {
        var successfulSave = true;
        try {
            getMasterData(fromSeqNum).stream().map(DataElement::data).forEach(this::appendData);
        } catch (RuntimeException e) {
            logger.warn("Failed backlog replication", e);
            successfulSave = false;
            onError.run();
        }
        return successfulSave;
    }

    private List<DataElement> getMasterData(long fromSeqNum) {
        return masterClient.getDataElements(fromSeqNum).stream().sorted(Comparator.comparingLong(
                DataElement::sequenceNum)).collect(Collectors.toList());
    }

    private void acknowledge(Acknowledgement acknowledgement) {
        try {
            masterClient.acknowledgeReception(acknowledgement);
        } catch (RuntimeException e) {
            logger.warn("Failed to acknowledge data element reception", e);
        }
    }

    private boolean appendDataAndAcknowledge(DataElement dataElement, Consumer<DataElement> onError) {
        var successfulSave = true;
        try {
            appendData(dataElement.data());
            acknowledge(new Acknowledgement(replicaId, dataElement.sequenceNum()));
        } catch (RuntimeException e) {
            successfulSave = false;
            logger.warn("Failed to append data", e);
            onError.accept(dataElement);
        }
        return successfulSave;
    }

    private void appendData(String data) {
        appliedSeqNum = repository.appendData(data);
    }

    public void start() {
        replicationThread.start();
    }

    public void stop() {
        replicationThread.interrupt();
    }
}
