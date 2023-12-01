package com.ds.replicationlog.statemachine;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Slave {
    private static final long QUEUE_POLL_WAIT_MS = 1_000;
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
           try {
               appendQueuedData();
           } catch (InterruptedException e){
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
        replicateBacklog(appliedSeqNum);
        while (!Thread.currentThread().isInterrupted()) {
            var dataElement = replicationQueue.poll(QUEUE_POLL_WAIT_MS, TimeUnit.MILLISECONDS);
            if (dataElement == null) {
                continue;
            }
            if (appliedSeqNum >= dataElement.sequenceNum()) {
                masterClient.acknowledgeReception(new Acknowledgement(replicaId, dataElement.sequenceNum()));
            } else if (appliedSeqNum + 1 == dataElement.sequenceNum()) {
                appendData(dataElement.data());
                masterClient.acknowledgeReception(new Acknowledgement(replicaId, dataElement.sequenceNum()));
            } else {
                replicateBacklog(appliedSeqNum + 1);
            }
        }
    }

    private void replicateBacklog(long fromSeqNum) {
        getMaterData(fromSeqNum).forEach(dataElement -> appliedSeqNum = repository.appendData(dataElement.data()));
    }

    private List<DataElement> getMaterData(long fromSeqNum) {
        return masterClient.getDataElements(fromSeqNum).stream().sorted(Comparator.comparingLong(
                DataElement::sequenceNum)).collect(Collectors.toList());
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
