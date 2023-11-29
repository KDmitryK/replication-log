package com.ds.replicationlog.statemachine;

import static java.util.Objects.requireNonNull;

public record Acknowledgement(String replicaId, long sequenceNum) {

    public Acknowledgement {
        requireNonNull(replicaId);
    }
}
