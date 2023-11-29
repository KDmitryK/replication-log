package com.ds.replicationlog.statemachine;

import static java.util.Objects.requireNonNull;

public record DataElement(String data, long sequenceNum) {
    public DataElement {
        requireNonNull(data);
    }
}
