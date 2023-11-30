package com.ds.replicationlog.statemachine;

public interface SlavesClient {
    void appendData(DataElement dataElement);
}
