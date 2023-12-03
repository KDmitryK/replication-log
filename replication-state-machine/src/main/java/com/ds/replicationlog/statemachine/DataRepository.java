package com.ds.replicationlog.statemachine;

import java.util.List;

public interface DataRepository {
    long appendData(String data);
    List<DataElement> getDataElements(long fromSeqNum);
}
