package com.ds.replicationlog.statemachine;

import java.util.List;

public interface MasterClient {
    void acknowledgeReception(Acknowledgement acknowledgement);
    List<DataElement> getDataElements(long fromSeqNum);
}
