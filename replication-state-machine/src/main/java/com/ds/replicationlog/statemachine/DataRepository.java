package com.ds.replicationlog.statemachine;

import com.ds.replicationlog.statemachine.DataElement;

import java.util.List;

public interface DataRepository {
    long appendData(String data);
    List<DataElement> getDataElements();
    void putDataElement(DataElement dataElement);
}
