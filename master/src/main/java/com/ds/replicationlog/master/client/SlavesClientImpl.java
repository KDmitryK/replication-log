package com.ds.replicationlog.master.client;

import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.SlavesClient;
import org.springframework.stereotype.Component;

@Component
public class SlavesClientImpl implements SlavesClient {

    @Override
    public void appendData(DataElement dataElement) {

    }
}
