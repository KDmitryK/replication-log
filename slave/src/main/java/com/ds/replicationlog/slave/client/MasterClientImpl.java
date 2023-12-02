package com.ds.replicationlog.slave.client;

import com.ds.replicationlog.statemachine.Acknowledgement;
import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.MasterClient;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class MasterClientImpl implements MasterClient {
    @Override
    public void acknowledgeReception(Acknowledgement acknowledgement) {

    }

    @Override
    public List<DataElement> getDataElements(long fromSeqNum) {
        return null;
    }
}
