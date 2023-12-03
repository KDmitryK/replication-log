package com.ds.replicationlog.statemachine.repository;

import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.DataRepository;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class InMemoryRepo implements DataRepository {
    private final AtomicLong sequenceNum = new AtomicLong();
    private final Map<Long, DataElement> dataElements = new ConcurrentHashMap<>();
    @Override
    public long appendData(String data) {
        requireNonNull(data);
        var seqNum = sequenceNum.incrementAndGet();
        dataElements.put(seqNum, new DataElement(data, seqNum));
        return seqNum;
    }

    @Override
    public List<DataElement> getDataElements(long fromSeqNum) {
        return dataElements.values().stream().sorted(Comparator.comparingLong(DataElement::sequenceNum))
                .filter(d -> d.sequenceNum() >= fromSeqNum).collect(Collectors.toList());
    }
}
