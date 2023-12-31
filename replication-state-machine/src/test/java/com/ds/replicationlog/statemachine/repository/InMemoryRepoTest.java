package com.ds.replicationlog.statemachine.repository;

import com.ds.replicationlog.statemachine.DataElement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class InMemoryRepoTest {

    @Test
    public void appendDataForNullDataFails() {
        var repo = new InMemoryRepo();

        assertThrows(NullPointerException.class, () -> repo.appendData(null));
    }

    @Test
    public void appendDataAddsDataWithIncrementedSequenceNum() {
        var repo = new InMemoryRepo();
        var addedData = new ArrayList<DataElement>();

        for (var i = 0; i < 1_000; i++) {
            var data = UUID.randomUUID().toString();
            var seqNum = repo.appendData(data);
            addedData.add(new DataElement(data, seqNum));
        }

        assertEquals(addedData.stream().filter(d -> d.sequenceNum() >= 500).collect(Collectors.toList()),
                repo.getDataElements(500));
    }
}
