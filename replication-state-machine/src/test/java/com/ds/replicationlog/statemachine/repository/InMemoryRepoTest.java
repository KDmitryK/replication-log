package com.ds.replicationlog.statemachine.repository;

import com.ds.replicationlog.statemachine.DataElement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.UUID;

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

        addedData.sort(Comparator.comparingLong(DataElement::sequenceNum));
        assertEquals(addedData, repo.getDataElements());
    }

    @Test
    public void putDataElementForNullDataElementFails() {
        var repo = new InMemoryRepo();

        assertThrows(NullPointerException.class, () -> repo.putDataElement(null));
    }

    @Test
    public void putDataElementIsIdempotent() {
        var repo = new InMemoryRepo();
        var toBeSaved = new DataElement("1", 1);
        repo.putDataElement(toBeSaved);
        repo.putDataElement(new DataElement("2", 1));

        var dataElements = repo.getDataElements();

        assertEquals(1, dataElements.size());
        assertEquals(toBeSaved, dataElements.get(0));
    }
}
