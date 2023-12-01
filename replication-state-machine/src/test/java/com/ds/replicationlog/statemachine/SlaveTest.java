package com.ds.replicationlog.statemachine;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class SlaveTest {
    private static final String REPLICA_ID = "r1";
    @Mock
    private MasterClient masterClient;
    @Mock
    private DataRepository repository;
    private Slave slave;

    @BeforeEach
    void setUp() {
        slave = new Slave(repository, masterClient, 100, REPLICA_ID);
    }

    @AfterEach
    void tearDown() {
        slave.stop();
    }

    @Test
    public void constructionForNullRepoFails() {
        assertThrows(NullPointerException.class, () -> new Slave(null, masterClient, 100, REPLICA_ID));
    }

    @Test
    public void constructionForNullClientFails() {
        assertThrows(NullPointerException.class, () -> new Slave(repository, null, 100, REPLICA_ID));
    }

    @Test
    public void constructionForNonPositiveQueueCapacityFails() {
        assertThrows(IllegalArgumentException.class, () -> new Slave(repository, masterClient, 0, REPLICA_ID));
    }

    @Test
    public void constructionForNullReplicaIdFails() {
        assertThrows(NullPointerException.class, () -> new Slave(repository, masterClient, 100, null));
    }

    @Test
    public void getDataSucceeds() {
        var fromSeqNum = 1;
        when(repository.getDataElements(fromSeqNum)).thenReturn(List.of(new DataElement("data", 1)));

        var result = slave.getData(fromSeqNum);

        assertNotNull(result);
        assertFalse(result.isEmpty());
    }

    @Test
    public void appendDataForNullDataElementFails() {
        assertThrows(NullPointerException.class, () -> slave.appendData(null));
    }

    @Test
    public void slaveRestoresStateFromMasterOnStart() throws InterruptedException {
        var masterData = List.of(new DataElement("data1", 1), new DataElement("data2", 2));
        when(masterClient.getDataElements(0)).thenReturn(masterData);
        when(repository.appendData("data1")).thenReturn(1L);
        when(repository.appendData("data2")).thenReturn(2L);

        slave.start();
        Thread.sleep(2_000);

        verify(repository).appendData("data1");
        verify(repository).appendData("data2");
        verify(masterClient, never()).acknowledgeReception(any());
    }

    @Test
    public void slaveForSavedElementSkipsSavingAndAcknowledges() throws InterruptedException {
        var masterData = List.of(new DataElement("data1", 1), new DataElement("data2", 2));
        when(masterClient.getDataElements(0)).thenReturn(masterData);
        when(repository.appendData("data1")).thenReturn(1L);
        when(repository.appendData("data2")).thenReturn(2L);

        slave.start();
        Thread.sleep(2_000);
        slave.appendData(new DataElement("data1", 1));

        verify(repository).appendData("data1");
        verify(repository).appendData("data2");
        verify(masterClient).acknowledgeReception(new Acknowledgement(REPLICA_ID, 1));
    }

    @Test
    public void slaveInRaceConditionFetchesMissingElementsFromMaster() throws InterruptedException {
        var masterData = List.of(new DataElement("data1", 1));
        when(masterClient.getDataElements(0)).thenReturn(masterData);
        when(repository.appendData("data1")).thenReturn(1L);
        when(masterClient.getDataElements(2)).thenReturn(List.of(new DataElement("data2", 2),
                new DataElement("data3", 3)));
        when(repository.appendData("data2")).thenReturn(2L);

        slave.start();
        Thread.sleep(2_000);
        slave.appendData(new DataElement("data3", 3));

        verify(repository).appendData("data1");
        verify(repository).appendData("data2");
        verify(repository).appendData("data3");
        verify(masterClient, never()).acknowledgeReception(any());
    }

    @Test
    public void slaveDoesInitialRestoreOnlyOnce() throws InterruptedException {
        when(masterClient.getDataElements(0)).thenReturn(List.of());

        slave.start();
        Thread.sleep(3_000);

        verify(masterClient).getDataElements(anyLong());
    }

    @Test
    public void slaveAcceptsDataElementAndAcknowledges() throws InterruptedException {
        when(masterClient.getDataElements(0)).thenReturn(List.of());
        when(repository.appendData("data1")).thenReturn(1L);
        when(repository.appendData("data2")).thenReturn(2L);

        slave.start();
        Thread.sleep(2_000);
        slave.appendData(new DataElement("data1", 1));
        slave.appendData(new DataElement("data2", 2));

        verify(masterClient).getDataElements(anyLong());
        verify(repository).appendData("data1");
        verify(masterClient).acknowledgeReception(new Acknowledgement(REPLICA_ID, 1));
        verify(repository).appendData("data2");
        verify(masterClient).acknowledgeReception(new Acknowledgement(REPLICA_ID, 2));
    }
}
