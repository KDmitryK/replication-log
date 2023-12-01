package com.ds.replicationlog.statemachine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MasterTest {
    private final Duration minAcknowledgmentsWaitTime = Duration.ofSeconds(3);
    @Mock
    private DataRepository repository;
    @Mock
    private SlavesClient slavesClient;

    @Test
    public void constructionForNullRepoFails() {
        assertThrows(NullPointerException.class, () -> new Master(null, minAcknowledgmentsWaitTime, slavesClient));
    }

    @Test
    public void constructionForNullMinAcknowledgmentsWaitTimeFails() {
        //noinspection DataFlowIssue
        assertThrows(NullPointerException.class, () -> new Master(repository, null, slavesClient));
    }

    @Test
    public void constructionForNonPositiveMinAcknowledgmentsWaitTimeFails() {
        assertThrows(IllegalArgumentException.class, () -> new Master(repository, Duration.ZERO, slavesClient));
    }

    @Test
    public void constructionForNullClientFails() {
        assertThrows(NullPointerException.class, () -> new Master(repository, minAcknowledgmentsWaitTime, null));
    }

    @Test
    public void getDataReturnsSavedItems() {
        var fromSeqNum = 5L;
        when(repository.getDataElements(fromSeqNum)).thenReturn(List.of(new DataElement("data", 1)));
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);

        var result = master.getData(fromSeqNum);

        assertEquals(List.of(new DataElement("data", 1)), result);
        verify(slavesClient, never()).appendData(any());
    }

    @Test
    public void appendDataForNegativeMinAcknowledgmentsFails() {
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);

        assertThrows(IllegalArgumentException.class, () -> master.appendData(-1, "data"));
    }

    @Test
    public void appendDataForNullDataFails() {
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);

        assertThrows(NullPointerException.class, () -> master.appendData(0, null));
    }

    @Test
    public void appendDataForForZeroMinAcknowledgmentsDoesNotWaitForAcknowledgments() throws InterruptedException,
            TimeoutException {
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);
        var data = "data";
        var seqNum = 1L;
        when(repository.appendData(data)).thenReturn(seqNum);

        master.appendData(0, data);

        verify(slavesClient).appendData(new DataElement(data, seqNum));
    }

    @Test
    public void appendDataIgnoresUnknownAcknowledgments() throws InterruptedException, TimeoutException {
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);
        var data = "data";
        var seqNum = 1L;
        when(repository.appendData(data)).thenReturn(seqNum);
        master.appendData(0, data);

        master.acknowledgeReception(new Acknowledgement("r1", seqNum));

        verify(slavesClient).appendData(new DataElement(data, seqNum));
    }

    @Test
    public void appendDataIgnoresAcknowledgmentDuplicates() throws InterruptedException, TimeoutException {
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);
        var data = "data";
        var seqNum = 2L;
        var replicaId1 = "r1";
        var replicaId2 = "r2";
        when(repository.appendData(data)).thenReturn(seqNum);
        Thread.startVirtualThread(() -> {
            //noinspection CatchMayIgnoreException
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
            }
            master.acknowledgeReception(new Acknowledgement(replicaId1, seqNum));
            master.acknowledgeReception(new Acknowledgement(replicaId1, seqNum));
            //noinspection CatchMayIgnoreException
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
            }
            master.acknowledgeReception(new Acknowledgement(replicaId2, seqNum));
        });

        var startNano = System.nanoTime();
        master.appendData(2, data);

        assertTrue(Duration.ofNanos(System.nanoTime() - startNano).toSeconds() >= 2);
        verify(slavesClient).appendData(new DataElement(data, seqNum));
    }

    @Test
    public void appendDataIfAcknowledgmentsAreNotReceivedInTimeInterrupts() {
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);
        var data = "data";
        var seqNum = 2L;
        var replicaId1 = "r1";
        when(repository.appendData(data)).thenReturn(seqNum);
        Thread.startVirtualThread(() -> {
            //noinspection CatchMayIgnoreException
            try {
                Thread.sleep(1_000);
            } catch (InterruptedException e) {
            }
            master.acknowledgeReception(new Acknowledgement(replicaId1, seqNum));
            master.acknowledgeReception(new Acknowledgement(replicaId1, seqNum));
        });

        assertThrows(TimeoutException.class, () -> master.appendData(2, data));

        verify(slavesClient).appendData(new DataElement(data, seqNum));
    }

    @Test
    public void appendDataForFailedReplicasUpdateFailsBecauseOfTimeout() {
        var master = new Master(repository, minAcknowledgmentsWaitTime, slavesClient);
        var data = "data";
        var seqNum = 2L;
        when(repository.appendData(data)).thenReturn(seqNum);
        doThrow(new RuntimeException("test")).when(slavesClient).appendData(new DataElement(data, seqNum));

        assertThrows(TimeoutException.class, () -> master.appendData(2, data));
    }
}
