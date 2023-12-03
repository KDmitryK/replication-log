package com.ds.replicationlog.slave.client;

import com.ds.replicationlog.statemachine.Acknowledgement;
import com.ds.replicationlog.statemachine.DataElement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;

import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.StringBody.exact;

public class MasterClientImplTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MasterClientImpl client = new MasterClientImpl("localhost:8080", 2);

    private ClientAndServer mockServer;

    @BeforeEach
    void setUp() {
        mockServer = startClientAndServer(8080);
    }

    @AfterEach
    void tearDown() {
        mockServer.stop();
    }

    @Test
    public void acknowledgeReceptionForNullAcknowledgmentFails() {
        assertThrows(NullPointerException.class, () -> client.acknowledgeReception(null));
    }

    @Test
    public void acknowledgeReceptionSucceeds() throws JsonProcessingException {
        mockAcknowledge(HttpURLConnection.HTTP_OK, 0);
        var acknowledgment = new Acknowledgement("r1", 1);

        client.acknowledgeReception(acknowledgment);

        verifyAcknowledge(acknowledgment);
    }

    private void mockAcknowledge(int responseStatus, int delayMillis) {
        //noinspection resource
        new MockServerClient("127.0.0.1", 8080).when(
                        request()
                                .withMethod("POST")
                                .withPath("/master/replication/acknowledge_reception")
                                .withHeader("Content-type", "application/json"),
                        exactly(1))
                .respond(response()
                        .withStatusCode(responseStatus)
                        .withDelay(TimeUnit.MILLISECONDS, delayMillis));
    }

    private void verifyAcknowledge(Acknowledgement acknowledgement) throws JsonProcessingException {
        //noinspection resource
        new MockServerClient("localhost", 8080).verify(
                request()
                        .withMethod("POST")
                        .withPath("/master/replication/acknowledge_reception")
                        .withBody(exact(objectMapper.writeValueAsString(acknowledgement))),
                VerificationTimes.exactly(1)
        );
    }

    @Test
    public void acknowledgeReceptionForNonOkResponseFails() throws JsonProcessingException {
        mockAcknowledge(HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        var acknowledgment = new Acknowledgement("r1", 1);

        assertThrows(RuntimeException.class, () -> client.acknowledgeReception(acknowledgment));

        verifyAcknowledge(acknowledgment);
    }

    @Test
    public void acknowledgeReceptionForTimeoutFails() throws JsonProcessingException {
        mockAcknowledge(HttpURLConnection.HTTP_OK, 3_000);
        var acknowledgment = new Acknowledgement("r1", 1);

        assertThrows(RuntimeException.class, () -> client.acknowledgeReception(acknowledgment));

        verifyAcknowledge(acknowledgment);
    }

    @Test
    public void getDataElementsSucceeds() throws JsonProcessingException {
        var fromSeqNum = 1L;
        var expectedResult = List.of(new DataElement("data", fromSeqNum));
        mockGetData(fromSeqNum, HttpURLConnection.HTTP_OK, 0, expectedResult);

        var result = client.getDataElements(fromSeqNum);

        assertEquals(expectedResult, result);
        verifyGetData(fromSeqNum);
    }

    private void mockGetData(long fromSeqNum, int responseStatus, int delayMillis, List<DataElement> result)
            throws JsonProcessingException {
        //noinspection resource
        new MockServerClient("127.0.0.1", 8080).when(
                        request()
                                .withMethod("GET")
                                .withPath("/master/data/get_data/" + fromSeqNum)
                                .withHeader("Content-type", "application/json"),
                        exactly(1))
                .respond(response()
                        .withStatusCode(responseStatus)
                        .withBody(objectMapper.writeValueAsString(result))
                        .withDelay(TimeUnit.MILLISECONDS, delayMillis));
    }

    private void verifyGetData(long fromSeqNum) {
        //noinspection resource
        new MockServerClient("localhost", 8080).verify(
                request()
                        .withMethod("GET")
                        .withPath("/master/data/get_data/" + fromSeqNum),
                VerificationTimes.exactly(1)
        );
    }

    @Test
    public void getDataForNonOkStatusFails() throws JsonProcessingException {
        var fromSeqNum = 1L;
        var expectedResult = List.of(new DataElement("data", fromSeqNum));
        mockGetData(fromSeqNum, HttpURLConnection.HTTP_INTERNAL_ERROR, 0, expectedResult);

        assertThrows(RuntimeException.class, () -> client.getDataElements(fromSeqNum));

        verifyGetData(fromSeqNum);
    }

    @Test
    public void getDataForTimeoutFails() throws JsonProcessingException {
        var fromSeqNum = 1L;
        var expectedResult = List.of(new DataElement("data", fromSeqNum));
        mockGetData(fromSeqNum, HttpURLConnection.HTTP_OK, 3_000, expectedResult);

        assertThrows(RuntimeException.class, () -> client.getDataElements(fromSeqNum));

        verifyGetData(fromSeqNum);
    }
}
