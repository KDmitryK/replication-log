package com.ds.replicationlog.master.client;

import com.ds.replicationlog.statemachine.DataElement;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.verify.VerificationTimes;
import org.springframework.http.HttpStatus;

import java.net.HttpURLConnection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.matchers.Times.exactly;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.model.StringBody.exact;

public class SlavesClientImplTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SlavesClientImpl client = new SlavesClientImpl(List.of("localhost:8080", "localhost:8081"), 2);

    private ClientAndServer mockServer1;
    private ClientAndServer mockServer2;

    @BeforeEach
    void setUp() {
        mockServer1 = startClientAndServer(8080);
        mockServer2 = startClientAndServer(8081);
    }

    @AfterEach
    void tearDown() {
        mockServer1.stop();
        mockServer2.stop();
    }

    @Test
    public void constructionForEmptyHostsPortsFails() {
        assertThrows(IllegalArgumentException.class, () -> new SlavesClientImpl(List.of(), 2));
    }

    @Test
    public void appendDataForNullDataElementFails() {
        assertThrows(NullPointerException.class, () -> client.appendData(null));
    }

    @Test
    public void appendDataSucceedsForAllSlaves() throws JsonProcessingException {
        mockServer(8080, HttpURLConnection.HTTP_OK, 0);
        mockServer(8081, HttpURLConnection.HTTP_OK, 0);
        var dataElement = new DataElement("data", 1);

        client.appendData(dataElement);

        verifyRequestSent(8080, dataElement);
        verifyRequestSent(8081, dataElement);
    }

    private void mockServer(int port, int responseStatus, int delayMillis) {
        //noinspection resource
        new MockServerClient("127.0.0.1", port).when(
                        request()
                                .withMethod("POST")
                                .withPath("/slave/replication/append_data")
                                .withHeader("Content-type", "application/json"),
                        exactly(1))
                .respond(response()
                        .withStatusCode(responseStatus)
                        .withDelay(TimeUnit.MILLISECONDS, delayMillis));
    }

    private void verifyRequestSent(int port, DataElement dataElement) throws JsonProcessingException {
        //noinspection resource
        new MockServerClient("localhost", port).verify(
                request()
                        .withMethod("POST")
                        .withPath("/slave/replication/append_data")
                        .withBody(exact(objectMapper.writeValueAsString(dataElement))),
                VerificationTimes.exactly(1)
        );
    }

    @Test
    public void appendDataSuppressesErrorResponses() throws JsonProcessingException {
        mockServer(8080, HttpURLConnection.HTTP_INTERNAL_ERROR, 0);
        mockServer(8081, HttpURLConnection.HTTP_OK, 0);
        var dataElement = new DataElement("data", 1);

        client.appendData(dataElement);

        verifyRequestSent(8080, dataElement);
        verifyRequestSent(8081, dataElement);
    }

    @Test
    public void appendDataHandlesConnectionProblemsIndependently() throws JsonProcessingException {
        mockServer1.stop();
        mockServer(8081, HttpURLConnection.HTTP_OK, 0);
        var dataElement = new DataElement("data", 1);

        client.appendData(dataElement);

        verifyRequestSent(8081, dataElement);
    }

    @Test
    public void appendSendsRequestsInParallel() throws JsonProcessingException {
        mockServer(8080, HttpURLConnection.HTTP_OK, 2_000);
        mockServer(8081, HttpURLConnection.HTTP_OK, 2_000);
        var dataElement = new DataElement("data", 1);

        long startNanos = System.nanoTime();
        client.appendData(dataElement);

        verifyRequestSent(8080, dataElement);
        verifyRequestSent(8081, dataElement);
        assertTrue(3_000 > Duration.ofNanos(System.nanoTime() - startNanos).toMillis());
    }
}
