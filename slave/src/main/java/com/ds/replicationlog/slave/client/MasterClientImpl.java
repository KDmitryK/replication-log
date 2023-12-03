package com.ds.replicationlog.slave.client;

import com.ds.replicationlog.statemachine.Acknowledgement;
import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.MasterClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;

import static java.util.Objects.requireNonNull;

@Component
public class MasterClientImpl implements MasterClient {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String acknowledgeUri;
    private final String dataElementsUri;
    private final Duration masterRequestTimeout;

    public MasterClientImpl(@Value("${masterHostPort}") String masterHostPort,
                            @Value("${masterRequestTimeoutSeconds}")  int masterRequestTimeoutSeconds) {
        this.acknowledgeUri = "http://%s/master/replication/acknowledge_reception".formatted(masterHostPort);
        this.dataElementsUri = "http://%s/master/data/get_data/".formatted(masterHostPort);
        this.masterRequestTimeout = Duration.ofSeconds(masterRequestTimeoutSeconds);
    }

    @Override
    public void acknowledgeReception(Acknowledgement acknowledgement) {
        requireNonNull(acknowledgement);
        try {
            var httpRequest = createAcknowledgmentRequest(acknowledgement);
            if (sendHttpRequest(httpRequest).statusCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed to send acknowledgment");
            }
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException("Failed to send acknowledge", e);
        }
    }

    private HttpRequest createAcknowledgmentRequest(Acknowledgement acknowledgement) throws URISyntaxException,
            JsonProcessingException {
        var jsonRequest = objectMapper.writeValueAsString(acknowledgement);
        return HttpRequest.newBuilder()
                .uri(new URI(acknowledgeUri))
                .timeout(masterRequestTimeout)
                .headers("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(jsonRequest.getBytes())))
                .build();
    }

    private HttpResponse<String> sendHttpRequest(HttpRequest request) throws URISyntaxException,
            IOException, InterruptedException {
        try (var client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            return client.send(request, HttpResponse.BodyHandlers.ofString());
        }
    }

    @Override
    public List<DataElement> getDataElements(long fromSeqNum) {
        try {
            var httpRequest = createDataRequest(fromSeqNum);
            var response = sendHttpRequest(httpRequest);
            if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed to retrieve data elements");
            }
            return objectMapper.readValue(response.body(), new TypeReference<>() {});
        } catch (URISyntaxException | IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private HttpRequest createDataRequest(long fromSeqNum) throws URISyntaxException {
        return HttpRequest.newBuilder()
                .uri(new URI(dataElementsUri + fromSeqNum))
                .timeout(masterRequestTimeout)
                .headers("Content-Type", "application/json")
                .GET()
                .build();
    }
}
