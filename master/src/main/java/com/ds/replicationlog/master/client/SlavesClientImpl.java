package com.ds.replicationlog.master.client;

import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.SlavesClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Component
public class SlavesClientImpl implements SlavesClient {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final List<String> appendEndpoints;
    private final Duration slaveRequestTimeout;

    public SlavesClientImpl(@Value("${slavesHostsPorts}") List<String> hostsPorts,
                            @Value("${slaveRequestTimeoutSeconds}") int slaveRequestTimeoutSeconds) {
        if (hostsPorts.isEmpty()) {
            throw new IllegalArgumentException("Hosts ports cannot be empty");
        }
        this.appendEndpoints = hostsPorts.stream().map("http://%s/slave/replication/append_data"::formatted)
                .collect(Collectors.toList());
        this.slaveRequestTimeout = Duration.ofSeconds(slaveRequestTimeoutSeconds);
    }

    @Override
    public void appendData(DataElement dataElement) {
        requireNonNull(dataElement);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            appendEndpoints.forEach(endpoint -> executor.submit(() -> sendRequest(endpoint, dataElement)));
        }
    }

    private void sendRequest(String endpoint, DataElement dataElement) {
        try {
            var status = sendHttpRequest(endpoint, dataElement);
            if (status != HttpStatus.OK) {
                logger.warn("Failed to send data update to slave, status: {}", status);
            }
        } catch (Exception e) {
            logger.warn("Failed to send data update to slave", e);
        }
    }

    private HttpStatus sendHttpRequest(String uri, DataElement dataElement) throws URISyntaxException, IOException,
            InterruptedException {
        byte[] dataElementBytes = objectMapper.writeValueAsString(dataElement).getBytes();
        var request = HttpRequest.newBuilder()
                .uri(new URI(uri))
                .timeout(slaveRequestTimeout)
                .headers("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofInputStream(() -> new ByteArrayInputStream(dataElementBytes)))
                .build();

        try (var client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            return HttpStatus.valueOf(client.send(request, HttpResponse.BodyHandlers.ofString()).statusCode());
        }
    }
}
