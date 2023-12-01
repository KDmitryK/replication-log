package com.ds.replicationlog.master.controller;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExceptionsHandlerTest {
    private final ExceptionsHandler handler = new ExceptionsHandler();

    @Test
    public void handleTimeoutExceptionReturnsRequestTimeout() {
        var response = handler.handleTimeoutException(new TimeoutException("test"));

        assertEquals(HttpStatus.REQUEST_TIMEOUT, response.getStatusCode());
    }

    @Test
    public void handleRuntimeExceptionReturnsInternalServerError() {
        var response = handler.handleRuntimeException(new RuntimeException("test"));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    }
}
