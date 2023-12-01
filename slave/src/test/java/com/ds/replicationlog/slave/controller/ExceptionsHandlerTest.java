package com.ds.replicationlog.slave.controller;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExceptionsHandlerTest {
    private final ExceptionsHandler handler = new ExceptionsHandler();

    @Test
    public void handleRuntimeExceptionReturnsInternalServerError() {
        var response = handler.handleRuntimeException(new RuntimeException("test"));

        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    }
}
