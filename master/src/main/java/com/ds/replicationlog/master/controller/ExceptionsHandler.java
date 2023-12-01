package com.ds.replicationlog.master.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.concurrent.TimeoutException;

@ControllerAdvice
public class ExceptionsHandler {

    @ExceptionHandler({TimeoutException.class})
    public ResponseEntity<Object> handleTimeoutException(TimeoutException exception) {
        return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Timeout during request processing");
    }

    @ExceptionHandler({RuntimeException.class})
    public ResponseEntity<Object> handleRuntimeException(RuntimeException exception) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Internal problem");
    }
}
