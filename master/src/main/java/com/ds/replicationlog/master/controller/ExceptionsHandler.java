package com.ds.replicationlog.master.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.util.concurrent.TimeoutException;

@ControllerAdvice
public class ExceptionsHandler {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @ExceptionHandler({TimeoutException.class})
    public ResponseEntity<Object> handleTimeoutException(TimeoutException exception) {
        logger.warn("Request has failed because of timeout", exception);
        return ResponseEntity.status(HttpStatus.REQUEST_TIMEOUT).body("Timeout during request processing");
    }

    @ExceptionHandler({RuntimeException.class})
    public ResponseEntity<Object> handleRuntimeException(RuntimeException exception) {
        logger.warn("Request has failed because of internal error", exception);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Internal problem");
    }
}
