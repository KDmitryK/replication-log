package com.ds.replicationlog.master.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/data/")
public class DataController {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostMapping("append_message")
    public void appendMessage(@RequestBody  Message message) {
        logger.info("Append message was called for: {}", message);
    }

    @GetMapping(value = "get_messages", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<String> getMessages() {
        logger.info("Get messages was executed");
        return List.of();
    }
}
