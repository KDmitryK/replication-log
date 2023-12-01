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
@RequestMapping("/master/")
public class MasterController {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @PostMapping("data/append_data")
    public void appendData(@RequestBody AppendDataRequest appendDataRequest) {
        logger.info("Append data was called for: {}", appendDataRequest);
    }

    @GetMapping(value = "data/get_data", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<String> getData() {
        logger.info("Get data was executed");
        return List.of();
    }
}
