package com.ds.replicationlog.master.controller;

import com.ds.replicationlog.statemachine.Acknowledgement;
import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.Master;
import io.swagger.v3.oas.annotations.Hidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

@RestController
@RequestMapping("/master/")
public class MasterController {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Master master;

    public MasterController(Master master) {
        this.master = requireNonNull(master);
    }

    @PostMapping("data/append_data")
    public void appendData(@RequestBody AppendDataRequest appendDataRequest) throws TimeoutException {
        logger.debug("Append data was called for: {}", appendDataRequest);
        try {
            master.appendData(appendDataRequest.minAcknowledgments(), appendDataRequest.data());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Operation was interrupted", e);
        }
    }

    @GetMapping(value = "data/get_data", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<DataElement> getData() {
        logger.debug("Get data was executed");
        return master.getData(0);
    }

    @Hidden
    @GetMapping(value = "data/get_data/{fromSeqNum}", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<DataElement> getData(@PathVariable long fromSeqNum) {
        logger.debug("Get data was executed for fromSeqNum: {}", fromSeqNum);
        return master.getData(fromSeqNum);
    }

    @Hidden
    @PostMapping("replication/acknowledge_reception")
    public void acknowledgeReception(@RequestBody Acknowledgement acknowledgement) {
        logger.debug("Reception by slave acknowledgment: {}", acknowledgement);
        master.acknowledgeReception(acknowledgement);
    }
}
