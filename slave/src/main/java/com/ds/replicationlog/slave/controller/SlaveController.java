package com.ds.replicationlog.slave.controller;

import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.Slave;
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

import static java.util.Objects.requireNonNull;

@RestController
@RequestMapping("/slave/")
public class SlaveController {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final Slave slave;

    public SlaveController(Slave slave) {
        this.slave = requireNonNull(slave);
    }

    @GetMapping(value = "data/get_data", produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseBody
    public List<DataElement> getData() {
        logger.debug("Get data was executed");
        return slave.getData(0);
    }

    @PostMapping("replication/append_data")
    public void appendData(@RequestBody DataElement dataElement) {
        logger.debug("Replication data append: {}", dataElement);
        slave.appendData(dataElement);
    }
}
