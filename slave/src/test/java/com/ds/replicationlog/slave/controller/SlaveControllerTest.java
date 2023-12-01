package com.ds.replicationlog.slave.controller;

import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.Slave;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(SlaveController.class)
public class SlaveControllerTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    private MockMvc mockMvc;
    @MockBean
    private Slave slave;

    @Test
    public void getDataSucceeds() throws Exception {
        var result = List.of(new DataElement("data", 1));
        when(slave.getData(0)).thenReturn(result);

        var resultJson= mockMvc.perform(get("/slave/data/get_data")).andDo(print()).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(result, objectMapper.readValue(resultJson, new TypeReference<List<DataElement>>() {}));
    }

    @Test
    public void appendDataAcceptsMessage() throws Exception {
        var dataElement = new DataElement("payload", 1);

        mockMvc.perform(post("/slave/replication/append_data").contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(dataElement))).andDo(print()).andExpect(status().isOk());

        verify(slave).appendData(dataElement);
    }
}
