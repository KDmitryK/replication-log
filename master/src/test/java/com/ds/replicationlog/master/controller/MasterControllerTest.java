package com.ds.replicationlog.master.controller;

import com.ds.replicationlog.statemachine.Acknowledgement;
import com.ds.replicationlog.statemachine.DataElement;
import com.ds.replicationlog.statemachine.Master;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MasterController.class)
public class MasterControllerTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    private MockMvc mockMvc;
    @MockBean
    private Master master;

    @Test
    public void appendDataAcceptsMessage() throws Exception {
        var appendRequest = new AppendDataRequest("payload", 1);

        mockMvc.perform(post("/master/data/append_data").contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(appendRequest))).andDo(print()).andExpect(status().isOk());

        verify(master).appendData(1, "payload");
    }

    @Test
    public void appendDataIfTimedOutReturnsTimeoutStatusCode() throws Exception {
        var appendRequest = new AppendDataRequest("payload", 1);
        doThrow(new TimeoutException("test")).when(master).appendData(1, "payload");

        mockMvc.perform(post("/master/data/append_data").contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(appendRequest))).andDo(print()).andExpect(status()
                .isRequestTimeout());
    }

    @Test
    public void appendDataIfInterruptedReturnsInternalErrorStatusCode() throws Exception {
        var appendRequest = new AppendDataRequest("payload", 1);
        doThrow(new InterruptedException("test")).when(master).appendData(1, "payload");

        mockMvc.perform(post("/master/data/append_data").contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(appendRequest))).andDo(print()).andExpect(status()
                .isInternalServerError());
    }

    @Test
    public void getDataSucceeds() throws Exception {
        var result = List.of(new DataElement("data", 1));
        when(master.getData(0)).thenReturn(result);

        var resultJson= mockMvc.perform(get("/master/data/get_data")).andDo(print()).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(result, objectMapper.readValue(resultJson, new TypeReference<List<DataElement>>() {}));
    }

    @Test
    public void getDataFromSeqNumSucceeds() throws Exception {
        var result = List.of(new DataElement("data", 1));
        when(master.getData(1)).thenReturn(result);

        var resultJson= mockMvc.perform(get("/master/data/get_data/1")).andDo(print()).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(result, objectMapper.readValue(resultJson, new TypeReference<List<DataElement>>() {}));
    }

    @Test
    public void acknowledgeReceptionSucceeds() throws Exception {
        var acknowledgement = new Acknowledgement("r1", 1);

        mockMvc.perform(post("/master/replication/acknowledge_reception").contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(acknowledgement))).andDo(print()).andExpect(status().isOk());

        verify(master).acknowledgeReception(acknowledgement);
    }
}
