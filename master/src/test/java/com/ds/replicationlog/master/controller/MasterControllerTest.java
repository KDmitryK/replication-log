package com.ds.replicationlog.master.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(MasterController.class)
public class MasterControllerTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void appendDataAcceptsMessage() throws Exception {
        var appendRequest = new AppendDataRequest("payload", 1);

        mockMvc.perform(post("/master/data/append_data").contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(appendRequest))).andDo(print()).andExpect(status().isOk());
    }

    @Test
    public void getDataSucceeds() throws Exception {
        var resultJson= mockMvc.perform(get("/master/data/get_data")).andDo(print()).andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString();

        assertEquals(List.of(), objectMapper.readValue(resultJson, List.class));
    }

    /*
    @MockBean
	private GreetingService service;

	@Test
	void greetingShouldReturnMessageFromService() throws Exception {
		when(service.greet()).thenReturn("Hello, Mock");
		this.mockMvc.perform(get("/greeting")).andDo(print()).andExpect(status().isOk())
				.andExpect(content().string(containsString("Hello, Mock")));
	}
     */
}
