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

@WebMvcTest(DataController.class)
public class DataControllerTest {
    private final ObjectMapper objectMapper = new ObjectMapper();
    @Autowired
    private MockMvc mockMvc;

    @Test
    public void appendMessageAcceptsMessage() throws Exception {
        var message = new Message("payload", 1);

        mockMvc.perform(post("/data/append_message").contentType(MediaType.APPLICATION_JSON)
                .content(objectMapper.writeValueAsString(message))).andDo(print()).andExpect(status().isOk());
    }

    @Test
    public void getMessagesSucceeds() throws Exception {
        var resultJson= mockMvc.perform(get("/data/get_messages")).andDo(print()).andExpect(status().isOk())
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
