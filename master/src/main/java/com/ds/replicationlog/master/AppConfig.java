package com.ds.replicationlog.master;

import com.ds.replicationlog.statemachine.DataRepository;
import com.ds.replicationlog.statemachine.Master;
import com.ds.replicationlog.statemachine.SlavesClient;
import com.ds.replicationlog.statemachine.repository.InMemoryRepo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatProtocolHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.scheduling.annotation.EnableAsync;

import java.time.Duration;
import java.util.concurrent.Executors;

@SuppressWarnings("unused")
@EnableAsync
@Configuration
public class AppConfig {
    @Bean
    public AsyncTaskExecutor applicationTaskExecutor() {
        return new TaskExecutorAdapter(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public TomcatProtocolHandlerCustomizer<?> protocolHandlerVirtualThreadExecutorCustomizer() {
        return protocolHandler -> protocolHandler.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
    }

    @Bean
    public Master master(DataRepository repository, SlavesClient slavesClient,
                         @Value("${minAcknowledgmentsWaitTimeSeconds}") int minAcknowledgmentsWaitTimeSeconds) {
        return new Master(repository, Duration.ofSeconds(minAcknowledgmentsWaitTimeSeconds), slavesClient);
    }

    @Bean
    public DataRepository repository() {
        return new InMemoryRepo();
    }
}
