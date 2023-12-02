package com.ds.replicationlog.slave;

import com.ds.replicationlog.statemachine.DataRepository;
import com.ds.replicationlog.statemachine.MasterClient;
import com.ds.replicationlog.statemachine.Slave;
import com.ds.replicationlog.statemachine.repository.InMemoryRepo;
import org.springframework.boot.web.embedded.tomcat.TomcatProtocolHandlerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.support.TaskExecutorAdapter;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.UUID;
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

    @Bean(initMethod = "start", destroyMethod = "stop")
    public Slave slave(DataRepository repository, MasterClient masterClient) {
        return new Slave(repository, masterClient, 1_000, UUID.randomUUID().toString());
    }

    @Bean
    public DataRepository repository() {
        return new InMemoryRepo();
    }
}
