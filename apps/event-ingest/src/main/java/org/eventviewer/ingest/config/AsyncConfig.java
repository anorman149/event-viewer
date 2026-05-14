package org.eventviewer.ingest.config;

import org.eventviewer.common.context.ContextSnapshotTaskDecorator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
@EnableScheduling
public class AsyncConfig {

    @Bean
    public ThreadPoolTaskExecutor asyncTaskExecutor(ContextSnapshotTaskDecorator contextSnapshotTaskDecorator) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setVirtualThreads(true);
        executor.setTaskDecorator(contextSnapshotTaskDecorator);
        executor.setThreadNamePrefix("async-");
        executor.initialize();
        return executor;
    }
}
