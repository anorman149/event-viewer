package org.eventviewer.leader;

import io.micrometer.core.instrument.MeterRegistry;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;

@AutoConfiguration
@EnableConfigurationProperties(RedisLeaderElectionProperties.class)
public class LeaderElectionAutoConfiguration {
    @Bean
    public Config config(RedisLeaderElectionProperties properties) {
        Config config = new Config();

        config.setLockWatchdogTimeout(properties.getLockWatchdogTimeoutMs());

        if(properties.isClustered()) {
            config.useClusterServers()
                    .addNodeAddress(properties.getHost())
                    .setPassword(properties.getPassword())
                    .setScanInterval(2000)
                    .setRetryAttempts(3)
                    .setKeepAlive(true)
                    .setTcpNoDelay(true);
        } else {
            config.useSingleServer().setAddress(properties.getHost());
        }

        return config;
    }

    @Bean
    public RedissonClient redissonClient(Config config) {
        return Redisson.create(config);
    }

    @Bean
    public RedissonLeaderElectionService leaderElectionService(
            RedissonClient redissonClient,
            RedisLeaderElectionProperties properties,
            List<LeaderListener> listeners,
            MeterRegistry meterRegistry) {
        return new RedissonLeaderElectionService(redissonClient, properties, listeners, meterRegistry);
    }

    @Bean
    public LeaderAwareScheduler leaderAwareScheduler(
            LeaderElectionService leaderElectionService,
            MeterRegistry meterRegistry) {
        return new LeaderAwareSchedulerImpl(leaderElectionService, meterRegistry);
    }
}
