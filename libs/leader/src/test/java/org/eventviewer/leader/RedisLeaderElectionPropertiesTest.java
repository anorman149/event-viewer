package org.eventviewer.leader;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest(classes = RedisLeaderElectionPropertiesTest.TestConfig.class)
@EnableConfigurationProperties(RedisLeaderElectionProperties.class)
@TestPropertySource(properties = {
        "leader-election.lock-name=test:lock",
        "leader-election.retry-interval-ms=1000",
        "leader-election.lock-watchdog-timeout-ms=10000"
})
class RedisLeaderElectionPropertiesTest {

    @Configuration
    static class TestConfig {}

    @Autowired
    RedisLeaderElectionProperties properties;

    @Test
    void bindsAllFields() {
        assertThat(properties.getLockName()).isEqualTo("test:lock");
        assertThat(properties.getRetryIntervalMs()).isEqualTo(1000L);
        assertThat(properties.getLockWatchdogTimeoutMs()).isEqualTo(10000L);
    }

    @Test
    void defaultLockName() {
        RedisLeaderElectionProperties props = new RedisLeaderElectionProperties();
        assertThat(props.getLockName()).isEqualTo("leader:event-ingest");
    }

    @Test
    void defaultRetryIntervalMs() {
        RedisLeaderElectionProperties props = new RedisLeaderElectionProperties();
        assertThat(props.getRetryIntervalMs()).isEqualTo(2000L);
    }

    @Test
    void defaultLockWatchdogTimeoutMs() {
        RedisLeaderElectionProperties props = new RedisLeaderElectionProperties();
        assertThat(props.getLockWatchdogTimeoutMs()).isEqualTo(30000L);
    }

    @Test
    void crossFieldValidationPassesWhenWatchdogExceedsRetry() {
        RedisLeaderElectionProperties props = new RedisLeaderElectionProperties();
        props.setRetryIntervalMs(2000L);
        props.setLockWatchdogTimeoutMs(30000L);
        assertThat(props.isWatchdogTimeoutGreaterThanRetryInterval()).isTrue();
    }

    @Test
    void crossFieldValidationFailsWhenWatchdogEqualsRetry() {
        RedisLeaderElectionProperties props = new RedisLeaderElectionProperties();
        props.setRetryIntervalMs(5000L);
        props.setLockWatchdogTimeoutMs(5000L);
        assertThat(props.isWatchdogTimeoutGreaterThanRetryInterval()).isFalse();
    }
}
