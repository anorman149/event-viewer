package org.eventviewer.gateway.config;

import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import org.springframework.cloud.gateway.filter.ratelimit.RateLimiter;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory adaptive rate limiter backed by Resilience4J AtomicRateLimiter.
 * One limiter instance per unique client ID (JWT subject or IP address).
 * Production deployments should replace this with a Redis-backed implementation.
 */
@Component("clientRateLimiter")
public class Resilience4JGatewayRateLimiter implements RateLimiter<Resilience4JGatewayRateLimiter.Config> {

    private static final RateLimiterConfig DEFAULT_CONFIG = RateLimiterConfig.custom()
            .limitForPeriod(100)
            .limitRefreshPeriod(Duration.ofSeconds(1))
            .timeoutDuration(Duration.ZERO)
            .build();

    private final ConcurrentHashMap<String, io.github.resilience4j.ratelimiter.RateLimiter> limiters =
            new ConcurrentHashMap<>();

    private final Map<String, Config> configs = Map.of("default", new Config());

    @Override
    public Mono<Response> isAllowed(String routeId, String id) {
        io.github.resilience4j.ratelimiter.RateLimiter rl =
                limiters.computeIfAbsent(id, k ->
                        io.github.resilience4j.ratelimiter.RateLimiter.of(k, DEFAULT_CONFIG));

        boolean permitted = rl.acquirePermission(1);
        Map<String, String> headers = Map.of(
                "X-RateLimit-Remaining",
                String.valueOf(rl.getMetrics().getAvailablePermissions()),
                "X-RateLimit-Limit", "100"
        );
        return Mono.just(new Response(permitted, headers));
    }

    @Override
    public Map<String, Config> getConfig() {
        return configs;
    }

    @Override
    public Class<Config> getConfigClass() {
        return Config.class;
    }

    @Override
    public Config newConfig() {
        return new Config();
    }

    public static class Config {}
}
