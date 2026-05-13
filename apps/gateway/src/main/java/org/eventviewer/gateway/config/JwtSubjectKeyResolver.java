package org.eventviewer.gateway.config;

import org.springframework.cloud.gateway.filter.ratelimit.KeyResolver;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.InetSocketAddress;

@Component("jwtSubjectKeyResolver")
public class JwtSubjectKeyResolver implements KeyResolver {

    @Override
    public Mono<String> resolve(ServerWebExchange exchange) {
        return exchange.getPrincipal()
                .filter(p -> p instanceof JwtAuthenticationToken)
                .cast(JwtAuthenticationToken.class)
                .map(token -> token.getToken().getSubject())
                .switchIfEmpty(Mono.fromSupplier(() -> {
                    InetSocketAddress addr = exchange.getRequest().getRemoteAddress();
                    return addr != null ? addr.getAddress().getHostAddress() : "unknown";
                }));
    }
}
