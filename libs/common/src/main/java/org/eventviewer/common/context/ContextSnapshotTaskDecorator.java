package org.eventviewer.common.context;

import io.micrometer.context.ContextSnapshot;
import io.micrometer.context.ContextSnapshotFactory;
import org.slf4j.MDC;
import org.springframework.core.task.TaskDecorator;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.Map;

/**
 * Propagates three execution contexts from the submitting thread into every spawned thread:
 *
 * 1. Micrometer observation spans — via ContextSnapshot (handles OpenTelemetry/Brave ThreadLocals)
 * 2. Logback MDC — correlation ID, trace ID, and any other MDC keys set on the submitting thread
 * 3. Spring Security principal — so @PreAuthorize and SecurityContextHolder work inside @Async tasks
 *
 * ContextSnapshot.captureAll() alone does NOT reliably capture MDC or the Security context because
 * those ThreadLocals are not registered with Micrometer's ContextRegistry by default in all
 * Spring Boot configurations. Explicit capture and restore is required for correctness.
 */
public class ContextSnapshotTaskDecorator implements TaskDecorator {

    @Override
    public Runnable decorate(Runnable runnable) {
        ContextSnapshot micrometerSnapshot = ContextSnapshotFactory.builder().build().captureAll();
        Map<String, String> mdcContext = MDC.getCopyOfContextMap();
        SecurityContext securityContext = SecurityContextHolder.getContext();

        return () -> {
            Map<String, String> originalMdc = MDC.getCopyOfContextMap();
            SecurityContext originalSecurityContext = SecurityContextHolder.getContext();

            try (ContextSnapshot.Scope ignored = micrometerSnapshot.setThreadLocals()) {
                if (mdcContext != null) {
                    MDC.setContextMap(mdcContext);
                } else {
                    MDC.clear();
                }
                SecurityContextHolder.setContext(securityContext);

                runnable.run();
            } finally {
                if (originalMdc != null) {
                    MDC.setContextMap(originalMdc);
                } else {
                    MDC.clear();
                }
                SecurityContextHolder.setContext(originalSecurityContext);
            }
        };
    }
}
