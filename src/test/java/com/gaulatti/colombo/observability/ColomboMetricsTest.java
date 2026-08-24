package com.gaulatti.colombo.observability;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ColomboMetricsTest {

    @Test
    void exposesServiceRuntimeAndBoundedDomainSignalsWithoutSensitiveLabels() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ColomboMetrics metrics = new ColomboMetrics(registry, "build-123");
        Map<String, Object> sessions = new ConcurrentHashMap<>();
        sessions.put("private-user", new Object());
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(2));

        try {
            metrics.registerSessions(sessions);
            metrics.registerExecutor("s3_upload", executor);
            metrics.authentication("ftp", "success");
            metrics.ftpConnection("connect");
            metrics.upload("ftp", "accepted", "queued");
            metrics.dependency(metrics.startDependency(), "cms", "validation_login", "success");
            metrics.retry("credential_refresh", "success");

            assertEquals(1, registry.get("colombo_build_identity").gauge().value());
            assertEquals(1, registry.get("colombo_ftp_sessions_active").gauge().value());
            assertNotNull(registry.get("colombo_upload_queue_depth").tag("queue", "s3_upload").gauge());
            assertNotNull(registry.get("colombo_upload_queue_active_threads").tag("queue", "s3_upload").gauge());
            assertEquals(1, registry.get("colombo_authentication_attempts_total").counter().count());
            assertEquals(1, registry.get("colombo_ftp_connection_events_total").counter().count());
            assertEquals(1, registry.get("colombo_upload_events_total").counter().count());
            assertEquals(1, registry.get("colombo_dependency_request_duration_seconds").timer().count());
            assertEquals(1, registry.get("colombo_retry_attempts_total").counter().count());

            for (Meter meter : registry.getMeters()) {
                assertFalse(meter.getId().getTags().stream()
                        .anyMatch(tag -> tag.getValue().contains("private-user")));
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void noopRegistrySupportsLegacyConstructors() {
        ColomboMetrics.noop().ftpConnection("connect");
    }
}
