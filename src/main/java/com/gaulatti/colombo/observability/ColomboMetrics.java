package com.gaulatti.colombo.observability;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** Bounded application metrics shared by the FTP, upload, and dependency paths. */
@Component
public class ColomboMetrics {

    private final MeterRegistry registry;

    public ColomboMetrics(
            MeterRegistry registry,
            @Value("${colombo.build.version:development}") String buildVersion
    ) {
        this.registry = registry;
        Gauge.builder("colombo_build_identity", () -> 1)
                .description("Colombo service and build identity")
                .tags("service", "colombo", "version", buildVersion)
                .strongReference(true)
                .register(registry);
        Counter.builder("colombo_authentication_attempts_total")
                .description("FTP and HTTP upload authentication outcomes")
                .tags("source", "ftp", "result", "success")
                .register(registry);
        Counter.builder("colombo_ftp_connection_events_total")
                .description("FTP connection lifecycle transitions")
                .tag("event", "connect")
                .register(registry);
        Counter.builder("colombo_upload_events_total")
                .description("Upload lifecycle outcomes")
                .tags("source", "ftp", "stage", "accepted", "result", "queued")
                .register(registry);
        Timer.builder("colombo_dependency_request_duration_seconds")
                .description("Outbound dependency request duration and outcome")
                .tags("dependency", "cms", "operation", "validation_login", "result", "success")
                .register(registry);
        Counter.builder("colombo_retry_attempts_total")
                .description("Credential refresh and S3 retry outcomes")
                .tags("operation", "credential_refresh", "result", "success")
                .register(registry);
    }

    public static ColomboMetrics noop() {
        return new ColomboMetrics(new SimpleMeterRegistry(), "test");
    }

    public void registerSessions(Map<String, ?> sessions) {
        Gauge.builder("colombo_ftp_sessions_active", sessions, Map::size)
                .description("Active authenticated FTP sessions")
                .register(registry);
    }

    public void registerExecutor(String queue, ThreadPoolExecutor executor) {
        Tags tags = Tags.of("queue", queue);
        Gauge.builder("colombo_upload_queue_depth", executor, value -> value.getQueue().size())
                .description("Queued asynchronous upload tasks")
                .tags(tags)
                .strongReference(true)
                .register(registry);
        Gauge.builder("colombo_upload_queue_active_threads", executor, ThreadPoolExecutor::getActiveCount)
                .description("Threads actively processing asynchronous upload tasks")
                .tags(tags)
                .strongReference(true)
                .register(registry);
    }

    public void authentication(String source, String result) {
        Counter.builder("colombo_authentication_attempts_total")
                .description("FTP and HTTP upload authentication outcomes")
                .tags("source", source, "result", result)
                .register(registry)
                .increment();
    }

    public void ftpConnection(String event) {
        Counter.builder("colombo_ftp_connection_events_total")
                .description("FTP connection lifecycle transitions")
                .tag("event", event)
                .register(registry)
                .increment();
    }

    public void upload(String source, String stage, String result) {
        Counter.builder("colombo_upload_events_total")
                .description("Upload lifecycle outcomes")
                .tags("source", source, "stage", stage, "result", result)
                .register(registry)
                .increment();
    }

    public Timer.Sample startDependency() {
        return Timer.start(registry);
    }

    public void dependency(
            Timer.Sample sample,
            String dependency,
            String operation,
            String result
    ) {
        sample.stop(Timer.builder("colombo_dependency_request_duration_seconds")
                .description("Outbound dependency request duration and outcome")
                .tags("dependency", dependency, "operation", operation, "result", result)
                .register(registry));
    }

    public void retry(String operation, String result) {
        Counter.builder("colombo_retry_attempts_total")
                .description("Credential refresh and S3 retry outcomes")
                .tags("operation", operation, "result", result)
                .register(registry)
                .increment();
    }
}
