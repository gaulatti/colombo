use std::sync::Arc;

use anyhow::Result;
use prometheus::{
    Encoder, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};

#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    pub authentication_attempts: IntCounterVec,
    pub ftp_connection_events: IntCounterVec,
    pub upload_events: IntCounterVec,
    pub dependency_duration: HistogramVec,
    pub retry_attempts: IntCounterVec,
    pub ftp_sessions: IntGauge,
    pub upload_queue_depth: IntGaugeVec,
    pub upload_active: IntGaugeVec,
}

impl Metrics {
    pub fn new(build_version: &str) -> Result<Arc<Self>> {
        let registry = Registry::new();
        let authentication_attempts = IntCounterVec::new(
            Opts::new(
                "colombo_authentication_attempts_total",
                "Authentication attempts",
            ),
            &["source", "result"],
        )?;
        let ftp_connection_events = IntCounterVec::new(
            Opts::new(
                "colombo_ftp_connection_events_total",
                "FTP connection lifecycle events",
            ),
            &["event"],
        )?;
        let upload_events = IntCounterVec::new(
            Opts::new("colombo_upload_events_total", "Upload pipeline events"),
            &["source", "stage", "result"],
        )?;
        let dependency_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "colombo_dependency_request_duration_seconds",
                "Outbound dependency request latency",
            ),
            &["dependency", "operation", "result"],
        )?;
        let retry_attempts = IntCounterVec::new(
            Opts::new("colombo_retry_attempts_total", "Upload retry attempts"),
            &["operation", "result"],
        )?;
        let ftp_sessions = IntGauge::new(
            "colombo_ftp_sessions_active",
            "Active authenticated FTP sessions",
        )?;
        let upload_queue_depth = IntGaugeVec::new(
            Opts::new("colombo_upload_queue_depth", "Queued upload work"),
            &["queue"],
        )?;
        let upload_active = IntGaugeVec::new(
            Opts::new(
                "colombo_upload_queue_active_threads",
                "Active upload workers",
            ),
            &["queue"],
        )?;
        authentication_attempts
            .with_label_values(&["ftp", "success"])
            .inc_by(0);
        ftp_connection_events
            .with_label_values(&["connect"])
            .inc_by(0);
        upload_events
            .with_label_values(&["ftp", "accepted", "queued"])
            .inc_by(0);
        dependency_duration.with_label_values(&["cms", "validation_login", "success"]);
        retry_attempts
            .with_label_values(&["credential_refresh", "success"])
            .inc_by(0);
        upload_queue_depth.with_label_values(&["s3_upload"]).set(0);
        upload_queue_depth
            .with_label_values(&["cms_callback"])
            .set(0);
        upload_active.with_label_values(&["s3_upload"]).set(0);
        upload_active.with_label_values(&["cms_callback"]).set(0);

        registry.register(Box::new(authentication_attempts.clone()))?;
        registry.register(Box::new(ftp_connection_events.clone()))?;
        registry.register(Box::new(upload_events.clone()))?;
        registry.register(Box::new(dependency_duration.clone()))?;
        registry.register(Box::new(retry_attempts.clone()))?;
        registry.register(Box::new(ftp_sessions.clone()))?;
        registry.register(Box::new(upload_queue_depth.clone()))?;
        registry.register(Box::new(upload_active.clone()))?;
        let build = IntGaugeVec::new(
            Opts::new("colombo_build_identity", "Running Colombo build identity"),
            &["service", "version"],
        )?;
        build.with_label_values(&["colombo", build_version]).set(1);
        registry.register(Box::new(build))?;
        Ok(Arc::new(Self {
            registry,
            authentication_attempts,
            ftp_connection_events,
            upload_events,
            dependency_duration,
            retry_attempts,
            ftp_sessions,
            upload_queue_depth,
            upload_active,
        }))
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut output = Vec::new();
        TextEncoder::new().encode(&self.registry.gather(), &mut output)?;
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exposition_has_required_bounded_families_and_build() {
        let metrics = Metrics::new("test-sha").unwrap();
        metrics
            .authentication_attempts
            .with_label_values(&["ftp", "success"])
            .inc();
        metrics.ftp_sessions.set(0);
        metrics
            .upload_queue_depth
            .with_label_values(&["s3_upload"])
            .set(0);
        metrics
            .upload_active
            .with_label_values(&["s3_upload"])
            .set(0);
        metrics
            .ftp_connection_events
            .with_label_values(&["disconnect"])
            .inc();
        metrics
            .upload_events
            .with_label_values(&["ftp", "complete", "success"])
            .inc();
        metrics
            .dependency_duration
            .with_label_values(&["s3", "put_object", "success"])
            .observe(0.1);
        metrics
            .retry_attempts
            .with_label_values(&["s3_put", "success"])
            .inc();
        let text = String::from_utf8(metrics.encode().unwrap()).unwrap();
        for family in [
            "colombo_build_identity",
            "colombo_ftp_sessions_active",
            "colombo_upload_queue_depth",
            "colombo_upload_queue_active_threads",
            "colombo_authentication_attempts_total",
            "colombo_ftp_connection_events_total",
            "colombo_upload_events_total",
            "colombo_dependency_request_duration_seconds",
            "colombo_retry_attempts_total",
        ] {
            assert!(text.contains(family), "missing {family}");
        }
        assert!(text.contains("service=\"colombo\""));
        assert!(text.contains("version=\"test-sha\""));
        assert!(text.contains("source=\"ftp\""));
        assert!(text.contains("operation=\"put_object\""));
        for forbidden in [
            "username",
            "assignment_id",
            "filename",
            "bucket",
            "url",
            "exception",
        ] {
            assert!(
                !text.contains(&format!("{forbidden}=\"")),
                "exposed forbidden label {forbidden}"
            );
        }
    }
}
