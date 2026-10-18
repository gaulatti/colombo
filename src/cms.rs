use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{SessionData, Tenant, ValidationResponse},
    metrics::Metrics,
};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("credentials denied")]
    Denied,
    #[error("CMS unavailable")]
    Unavailable(#[source] anyhow::Error),
}

#[derive(Clone)]
pub struct CmsClient {
    client: reqwest::Client,
    metrics: Arc<Metrics>,
}

impl CmsClient {
    pub fn new(metrics: Arc<Metrics>) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
            metrics,
        })
    }

    pub async fn validate(
        &self,
        tenant: &Tenant,
        password: &str,
        operation: &'static str,
    ) -> Result<SessionData, ValidationError> {
        let start = Instant::now();
        let response = match self
            .client
            .post(&tenant.validation_endpoint)
            .header("X-Colombo-API-Key", &tenant.api_key)
            .json(&serde_json::json!({"key": password}))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.observe("cms", operation, "unavailable", start);
                return Err(ValidationError::Unavailable(error.into()));
            }
        };
        let status = response.status();
        if status.is_client_error() {
            self.observe("cms", operation, "denied", start);
            return Err(ValidationError::Denied);
        }
        if !status.is_success() {
            self.observe("cms", operation, "unavailable", start);
            return Err(ValidationError::Unavailable(anyhow!(
                "CMS validation returned {status}"
            )));
        }
        let parsed: ValidationResponse = match response.json().await {
            Ok(parsed) => parsed,
            Err(error) => {
                self.observe("cms", operation, "unavailable", start);
                return Err(ValidationError::Unavailable(error.into()));
            }
        };
        if parsed.assignment_id.trim().is_empty() || !parsed.upload.valid() {
            self.observe("cms", operation, "denied", start);
            return Err(ValidationError::Denied);
        }
        self.observe("cms", operation, "success", start);
        Ok(SessionData {
            tenant: tenant.clone(),
            assignment_id: parsed.assignment_id,
            upload: Some(parsed.upload),
            validation_key: Some(password.to_owned()),
        })
    }

    pub async fn next_sequence(&self, session: &SessionData) -> Result<u64> {
        let upload = session
            .upload
            .as_ref()
            .context("upload credentials missing")?;
        let endpoint = upload
            .sequence_endpoint
            .as_deref()
            .context("sequence endpoint missing")?;
        let endpoint = url::Url::parse(&session.tenant.validation_endpoint)?.join(endpoint)?;
        let start = Instant::now();
        let result = async {
            let response = self
                .client
                .post(endpoint)
                .header("X-Colombo-API-Key", &session.tenant.api_key)
                .json(&serde_json::json!({"assignment_id": session.assignment_id}))
                .send()
                .await?;
            let status = response.status();
            if !status.is_success() {
                bail!("CMS sequence returned {status}");
            }
            #[derive(Deserialize)]
            struct Body {
                sequence: serde_json::Value,
            }
            let raw = response.json::<Body>().await?.sequence;
            let sequence = raw
                .as_u64()
                .or_else(|| raw.as_str().and_then(|v| v.parse().ok()))
                .context("CMS sequence response is invalid")?;
            if sequence < 1 {
                bail!("CMS sequence response is invalid");
            }
            Ok(sequence)
        }
        .await;
        self.observe(
            "cms",
            "sequence",
            if result.is_ok() { "success" } else { "error" },
            start,
        );
        result
    }

    pub async fn photo_callback(
        &self,
        session: &SessionData,
        s3_url: &str,
        original: &str,
        target: &str,
    ) -> Result<CallbackOutcome> {
        #[derive(Serialize)]
        struct Body<'a> {
            assignment_id: &'a str,
            s3_url: &'a str,
            original_filename: &'a str,
            target_filename: &'a str,
        }
        let start = Instant::now();
        let response = match self
            .client
            .post(&session.tenant.photo_endpoint)
            .header("X-Colombo-API-Key", &session.tenant.api_key)
            .json(&Body {
                assignment_id: &session.assignment_id,
                s3_url,
                original_filename: original,
                target_filename: target,
            })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.observe("cms", "photo_callback", "error", start);
                return Err(error.into());
            }
        };
        let status = response.status();
        if status.is_success() {
            self.observe("cms", "photo_callback", "success", start);
            Ok(CallbackOutcome::Accepted)
        } else if status.is_client_error() {
            self.observe("cms", "photo_callback", "denied", start);
            Ok(CallbackOutcome::Denied)
        } else {
            self.observe("cms", "photo_callback", "error", start);
            bail!("CMS callback returned {status}")
        }
    }

    fn observe(
        &self,
        dependency: &'static str,
        operation: &'static str,
        result: &'static str,
        start: Instant,
    ) {
        self.metrics
            .dependency_duration
            .with_label_values(&[dependency, operation, result])
            .observe(start.elapsed().as_secs_f64());
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackOutcome {
    Accepted,
    Denied,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{Mock, session, validation};

    #[tokio::test]
    async fn validation_enforces_status_payload_and_credentials_and_records_outcomes() {
        let metrics = Metrics::new("cms-test").unwrap();
        let client = CmsClient::new(metrics.clone()).unwrap();
        for (status, body, expected) in [
            (200, validation(&session("http://unused")), "success"),
            (401, "{}".into(), "denied"),
            (503, "{}".into(), "unavailable"),
            (200, "not json".into(), "unavailable"),
            (
                200,
                validation(&{
                    let mut s = session("http://unused");
                    s.assignment_id = " ".into();
                    s
                }),
                "denied",
            ),
            (
                200,
                validation(&{
                    let mut s = session("http://unused");
                    s.upload.as_mut().unwrap().bucket.clear();
                    s
                }),
                "denied",
            ),
        ] {
            let server = Mock::new(vec![(status, body)]).await;
            let tenant = session(&server.url).tenant;
            let result = client
                .validate(&tenant, "fictional-password", "validation_upload")
                .await;
            match expected {
                "success" => {
                    let result = result.unwrap();
                    assert_eq!(result.assignment_id, "assignment-123");
                    assert_eq!(result.validation_key.as_deref(), Some("fictional-password"));
                }
                "denied" => assert!(matches!(result, Err(ValidationError::Denied))),
                _ => assert!(matches!(result, Err(ValidationError::Unavailable(_)))),
            }
            let request = &server.requests()[0];
            assert_eq!(request.uri, "/validate");
            assert_eq!(request.headers["X-Colombo-API-Key"], "fictional-api-key");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                serde_json::json!({"key":"fictional-password"})
            );
        }
        let mut tenant = session("http://unused").tenant;
        tenant.validation_endpoint = "invalid://endpoint".into();
        let result = client
            .validate(&tenant, "key", "validation_upload")
            .await
            .unwrap_err();
        assert!(matches!(result, ValidationError::Unavailable(_)));
        assert_eq!(ValidationError::Denied.to_string(), "credentials denied");
        assert_eq!(result.to_string(), "CMS unavailable");
        let text = String::from_utf8(metrics.encode().unwrap()).unwrap();
        for result in ["success", "denied", "unavailable"] {
            assert!(text.contains(&format!(
                "operation=\"validation_upload\",result=\"{result}\""
            )));
        }
    }

    #[tokio::test]
    async fn cms_redirects_cannot_forward_credentials_or_turn_failures_into_success() {
        use axum::{Router, http::StatusCode, routing::any};

        let metrics = Metrics::new("cms-redirects").unwrap();
        let client = CmsClient::new(metrics.clone()).unwrap();
        for status in [307, 308, 301, 302, 303] {
            let destination = Mock::new(vec![(200, validation(&session("http://unused")))]).await;
            let location = destination.url.clone();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = format!("http://{}", listener.local_addr().unwrap());
            let redirect = Router::new().fallback(any(move || {
                let location = location.clone();
                async move {
                    (
                        StatusCode::from_u16(status).unwrap(),
                        [("location", location)],
                    )
                }
            }));
            let server =
                tokio::spawn(async move { axum::serve(listener, redirect).await.unwrap() });
            let mut session = session(&endpoint);
            session.upload.as_mut().unwrap().sequence_endpoint = Some("/sequence".into());

            let validation = client
                .validate(&session.tenant, "fictional-password", "validation_upload")
                .await;
            let sequence = client.next_sequence(&session).await;
            let callback = client
                .photo_callback(&session, "s3://uploads/key", "original.jpg", "target.jpg")
                .await;

            assert!(
                destination.requests().is_empty(),
                "CMS redirect {status} sent credential-bearing requests to a different endpoint"
            );
            assert!(matches!(validation, Err(ValidationError::Unavailable(_))));
            assert!(sequence.is_err());
            assert!(callback.is_err());
            server.abort();
        }
        let text = String::from_utf8(metrics.encode().unwrap()).unwrap();
        for (operation, result) in [
            ("validation_upload", "unavailable"),
            ("sequence", "error"),
            ("photo_callback", "error"),
        ] {
            assert!(text.lines().any(|line| line == format!(
                "colombo_dependency_request_duration_seconds_count{{dependency=\"cms\",operation=\"{operation}\",result=\"{result}\"}} 5"
            )));
            assert_eq!(
                metrics
                    .dependency_duration
                    .with_label_values(&["cms", operation, "success"])
                    .get_sample_count(),
                0
            );
        }
    }

    #[tokio::test]
    async fn sequence_requires_context_positive_number_and_successful_reply() {
        for (status, body, expected) in [
            (200, r#"{"sequence":7}"#, Some(7)),
            (200, r#"{"sequence":"9"}"#, Some(9)),
            (200, r#"{"sequence":0}"#, None),
            (200, r#"{"sequence":"bad"}"#, None),
            (200, r#"{"sequence":{}}"#, None),
            (200, "not json", None),
            (403, "{}", None),
        ] {
            let metrics = Metrics::new("sequence-reply").unwrap();
            let client = CmsClient::new(metrics.clone()).unwrap();
            let server = Mock::new(vec![(status, body.into())]).await;
            let mut s = session(&server.url);
            s.upload.as_mut().unwrap().sequence_endpoint = Some("/sequence".into());
            let result = client.next_sequence(&s).await;
            if let Some(n) = expected {
                assert_eq!(result.unwrap(), n);
            } else {
                assert!(result.is_err());
            }
            for (outcome, count) in [
                ("success", u64::from(expected.is_some())),
                ("error", u64::from(expected.is_none())),
            ] {
                assert_eq!(
                    metrics
                        .dependency_duration
                        .with_label_values(&["cms", "sequence", outcome])
                        .get_sample_count(),
                    count,
                    "status={status}, body={body}, outcome={outcome}"
                );
            }
            let request = &server.requests()[0];
            assert_eq!(request.uri, "/sequence");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                serde_json::json!({"assignment_id":"assignment-123"})
            );
        }
        let client = CmsClient::new(Metrics::new("test").unwrap()).unwrap();
        let mut s = session("invalid://url");
        assert!(client.next_sequence(&s).await.is_err());
        s.upload.as_mut().unwrap().sequence_endpoint = Some("http://[".into());
        assert!(client.next_sequence(&s).await.is_err());
        s.tenant.validation_endpoint = "http://localhost".into();
        assert!(client.next_sequence(&s).await.is_err());
        s.upload.as_mut().unwrap().sequence_endpoint = Some("invalid://endpoint".into());
        assert!(client.next_sequence(&s).await.is_err());
        s.upload = None;
        assert!(client.next_sequence(&s).await.is_err());
    }

    #[tokio::test]
    async fn sequence_body_disconnects_are_counted_as_errors_not_successes() {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut content_length = 0;
            loop {
                let mut line = String::new();
                assert!(stream.read_line(&mut line).await.unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    content_length = value.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; content_length];
            stream.read_exact(&mut body).await.unwrap();
            stream.get_mut().write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{\"sequence\":"
            ).await.unwrap();
            stream.get_mut().shutdown().await.unwrap();
        });
        let metrics = Metrics::new("sequence-body").unwrap();
        let client = CmsClient::new(metrics.clone()).unwrap();
        let mut session = session(&endpoint);
        session.upload.as_mut().unwrap().sequence_endpoint = Some("/sequence".into());
        assert!(client.next_sequence(&session).await.is_err());
        server.await.unwrap();
        assert_eq!(
            metrics
                .dependency_duration
                .with_label_values(&["cms", "sequence", "error"])
                .get_sample_count(),
            1
        );
        assert_eq!(
            metrics
                .dependency_duration
                .with_label_values(&["cms", "sequence", "success"])
                .get_sample_count(),
            0
        );
        let text = String::from_utf8(metrics.encode().unwrap()).unwrap();
        assert!(text.lines().any(|line| line == "colombo_dependency_request_duration_seconds_count{dependency=\"cms\",operation=\"sequence\",result=\"error\"} 1"));
    }

    #[tokio::test]
    async fn sequence_transport_failures_are_counted_in_dependency_metrics() {
        let metrics = Metrics::new("sequence-transport").unwrap();
        let client = CmsClient::new(metrics.clone()).unwrap();
        let mut session = session("http://127.0.0.1:0");
        session.upload.as_mut().unwrap().sequence_endpoint = Some("/sequence".into());
        assert!(client.next_sequence(&session).await.is_err());
        assert_eq!(
            metrics
                .dependency_duration
                .with_label_values(&["cms", "sequence", "error"])
                .get_sample_count(),
            1
        );
    }

    #[tokio::test]
    async fn callback_preserves_payload_and_distinguishes_denial_from_retryable_failure() {
        let metrics = Metrics::new("test").unwrap();
        let client = CmsClient::new(metrics.clone()).unwrap();
        for (status, outcome) in [
            (204, Some(CallbackOutcome::Accepted)),
            (403, Some(CallbackOutcome::Denied)),
            (503, None),
        ] {
            let server = Mock::new(vec![(status, String::new())]).await;
            let result = client
                .photo_callback(
                    &session(&server.url),
                    "s3://uploads/key",
                    "original.jpg",
                    "target.jpg",
                )
                .await;
            if let Some(outcome) = outcome {
                assert_eq!(result.unwrap(), outcome);
            } else {
                assert!(result.is_err());
            }
            let request = &server.requests()[0];
            assert_eq!(request.uri, "/photo");
            assert_eq!(request.headers["X-Colombo-API-Key"], "fictional-api-key");
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                serde_json::json!({"assignment_id":"assignment-123","s3_url":"s3://uploads/key","original_filename":"original.jpg","target_filename":"target.jpg"})
            );
        }
        let mut s = session("http://unused");
        s.tenant.photo_endpoint = "invalid://endpoint".into();
        assert!(
            client
                .photo_callback(&s, "s3://uploads/key", "original", "target")
                .await
                .is_err()
        );
        let text = String::from_utf8(metrics.encode().unwrap()).unwrap();
        for result in ["success", "denied", "error"] {
            assert!(text.contains(&format!("operation=\"photo_callback\",result=\"{result}\"")));
        }
    }
}
