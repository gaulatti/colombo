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
        let response = self
            .client
            .post(endpoint)
            .header("X-Colombo-API-Key", &session.tenant.api_key)
            .json(&serde_json::json!({"assignment_id": session.assignment_id}))
            .send()
            .await?;
        let status = response.status();
        self.metrics
            .dependency_duration
            .with_label_values(&[
                "cms",
                "sequence",
                if status.is_success() {
                    "success"
                } else {
                    "error"
                },
            ])
            .observe(start.elapsed().as_secs_f64());
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
