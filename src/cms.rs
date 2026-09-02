use std::{sync::Arc, time::Instant};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::StatusCode;
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
    ) -> Result<SessionData, ValidationError> {
        let start = Instant::now();
        let response = self
            .client
            .post(&tenant.validation_endpoint)
            .header("X-Colombo-API-Key", &tenant.api_key)
            .json(&serde_json::json!({"key": password}))
            .send()
            .await
            .map_err(|e| ValidationError::Unavailable(e.into()))?;
        let status = response.status();
        self.metrics
            .dependency_duration
            .with_label_values(&["cms", "validate", class(status)])
            .observe(start.elapsed().as_secs_f64());
        if status.is_client_error() {
            return Err(ValidationError::Denied);
        }
        if !status.is_success() {
            return Err(ValidationError::Unavailable(anyhow!(
                "CMS validation returned {status}"
            )));
        }
        let parsed: ValidationResponse = response
            .json()
            .await
            .map_err(|e| ValidationError::Unavailable(e.into()))?;
        if parsed.assignment_id.trim().is_empty() || !parsed.upload.valid() {
            return Err(ValidationError::Denied);
        }
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
            .with_label_values(&["cms", "sequence", class(status)])
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
        let response = self
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
            .await?;
        let status = response.status();
        self.metrics
            .dependency_duration
            .with_label_values(&["cms", "callback", class(status)])
            .observe(start.elapsed().as_secs_f64());
        if status.is_success() {
            Ok(CallbackOutcome::Accepted)
        } else if status.is_client_error() {
            Ok(CallbackOutcome::Denied)
        } else {
            bail!("CMS callback returned {status}")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackOutcome {
    Accepted,
    Denied,
}

fn class(status: StatusCode) -> &'static str {
    if status.is_success() {
        "success"
    } else if status.is_client_error() {
        "client_error"
    } else {
        "server_error"
    }
}
