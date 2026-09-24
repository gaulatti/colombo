use std::{sync::Arc, time::Instant};

use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{SessionData, Tenant, UploadCredentials, ValidationResponse},
    metrics::Metrics,
    spool::{UploadRecord, WandererReason},
};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("credentials denied")]
    Denied,
    #[error("CMS unavailable")]
    Unavailable(#[source] anyhow::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum DeliveryError {
    #[error("CMS denied request")]
    Denied(WandererReason),
    #[error("CMS unavailable")]
    Unavailable(#[source] anyhow::Error),
    #[error("CMS response or endpoint invalid")]
    Invalid(#[source] anyhow::Error),
}

#[derive(Deserialize)]
struct ReasonBody {
    reason: Option<WandererReason>,
}

async fn classify(response: reqwest::Response) -> Result<reqwest::Response, DeliveryError> {
    if response.status().is_client_error() {
        let reason = response
            .json::<ReasonBody>()
            .await
            .ok()
            .and_then(|b| b.reason)
            .unwrap_or(WandererReason::CallbackRejected);
        Err(DeliveryError::Denied(reason))
    } else if !response.status().is_success() {
        Err(DeliveryError::Unavailable(anyhow!(
            "CMS returned server error"
        )))
    } else {
        Ok(response)
    }
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
            device_id: parsed.device_id,
            upload: Some(parsed.upload),
            validation_key: Some(password.to_owned()),
        })
    }

    pub async fn refresh_credentials(
        &self,
        session: &SessionData,
        accepted_at: DateTime<Utc>,
    ) -> Result<UploadCredentials, DeliveryError> {
        let upload = session
            .upload
            .as_ref()
            .expect("upload credentials available");
        let endpoint = upload
            .credentials_endpoint
            .as_deref()
            .expect("credentials endpoint advertised");
        let endpoint = url::Url::parse(&session.tenant.validation_endpoint)
            .and_then(|base| base.join(endpoint))
            .map_err(|e| DeliveryError::Invalid(e.into()))?;
        let start = Instant::now();
        let response = self.client.post(endpoint)
            .header("X-Colombo-API-Key", &session.tenant.api_key)
            .json(&serde_json::json!({"assignment_id": session.assignment_id, "accepted_at": accepted_at.to_rfc3339()}))
            .send().await.map_err(|e| {
                self.observe("cms", "credentials", "unavailable", start);
                DeliveryError::Unavailable(e.into())
            })?;
        let status = response.status();
        self.observe(
            "cms",
            "credentials",
            if status.is_success() {
                "success"
            } else if status.is_client_error() {
                "denied"
            } else {
                "unavailable"
            },
            start,
        );
        let response = classify(response).await?;
        #[derive(Deserialize)]
        struct Body {
            upload: UploadCredentials,
        }
        let upload = response
            .json::<Body>()
            .await
            .map_err(|e| DeliveryError::Invalid(e.into()))?
            .upload;
        if !upload.valid() {
            return Err(DeliveryError::Invalid(anyhow!(
                "invalid CMS upload credentials"
            )));
        }
        Ok(upload)
    }

    pub async fn next_sequence(
        &self,
        session: &SessionData,
        accepted_at: DateTime<Utc>,
    ) -> Result<u64, DeliveryError> {
        let upload = session
            .upload
            .as_ref()
            .ok_or_else(|| DeliveryError::Invalid(anyhow!("upload credentials missing")))?;
        let endpoint = upload
            .sequence_endpoint
            .as_deref()
            .ok_or_else(|| DeliveryError::Invalid(anyhow!("sequence endpoint missing")))?;
        let endpoint = url::Url::parse(&session.tenant.validation_endpoint)
            .and_then(|base| base.join(endpoint))
            .map_err(|e| DeliveryError::Invalid(e.into()))?;
        let start = Instant::now();
        let response = self
            .client
            .post(endpoint)
            .header("X-Colombo-API-Key", &session.tenant.api_key)
            .json(&serde_json::json!({"assignment_id": session.assignment_id, "accepted_at": accepted_at.to_rfc3339()}))
            .send()
            .await.map_err(|e| DeliveryError::Unavailable(e.into()))?;
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
        let response = classify(response).await?;
        #[derive(Deserialize)]
        struct Body {
            sequence: serde_json::Value,
        }
        let raw = response
            .json::<Body>()
            .await
            .map_err(|e| DeliveryError::Invalid(e.into()))?
            .sequence;
        let sequence = raw
            .as_u64()
            .or_else(|| raw.as_str().and_then(|v| v.parse().ok()))
            .ok_or_else(|| DeliveryError::Invalid(anyhow!("CMS sequence response is invalid")))?;
        if sequence < 1 {
            return Err(DeliveryError::Invalid(anyhow!(
                "CMS sequence response is invalid"
            )));
        }
        Ok(sequence)
    }

    pub async fn photo_callback(
        &self,
        session: &SessionData,
        s3_url: &str,
        original: &str,
        target: &str,
        accepted_at: DateTime<Utc>,
        device_id: Option<&str>,
    ) -> Result<CallbackOutcome, DeliveryError> {
        #[derive(Serialize)]
        struct Body<'a> {
            assignment_id: &'a str,
            s3_url: &'a str,
            original_filename: &'a str,
            target_filename: &'a str,
            accepted_at: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            device_id: Option<&'a str>,
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
                accepted_at: accepted_at.to_rfc3339(),
                device_id,
            })
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.observe("cms", "photo_callback", "error", start);
                return Err(DeliveryError::Unavailable(error.into()));
            }
        };
        let status = response.status();
        if status.is_success() {
            self.observe("cms", "photo_callback", "success", start);
            Ok(CallbackOutcome::Accepted)
        } else {
            self.observe(
                "cms",
                "photo_callback",
                if status.is_client_error() {
                    "denied"
                } else {
                    "error"
                },
                start,
            );
            classify(response).await?;
            unreachable!()
        }
    }

    pub async fn register_wanderer(
        &self,
        session: &SessionData,
        record: &UploadRecord,
    ) -> Result<WandererRegistration, DeliveryError> {
        #[derive(Serialize)]
        struct Body<'a> {
            operation_id: uuid::Uuid,
            assignment_id: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            device_id: Option<&'a str>,
            accepted_at: String,
            reason: WandererReason,
            original_filename: &'a str,
            content_length: u64,
            checksum_sha256: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            s3_url: Option<&'a str>,
        }
        let endpoint = session
            .upload
            .as_ref()
            .and_then(|u| u.wanderers_endpoint.as_deref())
            .expect("wanderer endpoint advertised");
        let endpoint = url::Url::parse(&session.tenant.validation_endpoint)
            .and_then(|base| base.join(endpoint))
            .map_err(|e| DeliveryError::Invalid(e.into()))?;
        let start = Instant::now();
        let response = self
            .client
            .post(endpoint)
            .header("X-Colombo-API-Key", &session.tenant.api_key)
            .json(&Body {
                operation_id: record.operation_id,
                assignment_id: &record.assignment_id,
                device_id: record.device_id.as_deref(),
                accepted_at: record.accepted_at.to_rfc3339(),
                reason: record.wanderer_reason.expect("wanderer reason persisted"),
                original_filename: &record.original_filename,
                content_length: record.content_length,
                checksum_sha256: &record.checksum_sha256,
                s3_url: record.s3_url.as_deref(),
            })
            .send()
            .await
            .map_err(|e| {
                self.observe("cms", "wanderer_register", "unavailable", start);
                DeliveryError::Unavailable(e.into())
            })?;
        let status = response.status();
        self.observe(
            "cms",
            "wanderer_register",
            if status.is_success() {
                "success"
            } else if status.is_client_error() {
                "denied"
            } else {
                "unavailable"
            },
            start,
        );
        let response = classify(response).await?;
        response
            .json()
            .await
            .map_err(|e| DeliveryError::Invalid(e.into()))
    }

    pub async fn wanderer_delivered(
        &self,
        session: &SessionData,
        record: &UploadRecord,
        s3_url: &str,
    ) -> Result<(), DeliveryError> {
        let endpoint = session
            .upload
            .as_ref()
            .and_then(|u| u.wanderers_endpoint.as_deref())
            .expect("wanderer endpoint advertised");
        let base = url::Url::parse(&session.tenant.validation_endpoint)
            .and_then(|base| base.join(endpoint))
            .map_err(|e| DeliveryError::Invalid(e.into()))?;
        let endpoint = url::Url::parse(&format!(
            "{}/{}/delivered",
            base.as_str().trim_end_matches('/'),
            record.operation_id
        ))
        .map_err(|e| DeliveryError::Invalid(e.into()))?;
        let start = Instant::now();
        let response = self
            .client
            .post(endpoint)
            .header("X-Colombo-API-Key", &session.tenant.api_key)
            .json(&serde_json::json!({"s3_url": s3_url}))
            .send()
            .await
            .map_err(|e| {
                self.observe("cms", "wanderer_delivered", "unavailable", start);
                DeliveryError::Unavailable(e.into())
            })?;
        let status = response.status();
        self.observe(
            "cms",
            "wanderer_delivered",
            if status.is_success() {
                "success"
            } else if status.is_client_error() {
                "denied"
            } else {
                "unavailable"
            },
            start,
        );
        classify(response).await?;
        Ok(())
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

#[derive(Deserialize)]
pub struct WandererRegistration {
    pub status: String,
    pub upload: Option<UploadCredentials>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallbackOutcome {
    Accepted,
}
