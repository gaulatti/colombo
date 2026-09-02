use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, Result, anyhow, bail};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, error::ProvideErrorMetadata, primitives::ByteStream};
use chrono::Utc;
use tokio::sync::{RwLock, Semaphore};
use tracing::{error, info};

use crate::{
    cms::{CallbackOutcome, CmsClient, ValidationError},
    domain::SessionData,
    metrics::Metrics,
    naming,
};

#[derive(Debug)]
pub struct SessionHandle {
    data: RwLock<SessionData>,
    valid: AtomicBool,
}

impl SessionHandle {
    pub fn new(data: SessionData) -> Arc<Self> {
        Arc::new(Self {
            data: RwLock::new(data),
            valid: AtomicBool::new(true),
        })
    }
    pub fn invalidate(&self) {
        self.valid.store(false, Ordering::Release);
    }
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }
    pub async fn snapshot(&self) -> SessionData {
        self.data.read().await.clone()
    }
    async fn replace(&self, value: SessionData) {
        *self.data.write().await = value;
        self.valid.store(true, Ordering::Release);
    }
}

#[derive(Clone)]
pub struct UploadService {
    cms: CmsClient,
    metrics: Arc<Metrics>,
    s3_slots: Arc<Semaphore>,
    callback_slots: Arc<Semaphore>,
}

impl std::fmt::Debug for UploadService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadService").finish_non_exhaustive()
    }
}

impl UploadService {
    pub fn new(cms: CmsClient, metrics: Arc<Metrics>) -> Arc<Self> {
        Arc::new(Self {
            cms,
            metrics,
            s3_slots: Arc::new(Semaphore::new(8)),
            callback_slots: Arc::new(Semaphore::new(4)),
        })
    }

    pub fn queue_ftp(
        self: &Arc<Self>,
        session: Arc<SessionHandle>,
        original: String,
        path: PathBuf,
    ) {
        let service = Arc::clone(self);
        self.metrics
            .upload_queue_depth
            .with_label_values(&["s3"])
            .inc();
        tokio::spawn(async move {
            service
                .metrics
                .upload_queue_depth
                .with_label_values(&["s3"])
                .dec();
            if let Err(err) = service.process_ftp(session, original, path).await {
                error!(error = %err, "FTP background upload failed");
            }
        });
    }

    pub fn queue_http(self: &Arc<Self>, session: SessionData, original: String, path: PathBuf) {
        let service = Arc::clone(self);
        self.metrics
            .upload_queue_depth
            .with_label_values(&["s3"])
            .inc();
        tokio::spawn(async move {
            service
                .metrics
                .upload_queue_depth
                .with_label_values(&["s3"])
                .dec();
            let result = service
                .process_once("http", &session, &original, &path)
                .await;
            let _ = tokio::fs::remove_file(&path).await;
            match result {
                Ok(upload) => {
                    if let Err(err) = service
                        .callback("http", &session, &original, &upload, None)
                        .await
                    {
                        error!(error = %err, "HTTP callback failed");
                    }
                }
                Err(err) => error!(error = %err, "HTTP background upload failed"),
            }
        });
    }

    async fn process_ftp(
        &self,
        handle: Arc<SessionHandle>,
        original: String,
        path: PathBuf,
    ) -> Result<()> {
        if !handle.is_valid() {
            bail!("FTP session was evicted");
        }
        let session = handle.snapshot().await;
        let upload = match self.process_once("ftp", &session, &original, &path).await {
            Ok(result) => result,
            Err(UploadFailure::Expired) => {
                self.metrics
                    .retry_attempts
                    .with_label_values(&["expired_credentials", "attempted"])
                    .inc();
                let key = session
                    .validation_key
                    .as_deref()
                    .context("session has no validation key")?;
                match self.cms.validate(&session.tenant, key).await {
                    Ok(fresh) => {
                        handle.replace(fresh.clone()).await;
                        match self.process_once("ftp", &fresh, &original, &path).await {
                            Ok(result) => {
                                self.metrics
                                    .retry_attempts
                                    .with_label_values(&["expired_credentials", "success"])
                                    .inc();
                                result
                            }
                            Err(err) => {
                                if matches!(err, UploadFailure::Expired | UploadFailure::Denied) {
                                    handle.invalidate();
                                }
                                return Err(err.into());
                            }
                        }
                    }
                    Err(ValidationError::Denied) => {
                        handle.invalidate();
                        bail!("CMS denied credential refresh");
                    }
                    Err(err) => {
                        handle.invalidate();
                        return Err(err.into());
                    }
                }
            }
            Err(UploadFailure::Denied) => {
                handle.invalidate();
                bail!("S3 denied upload");
            }
            Err(err) => return Err(err.into()),
        };
        let active = handle.snapshot().await;
        self.callback("ftp", &active, &original, &upload, Some(&handle))
            .await
    }

    async fn process_once(
        &self,
        protocol: &'static str,
        session: &SessionData,
        original: &str,
        path: &Path,
    ) -> std::result::Result<Uploaded, UploadFailure> {
        let credentials = session
            .upload
            .as_ref()
            .ok_or_else(|| UploadFailure::Other(anyhow!("upload credentials missing")))?;
        if !credentials.valid() {
            return Err(UploadFailure::Other(anyhow!("upload credentials invalid")));
        }
        let target = if let Some(policy) = &credentials.naming_policy {
            if !policy.valid() {
                return Err(UploadFailure::Other(anyhow!(
                    "Invalid upload naming policy"
                )));
            }
            let sequence = self
                .cms
                .next_sequence(session)
                .await
                .map_err(UploadFailure::Other)?;
            naming::render(
                policy,
                &session.assignment_id,
                original,
                naming::capture_time(path),
                Utc::now(),
                sequence,
            )
            .map_err(UploadFailure::Other)?
        } else {
            original.to_owned()
        };
        let key = if credentials.key_prefix.ends_with('/') {
            format!("{}{}", credentials.key_prefix, target)
        } else {
            format!("{}/{}", credentials.key_prefix, target)
        };
        let _permit = self
            .s3_slots
            .acquire()
            .await
            .map_err(|e| UploadFailure::Other(e.into()))?;
        self.metrics.upload_active.with_label_values(&["s3"]).inc();
        self.metrics
            .upload_events
            .with_label_values(&[protocol, "s3", "started"])
            .inc();
        let started = Instant::now();
        let result = put_s3(credentials, original, path, &key).await;
        self.metrics.upload_active.with_label_values(&["s3"]).dec();
        let label = if result.is_ok() { "success" } else { "failure" };
        self.metrics
            .dependency_duration
            .with_label_values(&["s3", "put", label])
            .observe(started.elapsed().as_secs_f64());
        self.metrics
            .upload_events
            .with_label_values(&[protocol, "s3", label])
            .inc();
        result?;
        info!(assignment_id = %session.assignment_id, "S3 upload complete");
        Ok(Uploaded {
            s3_url: format!("s3://{}/{}", credentials.bucket, key),
            target,
        })
    }

    async fn callback(
        &self,
        protocol: &'static str,
        session: &SessionData,
        original: &str,
        upload: &Uploaded,
        handle: Option<&SessionHandle>,
    ) -> Result<()> {
        self.metrics
            .upload_queue_depth
            .with_label_values(&["callback"])
            .inc();
        let _permit = self.callback_slots.acquire().await?;
        self.metrics
            .upload_queue_depth
            .with_label_values(&["callback"])
            .dec();
        self.metrics
            .upload_active
            .with_label_values(&["callback"])
            .inc();
        let result = self
            .cms
            .photo_callback(session, &upload.s3_url, original, &upload.target)
            .await;
        self.metrics
            .upload_active
            .with_label_values(&["callback"])
            .dec();
        match result? {
            CallbackOutcome::Accepted => {
                self.metrics
                    .upload_events
                    .with_label_values(&[protocol, "callback", "success"])
                    .inc();
                Ok(())
            }
            CallbackOutcome::Denied => {
                if let Some(handle) = handle {
                    handle.invalidate();
                }
                self.metrics
                    .upload_events
                    .with_label_values(&[protocol, "callback", "denied"])
                    .inc();
                bail!("CMS denied photo callback")
            }
        }
    }
}

struct Uploaded {
    s3_url: String,
    target: String,
}

#[derive(Debug, thiserror::Error)]
enum UploadFailure {
    #[error("S3 credentials expired")]
    Expired,
    #[error("S3 upload denied")]
    Denied,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

async fn put_s3(
    credentials: &crate::domain::UploadCredentials,
    original: &str,
    path: &Path,
    key: &str,
) -> std::result::Result<(), UploadFailure> {
    let provider = SharedCredentialsProvider::new(Credentials::new(
        &credentials.access_key_id,
        &credentials.secret_access_key,
        Some(credentials.session_token.clone()),
        None,
        "colombo-cms",
    ));
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(credentials.region.clone()))
        .credentials_provider(provider);
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL_S3") {
        loader = loader.endpoint_url(endpoint);
    }
    let shared = loader.load().await;
    let mut builder = aws_sdk_s3::config::Builder::from(&shared);
    if std::env::var_os("AWS_ENDPOINT_URL_S3").is_some() {
        builder = builder.force_path_style(true);
    }
    let client = Client::from_conf(builder.build());
    let body = ByteStream::from_path(path)
        .await
        .map_err(|e| UploadFailure::Other(e.into()))?;
    let result = client
        .put_object()
        .bucket(&credentials.bucket)
        .key(key)
        .content_type(
            mime_guess::from_path(original)
                .first_or_octet_stream()
                .essence_str(),
        )
        .body(body)
        .send()
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(err) => {
            let code = err
                .as_service_error()
                .and_then(|e| e.code())
                .unwrap_or_default();
            let status = err.raw_response().map(|r| r.status().as_u16());
            if ["ExpiredToken", "RequestExpired", "InvalidToken"]
                .iter()
                .any(|v| code.eq_ignore_ascii_case(v))
            {
                Err(UploadFailure::Expired)
            } else if status == Some(403) || code.eq_ignore_ascii_case("AccessDenied") {
                Err(UploadFailure::Denied)
            } else {
                Err(UploadFailure::Other(anyhow!("S3 put failed ({code})")))
            }
        }
    }
}
