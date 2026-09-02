use std::{
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};

use anyhow::{Result, anyhow, bail};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, error::ProvideErrorMetadata, primitives::ByteStream};
use chrono::Utc;
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};
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

struct ActiveS3Work {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
}

impl Drop for ActiveS3Work {
    fn drop(&mut self) {
        self.metrics
            .upload_active
            .with_label_values(&["s3_upload"])
            .dec();
    }
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
            .upload_events
            .with_label_values(&["ftp", "accepted", "queued"])
            .inc();
        self.metrics
            .upload_queue_depth
            .with_label_values(&["s3_upload"])
            .inc();
        tokio::spawn(async move {
            let result = service.process_ftp(session, original, path).await;
            service
                .metrics
                .upload_events
                .with_label_values(&[
                    "ftp",
                    "complete",
                    if result.is_ok() { "success" } else { "failure" },
                ])
                .inc();
            if let Err(err) = result {
                error!(error = %err, "FTP background upload failed");
            }
        });
    }

    pub fn queue_http(self: &Arc<Self>, session: SessionData, original: String, path: PathBuf) {
        let service = Arc::clone(self);
        self.metrics
            .upload_events
            .with_label_values(&["http", "accepted", "queued"])
            .inc();
        self.metrics
            .upload_queue_depth
            .with_label_values(&["s3_upload"])
            .inc();
        tokio::spawn(async move {
            let result: Result<()> = async {
                let active = service.begin_s3_work().await;
                let upload = service.process_once(&session, &original, &path).await?;
                drop(active);
                service.callback(&session, &original, &upload, None).await
            }
            .await;
            let _ = tokio::fs::remove_file(&path).await;
            service
                .metrics
                .upload_events
                .with_label_values(&[
                    "http",
                    "complete",
                    if result.is_ok() { "success" } else { "failure" },
                ])
                .inc();
            if let Err(err) = result {
                error!(error = %err, "HTTP background upload or callback failed");
            }
        });
    }

    async fn process_ftp(
        &self,
        handle: Arc<SessionHandle>,
        original: String,
        path: PathBuf,
    ) -> Result<()> {
        let active = self.begin_s3_work().await;
        if !handle.is_valid() {
            bail!("FTP session was evicted");
        }
        let session = handle.snapshot().await;
        let upload = match self.process_once(&session, &original, &path).await {
            Ok(result) => result,
            Err(UploadFailure::Expired) => {
                self.metrics
                    .retry_attempts
                    .with_label_values(&["s3_put", "started"])
                    .inc();
                let Some(key) = session.validation_key.as_deref() else {
                    self.metrics
                        .retry_attempts
                        .with_label_values(&["credential_refresh", "denied"])
                        .inc();
                    self.metrics
                        .retry_attempts
                        .with_label_values(&["s3_put", "abandoned"])
                        .inc();
                    handle.invalidate();
                    bail!("session has no validation key");
                };
                match self
                    .cms
                    .validate(&session.tenant, key, "validation_refresh")
                    .await
                {
                    Ok(fresh) => {
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["credential_refresh", "success"])
                            .inc();
                        handle.replace(fresh.clone()).await;
                        match self.process_once(&fresh, &original, &path).await {
                            Ok(result) => {
                                self.metrics
                                    .retry_attempts
                                    .with_label_values(&["s3_put", "success"])
                                    .inc();
                                result
                            }
                            Err(err) => {
                                self.metrics
                                    .retry_attempts
                                    .with_label_values(&["s3_put", "failure"])
                                    .inc();
                                if matches!(err, UploadFailure::Expired | UploadFailure::Denied) {
                                    handle.invalidate();
                                }
                                return Err(err.into());
                            }
                        }
                    }
                    Err(ValidationError::Denied) => {
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["credential_refresh", "denied"])
                            .inc();
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["s3_put", "abandoned"])
                            .inc();
                        handle.invalidate();
                        bail!("CMS denied credential refresh");
                    }
                    Err(err) => {
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["credential_refresh", "unavailable"])
                            .inc();
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["s3_put", "abandoned"])
                            .inc();
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
        drop(active);
        let active = handle.snapshot().await;
        self.callback(&active, &original, &upload, Some(&handle))
            .await
    }

    async fn begin_s3_work(&self) -> ActiveS3Work {
        let permit = self
            .s3_slots
            .clone()
            .acquire_owned()
            .await
            .expect("S3 semaphore remains open while the service is alive");
        self.metrics
            .upload_queue_depth
            .with_label_values(&["s3_upload"])
            .dec();
        self.metrics
            .upload_active
            .with_label_values(&["s3_upload"])
            .inc();
        ActiveS3Work {
            _permit: permit,
            metrics: self.metrics.clone(),
        }
    }

    async fn process_once(
        &self,
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
        let started = Instant::now();
        let result = put_s3(credentials, original, path, &key).await;
        let label = match &result {
            Ok(()) => "success",
            Err(UploadFailure::Expired) => "expired",
            Err(UploadFailure::Denied) => "denied",
            Err(UploadFailure::Other(_)) => "error",
        };
        self.metrics
            .dependency_duration
            .with_label_values(&["s3", "put_object", label])
            .observe(started.elapsed().as_secs_f64());
        result?;
        info!(assignment_id = %session.assignment_id, "S3 upload complete");
        Ok(Uploaded {
            s3_url: format!("s3://{}/{}", credentials.bucket, key),
            target,
        })
    }

    async fn callback(
        &self,
        session: &SessionData,
        original: &str,
        upload: &Uploaded,
        handle: Option<&SessionHandle>,
    ) -> Result<()> {
        self.metrics
            .upload_queue_depth
            .with_label_values(&["cms_callback"])
            .inc();
        let _permit = self.callback_slots.acquire().await?;
        self.metrics
            .upload_queue_depth
            .with_label_values(&["cms_callback"])
            .dec();
        self.metrics
            .upload_active
            .with_label_values(&["cms_callback"])
            .inc();
        let result = self
            .cms
            .photo_callback(session, &upload.s3_url, original, &upload.target)
            .await;
        self.metrics
            .upload_active
            .with_label_values(&["cms_callback"])
            .dec();
        match result? {
            CallbackOutcome::Accepted => Ok(()),
            CallbackOutcome::Denied => {
                if let Some(handle) = handle {
                    handle.invalidate();
                }
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
