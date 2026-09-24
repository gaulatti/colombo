use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Result, anyhow, bail};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, error::ProvideErrorMetadata, primitives::ByteStream};
use chrono::{Duration, Utc};
use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    cms::{CallbackOutcome, CmsClient, DeliveryError, ValidationError},
    db,
    domain::SessionData,
    metrics::Metrics,
    naming,
    spool::{
        CALLBACK_ATTEMPT_LIMIT, CONFIRMED_RETENTION_DAYS, FAILED_RETENTION_DAYS, FailureCode,
        PrivateUploadContext, SourceProtocol, Spool, UPLOAD_ATTEMPT_LIMIT, UploadReceipt,
        UploadRecord, UploadState, WandererReason,
    },
};

const WORKER_COUNT: usize = 8;
const WORK_QUEUE_CAPACITY: usize = 256;

#[derive(Debug)]
pub struct SessionHandle {
    data: RwLock<SessionData>,
    valid: AtomicBool,
    last_validated: Mutex<Instant>,
}

impl SessionHandle {
    pub fn new(data: SessionData) -> Arc<Self> {
        Arc::new(Self {
            data: RwLock::new(data),
            valid: AtomicBool::new(true),
            last_validated: Mutex::new(Instant::now()),
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
        let mut last_validated = self.last_validated.lock().await;
        *self.data.write().await = value;
        *last_validated = Instant::now();
    }

    pub async fn revalidate_if_due(
        &self,
        cms: &CmsClient,
        metrics: &Metrics,
    ) -> Result<(), RevalidationError> {
        if !self.is_valid() {
            return Err(RevalidationError::Denied);
        }
        // Keep the check and CMS request under one per-session lock. Other
        // connections, including those with the same username, remain independent.
        let mut last_validated = self.last_validated.lock().await;
        let current = self.snapshot().await;
        let Some(seconds) = current.tenant.revalidate_after_seconds else {
            return Ok(());
        };
        let Some(key) = current.validation_key.as_deref() else {
            return Ok(());
        };
        if last_validated.elapsed() < StdDuration::from_secs(seconds as u64) {
            return Ok(());
        }
        let result = cms
            .validate(&current.tenant, key, "validation_revalidate")
            .await;
        match result {
            Ok(fresh) => {
                if !self.is_valid() {
                    return Err(RevalidationError::Denied);
                }
                let outcome = if fresh.assignment_id == current.assignment_id {
                    "success"
                } else {
                    "reassigned"
                };
                *self.data.write().await = fresh;
                *last_validated = Instant::now();
                metrics
                    .retry_attempts
                    .with_label_values(&["session_revalidation", outcome])
                    .inc();
                Ok(())
            }
            Err(ValidationError::Denied) => {
                self.invalidate();
                metrics
                    .retry_attempts
                    .with_label_values(&["session_revalidation", "denied"])
                    .inc();
                Err(RevalidationError::Denied)
            }
            Err(ValidationError::Unavailable(_)) => {
                metrics
                    .retry_attempts
                    .with_label_values(&["session_revalidation", "unavailable"])
                    .inc();
                Ok(())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RevalidationError {
    #[error("FTP session denied")]
    Denied,
}

#[derive(Clone)]
pub struct UploadService {
    pool: PgPool,
    cms: CmsClient,
    metrics: Arc<Metrics>,
    spool: Arc<Spool>,
    sender: mpsc::Sender<Uuid>,
    scheduled: Arc<DashMap<Uuid, ()>>,
    recovered: Arc<DashMap<Uuid, ()>>,
    session_handles: Arc<DashMap<Uuid, Weak<SessionHandle>>>,
    s3_slots: Arc<Semaphore>,
    callback_slots: Arc<Semaphore>,
}

struct ActiveWork {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<Metrics>,
    queue: &'static str,
}

impl Drop for ActiveWork {
    fn drop(&mut self) {
        self.metrics
            .upload_active
            .with_label_values(&[self.queue])
            .dec();
    }
}

impl std::fmt::Debug for UploadService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UploadService")
            .field("spool", &self.spool.root())
            .finish_non_exhaustive()
    }
}

impl UploadService {
    pub async fn revalidate_ftp_session(
        &self,
        session: &SessionHandle,
    ) -> Result<(), RevalidationError> {
        session.revalidate_if_due(&self.cms, &self.metrics).await
    }
    pub async fn new(
        pool: PgPool,
        cms: CmsClient,
        metrics: Arc<Metrics>,
        spool_path: PathBuf,
    ) -> Result<Arc<Self>> {
        let spool = Arc::new(tokio::task::spawn_blocking(move || Spool::open(spool_path)).await??);
        let (sender, receiver) = mpsc::channel(WORK_QUEUE_CAPACITY);
        let service = Arc::new(Self {
            pool,
            cms,
            metrics,
            spool,
            sender,
            scheduled: Arc::new(DashMap::new()),
            recovered: Arc::new(DashMap::new()),
            session_handles: Arc::new(DashMap::new()),
            s3_slots: Arc::new(Semaphore::new(WORKER_COUNT)),
            callback_slots: Arc::new(Semaphore::new(4)),
        });
        service.start_workers(receiver);
        service.scan_and_schedule(true).await?;
        service.start_scheduler();
        Ok(service)
    }

    pub async fn accept_http(
        self: &Arc<Self>,
        session: SessionData,
        original: String,
        path: PathBuf,
    ) -> Result<UploadReceipt> {
        let record = self
            .persist_acceptance(session, SourceProtocol::Http, original, path)
            .await?;
        self.observe_acceptance(&record);
        self.enqueue(record.operation_id);
        Ok(record.receipt())
    }

    pub async fn accept_ftp(
        self: &Arc<Self>,
        session: Arc<SessionHandle>,
        original: String,
        path: PathBuf,
    ) -> Result<Uuid> {
        if !session.is_valid() {
            bail!("FTP session was evicted");
        }
        let snapshot = session.snapshot().await;
        let record = self
            .persist_acceptance(snapshot, SourceProtocol::Ftp, original, path.clone())
            .await?;
        if let Err(error) = tokio::fs::remove_file(&path).await {
            warn!(operation_id = %record.operation_id, error = %error, "durably spooled FTP ingress copy could not be removed");
        }
        self.session_handles
            .insert(record.operation_id, Arc::downgrade(&session));
        self.observe_acceptance(&record);
        self.enqueue(record.operation_id);
        Ok(record.operation_id)
    }

    pub async fn receipt_for_assignment(
        &self,
        operation_id: Uuid,
        tenant_id: i64,
        assignment_id: String,
    ) -> Result<Option<UploadReceipt>> {
        let spool = self.spool.clone();
        tokio::task::spawn_blocking(move || match spool.load_record(operation_id) {
            Ok(record)
                if record.tenant_id == tenant_id && record.assignment_id == assignment_id =>
            {
                Ok(Some(record.receipt()))
            }
            Ok(_) => Ok(None),
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|value| value.kind() == std::io::ErrorKind::NotFound) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        })
        .await?
    }

    async fn persist_acceptance(
        &self,
        session: SessionData,
        source: SourceProtocol,
        original: String,
        path: PathBuf,
    ) -> Result<UploadRecord> {
        let spool = self.spool.clone();
        tokio::task::spawn_blocking(move || spool.accept(&session, source, &original, &path))
            .await?
    }

    fn observe_acceptance(&self, record: &UploadRecord) {
        self.metrics
            .upload_events
            .with_label_values(&[record.source_protocol.as_str(), "accepted", "queued"])
            .inc();
        self.metrics
            .spool_outcomes
            .with_label_values(&[record.source_protocol.as_str(), "accepted"])
            .inc();
        info!(
            operation_id = %record.operation_id,
            source = record.source_protocol.as_str(),
            "upload durably accepted"
        );
    }

    fn start_workers(self: &Arc<Self>, receiver: mpsc::Receiver<Uuid>) {
        let receiver = Arc::new(Mutex::new(receiver));
        for _ in 0..WORKER_COUNT {
            let service = self.clone();
            let receiver = receiver.clone();
            tokio::spawn(async move {
                loop {
                    let next = receiver.lock().await.recv().await;
                    let Some(operation_id) = next else {
                        return;
                    };
                    if let Err(error) = service.process(operation_id).await {
                        error!(operation_id = %operation_id, error = %error, "durable upload worker failed");
                    }
                    service.scheduled.remove(&operation_id);
                }
            });
        }
    }

    fn start_scheduler(self: &Arc<Self>) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(StdDuration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) = service.scan_and_schedule(false).await {
                    error!(error = %error, "durable upload spool scan failed");
                }
            }
        });
    }

    fn enqueue(&self, operation_id: Uuid) {
        if self.scheduled.insert(operation_id, ()).is_some() {
            return;
        }
        if self.sender.try_send(operation_id).is_err() {
            self.scheduled.remove(&operation_id);
        }
    }

    async fn scan_and_schedule(&self, startup: bool) -> Result<()> {
        let spool = self.spool.clone();
        let (records, expired_sources) = tokio::task::spawn_blocking(move || {
            let expired_sources = spool.maintain(Utc::now())?;
            let mut records = Vec::new();
            for operation_id in spool.operation_ids()? {
                records.push(spool.load_record(operation_id)?);
            }
            Result::<(Vec<UploadRecord>, Vec<SourceProtocol>)>::Ok((records, expired_sources))
        })
        .await??;

        for source in expired_sources {
            self.metrics
                .spool_outcomes
                .with_label_values(&[source.as_str(), "expired"])
                .inc();
        }

        let now = Utc::now();
        let states = [
            UploadState::Accepted,
            UploadState::Uploading,
            UploadState::Delivered,
            UploadState::Wandering,
            UploadState::Held,
            UploadState::CallbackConfirmed,
            UploadState::Failed,
            UploadState::Expired,
        ];
        let mut counts = [0_i64; 8];
        let mut oldest = [0_f64; 8];
        let mut s3_queued = 0_i64;
        let mut callback_queued = 0_i64;
        for record in &records {
            let index = states
                .iter()
                .position(|state| *state == record.state)
                .expect("every upload state has a metric slot");
            counts[index] += 1;
            oldest[index] = oldest[index].max(
                (now - record.accepted_at)
                    .to_std()
                    .unwrap_or_default()
                    .as_secs_f64(),
            );
            match record.state {
                UploadState::Accepted | UploadState::Uploading => s3_queued += 1,
                UploadState::Delivered | UploadState::Wandering => callback_queued += 1,
                _ => {}
            }
            if record.due(now) {
                self.enqueue(record.operation_id);
            }
            if startup
                && record.state.pending()
                && self.recovered.insert(record.operation_id, ()).is_none()
            {
                self.metrics
                    .spool_outcomes
                    .with_label_values(&[record.source_protocol.as_str(), "recovered"])
                    .inc();
            }
        }
        for (index, state) in states.iter().enumerate() {
            self.metrics
                .spool_operations
                .with_label_values(&[state.as_str()])
                .set(counts[index]);
            self.metrics
                .spool_oldest_age
                .with_label_values(&[state.as_str()])
                .set(oldest[index]);
        }
        self.metrics
            .upload_queue_depth
            .with_label_values(&["s3_upload"])
            .set(s3_queued);
        self.metrics
            .upload_queue_depth
            .with_label_values(&["cms_callback"])
            .set(callback_queued);
        self.metrics.oldest_wandering_age.set(oldest[3]);
        Ok(())
    }

    async fn process(&self, operation_id: Uuid) -> Result<()> {
        let spool = self.spool.clone();
        let lock = tokio::task::spawn_blocking(move || spool.try_lock(operation_id)).await??;
        let Some(_lock) = lock else {
            return Ok(());
        };
        let mut record = self.spool.load_record(operation_id)?;
        if !record.state.pending() || !record.due(Utc::now()) {
            return Ok(());
        }
        let private = match self.spool.load_private(operation_id) {
            Ok(value) => value,
            Err(error) => {
                warn!(operation_id = %operation_id, error = %error, "durable upload private context is unavailable");
                self.fail(&mut record, FailureCode::InvalidMetadata).await?;
                return Ok(());
            }
        };
        let Some(tenant) = db::tenant_by_id(&self.pool, record.tenant_id).await? else {
            self.fail(&mut record, FailureCode::TenantMissing).await?;
            return Ok(());
        };
        let mut session = SessionData {
            tenant,
            assignment_id: record.assignment_id.clone(),
            device_id: record.device_id.clone(),
            upload: Some(private.upload.clone()),
            validation_key: private.validation_key.clone(),
        };
        let mut private = private;

        if record.state == UploadState::Wandering {
            if Utc::now() >= record.accepted_at + Duration::days(FAILED_RETENTION_DAYS) {
                self.fail(&mut record, FailureCode::RetryExhausted).await?;
                return Ok(());
            }
            self.deliver_wanderer(&mut record, &session).await?;
            return Ok(());
        }
        if self.late_enabled(&session) && Utc::now() >= record.accepted_at + Duration::hours(48) {
            self.start_wandering(&mut record, WandererReason::DeadlineExceeded)
                .await?;
            self.deliver_wanderer(&mut record, &session).await?;
            return Ok(());
        }

        if matches!(record.state, UploadState::Accepted | UploadState::Uploading) {
            let spool = self.spool.clone();
            let verification_record = record.clone();
            if tokio::task::spawn_blocking(move || spool.verify_content(&verification_record))
                .await?
                .is_err()
            {
                self.fail(&mut record, FailureCode::CorruptContent).await?;
                return Ok(());
            }
            if let Err(failure) = self
                .deliver_to_s3(&mut record, &mut private, &mut session)
                .await
            {
                self.handle_upload_failure(&mut record, failure).await?;
                return Ok(());
            }
        }

        if record.state == UploadState::Delivered {
            self.deliver_callback(&mut record, &session).await?;
        }
        Ok(())
    }

    async fn deliver_to_s3(
        &self,
        record: &mut UploadRecord,
        private: &mut PrivateUploadContext,
        session: &mut SessionData,
    ) -> std::result::Result<(), StageFailure> {
        if record.upload_attempts >= UPLOAD_ATTEMPT_LIMIT {
            return Err(StageFailure::Transient(anyhow!(
                "S3 retry budget exhausted"
            )));
        }
        record.upload_attempts += 1;
        record.state = UploadState::Uploading;
        record.updated_at = Utc::now();
        self.spool
            .save_record(record)
            .map_err(StageFailure::Transient)?;

        if record.target_filename.is_none() {
            let target = self.resolve_target(record, session).await?;
            let credentials = session.upload.as_ref().ok_or(StageFailure::Invalid)?;
            let key = if credentials.key_prefix.ends_with('/') {
                format!("{}{}", credentials.key_prefix, target)
            } else {
                format!("{}/{}", credentials.key_prefix, target)
            };
            record.target_filename = Some(target);
            record.object_bucket = Some(credentials.bucket.clone());
            record.object_key = Some(key);
            record.updated_at = Utc::now();
            self.spool
                .save_record(record)
                .map_err(StageFailure::Transient)?;
        }

        let mut result = self.put_attempt(record, session).await;
        if matches!(result, Err(UploadFailure::Expired))
            && record.upload_attempts < UPLOAD_ATTEMPT_LIMIT
        {
            self.metrics
                .retry_attempts
                .with_label_values(&["credential_refresh", "started"])
                .inc();
            if session
                .upload
                .as_ref()
                .and_then(|u| u.credentials_endpoint.as_ref())
                .is_some()
            {
                match self
                    .cms
                    .refresh_credentials(session, record.accepted_at)
                    .await
                {
                    Ok(fresh_upload) => {
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["credential_refresh", "success"])
                            .inc();
                        private.upload = fresh_upload.clone();
                        self.spool
                            .save_private(record.operation_id, private)
                            .map_err(StageFailure::Transient)?;
                        session.upload = Some(fresh_upload);
                        result = self.put_attempt(record, session).await;
                    }
                    Err(DeliveryError::Denied(reason)) => {
                        return Err(StageFailure::CmsDenied(reason));
                    }
                    Err(DeliveryError::Unavailable(error)) => {
                        return Err(StageFailure::CmsUnavailable(error));
                    }
                    Err(DeliveryError::Invalid(_)) => return Err(StageFailure::Invalid),
                }
            } else {
                let key = private
                    .validation_key
                    .as_deref()
                    .ok_or(StageFailure::Denied)?;
                match self
                    .cms
                    .validate(&session.tenant, key, "validation_refresh")
                    .await
                {
                    Ok(fresh) if fresh.assignment_id == record.assignment_id => {
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["credential_refresh", "success"])
                            .inc();
                        let fresh_upload = fresh.upload.clone().ok_or(StageFailure::Invalid)?;
                        private.upload = fresh_upload;
                        self.spool
                            .save_private(record.operation_id, private)
                            .map_err(StageFailure::Transient)?;
                        *session = fresh;
                        if let Some(handle) = self
                            .session_handles
                            .get(&record.operation_id)
                            .and_then(|value| value.upgrade())
                        {
                            handle.replace(session.clone()).await;
                        }
                        result = self.put_attempt(record, session).await;
                    }
                    Ok(_) | Err(ValidationError::Denied) => {
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["credential_refresh", "denied"])
                            .inc();
                        return Err(StageFailure::Denied);
                    }
                    Err(ValidationError::Unavailable(error)) => {
                        self.metrics
                            .retry_attempts
                            .with_label_values(&["credential_refresh", "unavailable"])
                            .inc();
                        return Err(StageFailure::CmsUnavailable(error));
                    }
                }
            }
        }

        match result {
            Ok(()) => {
                let bucket = record
                    .object_bucket
                    .as_deref()
                    .ok_or(StageFailure::Invalid)?;
                let key = record.object_key.as_deref().ok_or(StageFailure::Invalid)?;
                let now = Utc::now();
                record.state = UploadState::Delivered;
                record.s3_url = Some(format!("s3://{bucket}/{key}"));
                record.delivered_at = Some(now);
                record.updated_at = now;
                record.next_attempt_at = now;
                self.spool
                    .save_record(record)
                    .map_err(StageFailure::Transient)?;
                self.metrics
                    .spool_outcomes
                    .with_label_values(&[record.source_protocol.as_str(), "delivered"])
                    .inc();
                info!(operation_id = %record.operation_id, "S3 delivery persisted");
                Ok(())
            }
            Err(UploadFailure::Denied) => Err(StageFailure::Denied),
            Err(UploadFailure::Expired) => Err(StageFailure::Transient(anyhow!(
                "S3 credentials remained expired after refresh"
            ))),
            Err(UploadFailure::Other(error)) => Err(StageFailure::Transient(error)),
        }
    }

    async fn resolve_target(
        &self,
        record: &UploadRecord,
        session: &SessionData,
    ) -> std::result::Result<String, StageFailure> {
        let credentials = session.upload.as_ref().ok_or(StageFailure::Invalid)?;
        if let Some(policy) = &credentials.naming_policy {
            if !policy.valid() {
                return Err(StageFailure::Invalid);
            }
            let sequence = self
                .cms
                .next_sequence(session, record.accepted_at)
                .await
                .map_err(|e| match e {
                    DeliveryError::Denied(reason) if self.late_enabled(session) => {
                        StageFailure::CmsDenied(reason)
                    }
                    DeliveryError::Denied(_) => {
                        StageFailure::Transient(anyhow!("CMS sequence denied"))
                    }
                    DeliveryError::Unavailable(error) => StageFailure::CmsUnavailable(error),
                    DeliveryError::Invalid(error) => StageFailure::Transient(error),
                })?;
            naming::render(
                policy,
                &session.assignment_id,
                &record.original_filename,
                naming::capture_time(&self.spool.content_path(record.operation_id)),
                Utc::now(),
                sequence,
            )
            .map_err(StageFailure::Transient)
        } else {
            Ok(record.original_filename.clone())
        }
    }

    async fn put_attempt(
        &self,
        record: &UploadRecord,
        session: &SessionData,
    ) -> std::result::Result<(), UploadFailure> {
        let credentials = session
            .upload
            .as_ref()
            .ok_or_else(|| UploadFailure::Other(anyhow!("upload credentials missing")))?;
        let bucket = record
            .object_bucket
            .as_deref()
            .ok_or_else(|| UploadFailure::Other(anyhow!("object bucket missing")))?;
        let key = record
            .object_key
            .as_deref()
            .ok_or_else(|| UploadFailure::Other(anyhow!("object key missing")))?;
        let active = self.begin_work("s3_upload", &self.s3_slots).await;
        let started = Instant::now();
        let result = put_s3(
            credentials,
            &record.content_type,
            &self.spool.content_path(record.operation_id),
            bucket,
            key,
        )
        .await;
        drop(active);
        let result_label = match &result {
            Ok(()) => "success",
            Err(UploadFailure::Expired) => "expired",
            Err(UploadFailure::Denied) => "denied",
            Err(UploadFailure::Other(_)) => "error",
        };
        self.metrics
            .dependency_duration
            .with_label_values(&["s3", "put_object", result_label])
            .observe(started.elapsed().as_secs_f64());
        result
    }

    async fn handle_upload_failure(
        &self,
        record: &mut UploadRecord,
        failure: StageFailure,
    ) -> Result<()> {
        match failure {
            StageFailure::CmsDenied(reason) if self.has_wanderers(record) => {
                self.start_wandering(record, reason).await
            }
            StageFailure::CmsUnavailable(error) if self.has_wanderers(record) => {
                warn!(operation_id = %record.operation_id, error = %error, "CMS unavailable; upload rescheduled");
                record.upload_attempts = record.upload_attempts.saturating_sub(1);
                record.state = UploadState::Accepted;
                record.next_attempt_at = Utc::now() + Duration::minutes(5);
                self.spool.save_record(record)
            }
            StageFailure::CmsDenied(_) => self.fail(record, FailureCode::DependencyDenied).await,
            StageFailure::CmsUnavailable(error)
                if record.upload_attempts >= UPLOAD_ATTEMPT_LIMIT =>
            {
                warn!(operation_id = %record.operation_id, error = %error, "CMS retry budget exhausted");
                self.fail(record, FailureCode::RetryExhausted).await
            }
            StageFailure::CmsUnavailable(error) => {
                warn!(operation_id = %record.operation_id, error = %error, "CMS delivery will retry");
                record.state = UploadState::Accepted;
                record.next_attempt_at = Utc::now() + retry_delay(record.upload_attempts);
                self.spool.save_record(record)
            }
            StageFailure::Denied => self.fail(record, FailureCode::DependencyDenied).await,
            StageFailure::Invalid => self.fail(record, FailureCode::InvalidMetadata).await,
            StageFailure::Transient(error) if record.upload_attempts >= UPLOAD_ATTEMPT_LIMIT => {
                warn!(operation_id = %record.operation_id, error = %error, "S3 retry budget exhausted");
                self.fail(record, FailureCode::RetryExhausted).await
            }
            StageFailure::Transient(error) => {
                warn!(operation_id = %record.operation_id, error = %error, "S3 delivery will retry");
                self.metrics
                    .retry_attempts
                    .with_label_values(&["s3_put", "scheduled"])
                    .inc();
                let now = Utc::now();
                record.state = UploadState::Accepted;
                record.updated_at = now;
                record.next_attempt_at = now + retry_delay(record.upload_attempts);
                self.spool.save_record(record)
            }
        }
    }

    async fn deliver_callback(
        &self,
        record: &mut UploadRecord,
        session: &SessionData,
    ) -> Result<()> {
        if record.callback_attempts >= CALLBACK_ATTEMPT_LIMIT {
            return self.fail(record, FailureCode::RetryExhausted).await;
        }
        record.callback_attempts += 1;
        record.updated_at = Utc::now();
        self.spool.save_record(record)?;
        let (Some(target), Some(s3_url)) =
            (record.target_filename.as_deref(), record.s3_url.as_deref())
        else {
            return self.fail(record, FailureCode::InvalidMetadata).await;
        };
        let active = self.begin_work("cms_callback", &self.callback_slots).await;
        let result = self
            .cms
            .photo_callback(
                session,
                s3_url,
                &record.original_filename,
                target,
                record.accepted_at,
                record.device_id.as_deref(),
            )
            .await;
        drop(active);
        match result {
            Ok(CallbackOutcome::Accepted) => {
                let now = Utc::now();
                record.state = UploadState::CallbackConfirmed;
                record.updated_at = now;
                record.terminal_at = Some(now);
                record.expires_at = Some(now + Duration::days(CONFIRMED_RETENTION_DAYS));
                record.failure_code = None;
                self.spool.save_record(record)?;
                self.spool.delete_private_data(record.operation_id)?;
                self.metrics
                    .upload_events
                    .with_label_values(&[record.source_protocol.as_str(), "complete", "success"])
                    .inc();
                self.metrics
                    .spool_outcomes
                    .with_label_values(&[record.source_protocol.as_str(), "callback_confirmed"])
                    .inc();
                self.session_handles.remove(&record.operation_id);
                info!(operation_id = %record.operation_id, "CMS callback confirmation persisted");
                Ok(())
            }
            Err(DeliveryError::Denied(reason)) if self.late_enabled(session) => {
                self.start_wandering(record, reason).await
            }
            Err(DeliveryError::Denied(_)) => self.fail(record, FailureCode::DependencyDenied).await,
            Err(DeliveryError::Unavailable(error)) if self.late_enabled(session) => {
                record.callback_attempts = record.callback_attempts.saturating_sub(1);
                record.next_attempt_at = Utc::now() + Duration::minutes(5);
                self.spool.save_record(record)?;
                warn!(operation_id = %record.operation_id, error = %error, "CMS unavailable; callback rescheduled");
                Ok(())
            }
            Err(DeliveryError::Invalid(_)) => self.fail(record, FailureCode::InvalidMetadata).await,
            Err(error) if record.callback_attempts >= CALLBACK_ATTEMPT_LIMIT => {
                warn!(operation_id = %record.operation_id, error = %error, "callback retry budget exhausted");
                self.fail(record, FailureCode::RetryExhausted).await
            }
            Err(error) => {
                warn!(operation_id = %record.operation_id, error = %error, "callback delivery will retry");
                self.metrics
                    .retry_attempts
                    .with_label_values(&["cms_callback", "scheduled"])
                    .inc();
                let now = Utc::now();
                record.state = UploadState::Delivered;
                record.updated_at = now;
                record.next_attempt_at = now + retry_delay(record.callback_attempts);
                self.spool.save_record(record)
            }
        }
    }

    async fn fail(&self, record: &mut UploadRecord, code: FailureCode) -> Result<()> {
        let now = Utc::now();
        record.state = UploadState::Failed;
        record.updated_at = now;
        record.terminal_at = Some(now);
        record.expires_at = Some(now + Duration::days(FAILED_RETENTION_DAYS));
        record.failure_code = Some(code);
        self.spool.save_record(record)?;
        if let Err(error) = self.spool.quarantine_content(record.operation_id) {
            error!(operation_id = %record.operation_id, error = %error, "failed upload content could not be quarantined");
        }
        if let Err(error) = self.spool.delete_private_data(record.operation_id) {
            error!(operation_id = %record.operation_id, error = %error, "failed upload private context could not be removed");
        }
        if code == FailureCode::DependencyDenied
            && let Some(handle) = self
                .session_handles
                .get(&record.operation_id)
                .and_then(|value| value.upgrade())
        {
            handle.invalidate();
        }
        self.session_handles.remove(&record.operation_id);
        self.metrics
            .upload_events
            .with_label_values(&[record.source_protocol.as_str(), "complete", "failure"])
            .inc();
        self.metrics
            .spool_outcomes
            .with_label_values(&[record.source_protocol.as_str(), code.as_str()])
            .inc();
        Ok(())
    }

    fn has_wanderers(&self, record: &UploadRecord) -> bool {
        self.spool
            .load_private(record.operation_id)
            .ok()
            .and_then(|private| private.upload.wanderers_endpoint)
            .is_some()
    }

    fn late_enabled(&self, session: &SessionData) -> bool {
        session
            .upload
            .as_ref()
            .and_then(|upload| upload.wanderers_endpoint.as_ref())
            .is_some()
    }

    async fn start_wandering(
        &self,
        record: &mut UploadRecord,
        reason: WandererReason,
    ) -> Result<()> {
        record.state = UploadState::Wandering;
        record.wanderer_reason = Some(reason);
        record.updated_at = Utc::now();
        record.next_attempt_at = Utc::now();
        record.expires_at = Some(record.accepted_at + Duration::days(FAILED_RETENTION_DAYS));
        self.spool.save_record(record)?;
        self.metrics
            .spool_outcomes
            .with_label_values(&[record.source_protocol.as_str(), "wandering"])
            .inc();
        Ok(())
    }

    async fn deliver_wanderer(
        &self,
        record: &mut UploadRecord,
        session: &SessionData,
    ) -> Result<()> {
        // A registered operation is idempotent in the CMS, including after process restart.
        let registration = match self.cms.register_wanderer(session, record).await {
            Ok(value) => value,
            Err(DeliveryError::Denied(_)) => {
                return self.fail(record, FailureCode::DependencyDenied).await;
            }
            Err(DeliveryError::Unavailable(error)) => {
                warn!(operation_id = %record.operation_id, error = %error, "wanderer registration will retry");
                record.next_attempt_at = Utc::now() + Duration::minutes(5);
                return self.spool.save_record(record);
            }
            Err(DeliveryError::Invalid(_)) => {
                return self.fail(record, FailureCode::InvalidMetadata).await;
            }
        };
        if matches!(
            registration.status.as_str(),
            "pending_review" | "assigned" | "discarded"
        ) {
            return self.hold(record).await;
        }
        if registration.status != "registered" {
            return self.fail(record, FailureCode::InvalidMetadata).await;
        }
        let s3_url = if let Some(url) = record.s3_url.clone() {
            url
        } else {
            let Some(credentials) = registration.upload else {
                return self.fail(record, FailureCode::InvalidMetadata).await;
            };
            if !credentials.valid()
                || credentials.key_prefix != format!("wanderers/{}/", record.operation_id)
            {
                return self.fail(record, FailureCode::InvalidMetadata).await;
            }
            let basename = record
                .original_filename
                .rsplit(['/', '\\'])
                .next()
                .unwrap_or_default();
            let basename = naming::sanitize(basename);
            if naming::validate_target(&basename).is_err() {
                return self.fail(record, FailureCode::InvalidMetadata).await;
            }
            let key = format!("wanderers/{}/{}", record.operation_id, basename);
            match put_s3(
                &credentials,
                &record.content_type,
                &self.spool.content_path(record.operation_id),
                &credentials.bucket,
                &key,
            )
            .await
            {
                Ok(()) => {}
                Err(error) => {
                    warn!(operation_id = %record.operation_id, error = %error, "wanderer S3 delivery will retry");
                    record.next_attempt_at = Utc::now() + Duration::minutes(5);
                    return self.spool.save_record(record);
                }
            }
            let url = format!("s3://{}/{}", credentials.bucket, key);
            record.s3_url = Some(url.clone());
            self.spool.save_record(record)?;
            url
        };
        match self.cms.wanderer_delivered(session, record, &s3_url).await {
            Ok(()) => self.hold(record).await,
            Err(DeliveryError::Denied(_)) => self.fail(record, FailureCode::DependencyDenied).await,
            Err(DeliveryError::Unavailable(error)) => {
                warn!(operation_id = %record.operation_id, error = %error, "wanderer delivery confirmation will retry");
                record.next_attempt_at = Utc::now() + Duration::minutes(5);
                self.spool.save_record(record)
            }
            Err(DeliveryError::Invalid(_)) => self.fail(record, FailureCode::InvalidMetadata).await,
        }
    }

    async fn hold(&self, record: &mut UploadRecord) -> Result<()> {
        let now = Utc::now();
        record.state = UploadState::Held;
        record.updated_at = now;
        record.terminal_at = Some(now);
        record.expires_at = Some(now + Duration::days(CONFIRMED_RETENTION_DAYS));
        self.spool.save_record(record)?;
        self.spool.delete_private_data(record.operation_id)?;
        self.session_handles.remove(&record.operation_id);
        self.metrics
            .spool_outcomes
            .with_label_values(&[record.source_protocol.as_str(), "held"])
            .inc();
        Ok(())
    }

    async fn begin_work(&self, queue: &'static str, semaphore: &Arc<Semaphore>) -> ActiveWork {
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .expect("upload worker semaphore remains open");
        self.metrics.upload_active.with_label_values(&[queue]).inc();
        ActiveWork {
            _permit: permit,
            metrics: self.metrics.clone(),
            queue,
        }
    }
}

fn retry_delay(attempt: u32) -> Duration {
    match attempt {
        0 | 1 => Duration::seconds(1),
        2 => Duration::seconds(5),
        3 => Duration::seconds(30),
        4 => Duration::minutes(2),
        _ => Duration::minutes(5),
    }
}

#[derive(Debug)]
enum StageFailure {
    Denied,
    CmsDenied(WandererReason),
    CmsUnavailable(anyhow::Error),
    Invalid,
    Transient(anyhow::Error),
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
    content_type: &str,
    path: &Path,
    bucket: &str,
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
        .map_err(|error| UploadFailure::Other(error.into()))?;
    let result = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .content_type(content_type)
        .body(body)
        .send()
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) => {
            let code = error
                .as_service_error()
                .and_then(|value| value.code())
                .unwrap_or_default();
            let status = error.raw_response().map(|value| value.status().as_u16());
            if ["ExpiredToken", "RequestExpired", "InvalidToken"]
                .iter()
                .any(|value| code.eq_ignore_ascii_case(value))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{Tenant, UploadCredentials},
        spool::EXPIRED_RECEIPT_RETENTION_DAYS,
    };
    use sqlx::postgres::PgPoolOptions;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn revalidation_obeys_threshold_and_failures_per_session() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/validate", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let mode = Arc::new(AtomicUsize::new(0));
        let request_count = requests.clone();
        let response_mode = mode.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut buffer = [0u8; 8192];
                let bytes_read = stream.read(&mut buffer).await.unwrap();
                assert!(bytes_read > 0);
                request_count.fetch_add(1, Ordering::SeqCst);
                let (status, body) = match response_mode.load(Ordering::SeqCst) {
                    1 => (
                        "200 OK",
                        r#"{"assignmentId":"assignment-new","upload":{"accessKeyId":"fake","secretAccessKey":"fake","sessionToken":"fake","region":"local","bucket":"fake","keyPrefix":"assignment-new/","expiresAt":"2099-01-01T00:00:00Z"}}"#,
                    ),
                    2 => ("503 Service Unavailable", "{}"),
                    3 => ("401 Unauthorized", "{}"),
                    _ => (
                        "200 OK",
                        r#"{"assignmentId":"assignment-123","upload":{"accessKeyId":"fake","secretAccessKey":"fake","sessionToken":"fake","region":"local","bucket":"fake","keyPrefix":"assignment-123/","expiresAt":"2099-01-01T00:00:00Z"}}"#,
                    ),
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let metrics = Metrics::new("test").unwrap();
        let cms = CmsClient::new(metrics.clone()).unwrap();
        let mut data = session();
        data.tenant.validation_endpoint = endpoint;
        data.tenant.revalidate_after_seconds = Some(60);
        let first = SessionHandle::new(data.clone());
        let second = SessionHandle::new(data);
        *first.last_validated.lock().await = Instant::now() - StdDuration::from_secs(10);
        first.revalidate_if_due(&cms, &metrics).await.unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 0);
        *first.last_validated.lock().await = Instant::now() - StdDuration::from_secs(61);
        first.revalidate_if_due(&cms, &metrics).await.unwrap();
        first.revalidate_if_due(&cms, &metrics).await.unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(first.snapshot().await.upload.unwrap().access_key_id, "fake");
        assert_eq!(second.snapshot().await.assignment_id, "assignment-123");
        mode.store(1, Ordering::SeqCst);
        *first.last_validated.lock().await = Instant::now() - StdDuration::from_secs(61);
        first.revalidate_if_due(&cms, &metrics).await.unwrap();
        assert_eq!(first.snapshot().await.assignment_id, "assignment-new");
        assert_eq!(second.snapshot().await.assignment_id, "assignment-123");
        mode.store(2, Ordering::SeqCst);
        *first.last_validated.lock().await = Instant::now() - StdDuration::from_secs(61);
        first.revalidate_if_due(&cms, &metrics).await.unwrap();
        assert_eq!(requests.load(Ordering::SeqCst), 3);
        mode.store(3, Ordering::SeqCst);
        assert!(first.revalidate_if_due(&cms, &metrics).await.is_err());
        assert!(!first.is_valid());
        assert!(first.revalidate_if_due(&cms, &metrics).await.is_err());
        assert_eq!(requests.load(Ordering::SeqCst), 4);
        server.abort();
    }

    #[tokio::test]
    async fn disabled_and_master_sessions_never_revalidate() {
        let metrics = Metrics::new("test").unwrap();
        let cms = CmsClient::new(metrics.clone()).unwrap();
        let disabled = SessionHandle::new(session());
        *disabled.last_validated.lock().await = Instant::now() - StdDuration::from_secs(61);
        disabled.revalidate_if_due(&cms, &metrics).await.unwrap();
        let mut master_data = session();
        master_data.tenant.revalidate_after_seconds = Some(1);
        master_data.validation_key = None;
        let master = SessionHandle::new(master_data);
        *master.last_validated.lock().await = Instant::now() - StdDuration::from_secs(61);
        master.revalidate_if_due(&cms, &metrics).await.unwrap();
        for result in ["success", "denied", "unavailable", "reassigned"] {
            assert_eq!(
                metrics
                    .retry_attempts
                    .with_label_values(&["session_revalidation", result])
                    .get(),
                0
            );
        }
    }

    fn session() -> SessionData {
        SessionData {
            tenant: Tenant {
                id: 7,
                name: "Test".into(),
                ftp_username: "photographer".into(),
                api_key: "tenant-api-key".into(),
                validation_endpoint: "http://example.test/validate".into(),
                photo_endpoint: "http://example.test/photo".into(),
                revalidate_after_seconds: None,
            },
            assignment_id: "assignment-123".into(),
            upload: Some(UploadCredentials {
                access_key_id: "access".into(),
                secret_access_key: "secret".into(),
                session_token: "token".into(),
                region: "us-east-1".into(),
                bucket: "uploads".into(),
                key_prefix: "assignment-123".into(),
                expires_at: "2099-01-01T00:00:00Z".into(),
                naming_policy: None,
                sequence_endpoint: None,
                credentials_endpoint: None,
                wanderers_endpoint: None,
            }),
            validation_key: Some("client-secret".into()),
            device_id: None,
        }
    }

    fn test_service(spool: Arc<Spool>) -> UploadService {
        let metrics = Metrics::new("test").unwrap();
        let cms = CmsClient::new(metrics.clone()).unwrap();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://localhost/colombo")
            .unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        UploadService {
            pool,
            cms,
            metrics,
            spool,
            sender,
            scheduled: Arc::new(DashMap::new()),
            recovered: Arc::new(DashMap::new()),
            session_handles: Arc::new(DashMap::new()),
            s3_slots: Arc::new(Semaphore::new(1)),
            callback_slots: Arc::new(Semaphore::new(1)),
        }
    }

    #[test]
    fn retries_are_bounded_and_back_off() {
        assert_eq!(UPLOAD_ATTEMPT_LIMIT, 5);
        assert_eq!(CALLBACK_ATTEMPT_LIMIT, 5);
        assert_eq!(retry_delay(1), Duration::seconds(1));
        assert_eq!(retry_delay(4), Duration::minutes(2));
        assert_eq!(retry_delay(5), Duration::minutes(5));
    }

    #[tokio::test]
    async fn wandering_and_held_are_durable_and_receipt_keeps_private_fields_out() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        std::fs::write(&source, b"durable-media").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let mut session = session();
        session.device_id = Some("17".into());
        session.upload.as_mut().unwrap().wanderers_endpoint = Some("/wanderers".into());
        let mut record = spool
            .accept(&session, SourceProtocol::Http, "source.bin", &source)
            .unwrap();
        let service = test_service(spool.clone());
        service
            .start_wandering(&mut record, WandererReason::AssignmentMissing)
            .await
            .unwrap();
        let restored = spool.load_record(record.operation_id).unwrap();
        assert_eq!(restored.state, UploadState::Wandering);
        assert_eq!(
            restored.wanderer_reason,
            Some(WandererReason::AssignmentMissing)
        );
        assert!(restored.due(Utc::now()));
        service.hold(&mut record).await.unwrap();
        let receipt =
            serde_json::to_value(spool.load_record(record.operation_id).unwrap().receipt())
                .unwrap();
        assert_eq!(receipt["state"], "held");
        for private in [
            "device_id",
            "s3_url",
            "object_bucket",
            "object_key",
            "original_filename",
        ] {
            assert!(receipt.get(private).is_none(), "receipt exposed {private}");
        }
        assert!(!spool.content_path(record.operation_id).exists());
        assert!(spool.load_private(record.operation_id).is_err());
    }

    #[tokio::test]
    async fn cms_outage_does_not_spend_upload_attempt_budget_when_advertised() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        std::fs::write(&source, b"durable-media").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let mut session = session();
        session.upload.as_mut().unwrap().wanderers_endpoint = Some("/wanderers".into());
        let mut record = spool
            .accept(&session, SourceProtocol::Http, "source.bin", &source)
            .unwrap();
        record.upload_attempts = 1;
        let service = test_service(spool.clone());
        let before = Utc::now();
        service
            .handle_upload_failure(
                &mut record,
                StageFailure::CmsUnavailable(anyhow!("simulated outage")),
            )
            .await
            .unwrap();
        let restored = spool.load_record(record.operation_id).unwrap();
        assert_eq!(restored.upload_attempts, 0);
        assert_eq!(restored.state, UploadState::Accepted);
        assert!(restored.next_attempt_at >= before + Duration::minutes(5));
    }

    #[tokio::test]
    async fn retry_exhaustion_quarantines_bytes_and_cleanup_expires_the_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        std::fs::write(&source, b"durable-media").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let mut record = spool
            .accept(&session(), SourceProtocol::Http, "source.bin", &source)
            .unwrap();
        record.upload_attempts = UPLOAD_ATTEMPT_LIMIT;
        let service = test_service(spool.clone());
        assert!(
            service
                .receipt_for_assignment(
                    record.operation_id,
                    record.tenant_id,
                    record.assignment_id.clone(),
                )
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            service
                .receipt_for_assignment(
                    record.operation_id,
                    record.tenant_id,
                    "assignment-other".into(),
                )
                .await
                .unwrap()
                .is_none()
        );

        service
            .handle_upload_failure(
                &mut record,
                StageFailure::Transient(anyhow!("simulated dependency failure")),
            )
            .await
            .unwrap();

        let failed = spool.load_record(record.operation_id).unwrap();
        assert_eq!(failed.state, UploadState::Failed);
        assert_eq!(failed.failure_code, Some(FailureCode::RetryExhausted));
        assert!(!spool.content_path(record.operation_id).exists());
        assert!(spool.load_private(record.operation_id).is_err());
        assert!(
            spool
                .root()
                .join("quarantine")
                .join(format!("{}.content", record.operation_id))
                .exists()
        );

        let after_failure_retention = failed.expires_at.unwrap() + Duration::seconds(1);
        spool.maintain(after_failure_retention).unwrap();
        let expired = spool.load_record(record.operation_id).unwrap();
        assert_eq!(expired.state, UploadState::Expired);
        assert!(
            !spool
                .root()
                .join("quarantine")
                .join(format!("{}.content", record.operation_id))
                .exists()
        );

        spool
            .maintain(
                after_failure_retention
                    + Duration::days(EXPIRED_RECEIPT_RETENTION_DAYS)
                    + Duration::seconds(1),
            )
            .unwrap();
        assert!(spool.load_record(record.operation_id).is_err());
    }
}
