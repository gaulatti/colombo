use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration as StdDuration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, error::ProvideErrorMetadata, primitives::ByteStream};
use chrono::{DateTime, Duration, Utc};
use dashmap::DashMap;
use sqlx::PgPool;
use tokio::sync::{Mutex, OwnedSemaphorePermit, RwLock, Semaphore, mpsc};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{
    cms::{CallbackOutcome, CmsClient, ValidationError},
    db,
    domain::SessionData,
    metrics::Metrics,
    naming,
    spool::{
        CALLBACK_ATTEMPT_LIMIT, CONFIRMED_RETENTION_DAYS, FAILED_RETENTION_DAYS, FailureCode,
        PrivateUploadContext, SourceProtocol, Spool, UPLOAD_ATTEMPT_LIMIT, UploadReceipt,
        UploadRecord, UploadState,
    },
};

const WORKER_COUNT: usize = 8;
const WORK_QUEUE_CAPACITY: usize = 256;
const SPOOL_SCAN_INTERVAL: StdDuration = StdDuration::from_secs(60);

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
    pool: PgPool,
    cms: CmsClient,
    metrics: Arc<Metrics>,
    spool: Arc<Spool>,
    sender: mpsc::Sender<Uuid>,
    scheduled: Arc<DashMap<Uuid, ()>>,
    pending: Arc<DashMap<Uuid, DateTime<Utc>>>,
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
    #[cfg(test)]
    pub(crate) fn isolated(pool: PgPool, spool: Arc<Spool>) -> Arc<Self> {
        let metrics = Metrics::new("test").unwrap();
        let cms = CmsClient::new(metrics.clone()).unwrap();
        let (sender, _receiver) = mpsc::channel(1);
        Arc::new(Self {
            pool,
            cms,
            metrics,
            spool,
            sender,
            scheduled: Arc::new(DashMap::new()),
            pending: Arc::new(DashMap::new()),
            recovered: Arc::new(DashMap::new()),
            session_handles: Arc::new(DashMap::new()),
            s3_slots: Arc::new(Semaphore::new(1)),
            callback_slots: Arc::new(Semaphore::new(1)),
        })
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
            pending: Arc::new(DashMap::new()),
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
        self.pending
            .insert(record.operation_id, record.next_attempt_at);
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
        self.pending
            .insert(record.operation_id, record.next_attempt_at);
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
                    if let Err(error) = service.refresh_pending(operation_id) {
                        error!(operation_id = %operation_id, error = %error, "durable upload state refresh failed");
                    }
                    service.scheduled.remove(&operation_id);
                }
            });
        }
    }

    fn start_scheduler(self: &Arc<Self>) {
        let service = self.clone();
        tokio::spawn(async move {
            let mut due_interval = tokio::time::interval(StdDuration::from_secs(1));
            due_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut scan_interval = tokio::time::interval(SPOOL_SCAN_INTERVAL);
            scan_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            scan_interval.tick().await; // startup already scanned the spool
            loop {
                tokio::select! {
                    _ = due_interval.tick() => service.schedule_due(),
                    _ = scan_interval.tick() => service.rescan().await,
                }
            }
        });
    }

    async fn rescan(&self) {
        if let Err(error) = self.scan_and_schedule(false).await {
            error!(error = %error, "durable upload spool scan failed");
        }
    }

    fn schedule_due(&self) {
        let now = Utc::now();
        for entry in self.pending.iter() {
            if *entry.value() <= now {
                self.enqueue(*entry.key());
            }
        }
    }

    fn refresh_pending(&self, operation_id: Uuid) -> Result<()> {
        let record = self.spool.load_record(operation_id)?;
        if record.state.pending() {
            self.pending.insert(operation_id, record.next_attempt_at);
        } else {
            self.pending.remove(&operation_id);
        }
        Ok(())
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
            UploadState::CallbackConfirmed,
            UploadState::Failed,
            UploadState::Expired,
        ];
        let mut counts = [0_i64; 6];
        let mut oldest = [0_f64; 6];
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
                UploadState::Delivered => callback_queued += 1,
                _ => {}
            }
            if record.due(now) {
                self.enqueue(record.operation_id);
            }
            if record.state.pending() {
                self.pending
                    .insert(record.operation_id, record.next_attempt_at);
            } else {
                self.pending.remove(&record.operation_id);
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
            upload: Some(private.upload.clone()),
            validation_key: private.validation_key.clone(),
        };
        let mut private = private;

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

        // Pending operations reach here only after persisted S3 delivery.
        self.deliver_callback(&mut record, &session).await?;
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
            let target = self
                .resolve_target(record, session)
                .await
                .map_err(StageFailure::Transient)?;
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
                    return Err(StageFailure::Transient(error));
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

    async fn resolve_target(&self, record: &UploadRecord, session: &SessionData) -> Result<String> {
        let credentials = session
            .upload
            .as_ref()
            .context("upload credentials missing")?;
        if let Some(policy) = &credentials.naming_policy {
            if !policy.valid() {
                bail!("invalid upload naming policy");
            }
            let sequence = self.cms.next_sequence(session).await?;
            naming::render(
                policy,
                &session.assignment_id,
                &record.original_filename,
                naming::capture_time(&self.spool.content_path(record.operation_id)),
                Utc::now(),
                sequence,
            )
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
            .photo_callback(session, s3_url, &record.original_filename, target)
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
            Ok(CallbackOutcome::Denied) => self.fail(record, FailureCode::DependencyDenied).await,
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

    #[tokio::test]
    async fn legacy_log_consumers_receive_structured_acceptance_and_listener_events() {
        if std::env::var_os("COLOMBO_LOG_FIXTURE_CHILD").is_none() {
            let status=std::process::Command::new(std::env::current_exe().unwrap()).args(["--exact","upload::tests::legacy_log_consumers_receive_structured_acceptance_and_listener_events"])
                .env("COLOMBO_LOG_FIXTURE_CHILD","1").status().unwrap();
            assert!(status.success());
            return;
        }
        struct Logger(std::sync::Mutex<Vec<String>>);
        impl log::Log for Logger {
            fn enabled(&self, _: &log::Metadata<'_>) -> bool {
                true
            }
            fn log(&self, record: &log::Record<'_>) {
                self.0.lock().unwrap().push(record.args().to_string());
            }
            fn flush(&self) {}
        }
        static LOGGER: Logger = Logger(std::sync::Mutex::new(Vec::new()));
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::write(&source, b"content").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let service = Arc::new(test_service(spool.clone()));
        let record = spool
            .accept(&session(), SourceProtocol::Http, "source", &source)
            .unwrap();
        service.observe_acceptance(&record);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mut config = crate::test_support::config();
        config.ftp_port = listener.local_addr().unwrap().port();
        config.ftp_root = spool.root().join("ftp-incoming");
        let passive = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = passive.local_addr().unwrap().port();
        drop(passive);
        config.passive_ports = port..=port;
        let _ = tokio::time::timeout(
            StdDuration::from_secs(1),
            crate::ftp::serve(
                config,
                service.pool.clone(),
                service.cms.clone(),
                service.clone(),
                service.metrics.clone(),
            ),
        )
        .await;
        let state = crate::http::AppState {
            pool: service.pool.clone(),
            cms: service.cms.clone(),
            uploads: service.clone(),
            metrics: service.metrics.clone(),
            metrics_token: Arc::from(""),
        };
        let http = tokio::spawn(crate::http::serve(0, state));
        tokio::time::sleep(StdDuration::from_millis(20)).await;
        assert!(
            std::process::Command::new("/bin/kill")
                .args(["-INT", &std::process::id().to_string()])
                .status()
                .unwrap()
                .success()
        );
        tokio::time::timeout(StdDuration::from_secs(2), http)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let messages = LOGGER.0.lock().unwrap();
        assert!(
            messages
                .iter()
                .any(|m| m.contains("upload durably accepted"))
        );
        assert!(messages.iter().any(|m| m.contains("FTP listener ready")));
        assert!(messages.iter().any(|m| m.contains("HTTP listener ready")));
        log::logger().flush();
    }

    #[tokio::test]
    async fn invalid_targets_and_incomplete_delivery_metadata_fail_explicitly() {
        use crate::{
            domain::{UploadNamingPolicy, UploadNamingSegment},
            test_support,
        };
        let mock = test_support::Mock::new(vec![(200, r#"{"sequence":7}"#.into())]).await;
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::write(&source, b"content").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let service = test_service(spool.clone());
        let mut session = test_support::session(&mock.url);
        let mut record = spool
            .accept(&session, SourceProtocol::Http, "original.jpg", &source)
            .unwrap();
        let mut private = spool.load_private(record.operation_id).unwrap();
        record.upload_attempts = UPLOAD_ATTEMPT_LIMIT;
        assert!(matches!(
            service
                .deliver_to_s3(&mut record, &mut private, &mut session)
                .await,
            Err(StageFailure::Transient(_))
        ));
        assert!(matches!(
            service.put_attempt(&record, &session).await,
            Err(UploadFailure::Other(_))
        ));
        record.object_bucket = Some("uploads".into());
        assert!(matches!(
            service.put_attempt(&record, &session).await,
            Err(UploadFailure::Other(_))
        ));
        session.upload = None;
        assert!(service.resolve_target(&record, &session).await.is_err());
        assert!(matches!(
            service.put_attempt(&record, &session).await,
            Err(UploadFailure::Other(_))
        ));
        session = test_support::session(&mock.url);
        let upload = session.upload.as_mut().unwrap();
        upload.sequence_endpoint = Some("/sequence".into());
        upload.naming_policy = Some(UploadNamingPolicy {
            version: 1,
            assignment_slug: "news".into(),
            path: vec![],
            filename: vec![UploadNamingSegment {
                kind: "placeholder".into(),
                value: None,
                name: Some("sequence".into()),
                format: None,
                width: Some(4),
            }],
            timezone: "UTC".into(),
            capture_time_fallback: "uploadedTime".into(),
            case_mode: "preserve".into(),
        });
        assert_eq!(
            service.resolve_target(&record, &session).await.unwrap(),
            "0007"
        );
        session
            .upload
            .as_mut()
            .unwrap()
            .naming_policy
            .as_mut()
            .unwrap()
            .version = 2;
        assert!(service.resolve_target(&record, &session).await.is_err());
        service
            .handle_upload_failure(&mut record, StageFailure::Invalid)
            .await
            .unwrap();
        assert_eq!(
            spool.load_record(record.operation_id).unwrap().failure_code,
            Some(FailureCode::InvalidMetadata)
        );
        // Local body setup fails before any request can reach an AWS endpoint.
        let missing = put_s3(
            session.upload.as_ref().unwrap(),
            "image/jpeg",
            &temp.path().join("missing"),
            "uploads",
            "key",
        )
        .await;
        assert!(matches!(missing, Err(UploadFailure::Other(_))));
    }

    #[tokio::test]
    async fn spool_scans_expire_receipts_and_count_delivered_due_work() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::write(&source, b"content").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let mut service = test_service(spool.clone());
        let (sender, mut receiver) = mpsc::channel(4);
        service.sender = sender;
        let mut terminal = spool
            .accept(&session(), SourceProtocol::Http, "expired", &source)
            .unwrap();
        terminal.state = UploadState::CallbackConfirmed;
        terminal.expires_at = Some(Utc::now() - Duration::seconds(1));
        spool.save_record(&terminal).unwrap();
        let mut pending = spool
            .accept(&session(), SourceProtocol::Ftp, "pending", &source)
            .unwrap();
        pending.state = UploadState::Delivered;
        spool.save_record(&pending).unwrap();
        service.scan_and_schedule(true).await.unwrap();
        assert_eq!(receiver.try_recv().unwrap(), pending.operation_id);
        assert_eq!(
            service
                .metrics
                .upload_queue_depth
                .with_label_values(&["cms_callback"])
                .get(),
            1
        );
        assert_eq!(
            service
                .metrics
                .spool_outcomes
                .with_label_values(&["http", "expired"])
                .get(),
            1
        );
        service.scan_and_schedule(true).await.unwrap();
        assert_eq!(
            service
                .metrics
                .spool_outcomes
                .with_label_values(&["ftp", "recovered"])
                .get(),
            1
        );
        service.scan_and_schedule(false).await.unwrap();
    }

    #[tokio::test]
    async fn failure_cleanup_is_best_effort_but_always_persists_the_terminal_receipt() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source");
        std::fs::write(&source, b"content").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let service = test_service(spool.clone());
        let mut record = spool
            .accept(&session(), SourceProtocol::Ftp, "source", &source)
            .unwrap();
        let quarantine = spool
            .root()
            .join("quarantine")
            .join(format!("{}.content", record.operation_id));
        std::fs::create_dir(&quarantine).unwrap();
        let private = spool
            .root()
            .join("operations")
            .join(record.operation_id.to_string())
            .join("private.json");
        std::fs::remove_file(&private).unwrap();
        std::fs::create_dir(&private).unwrap();
        service
            .fail(&mut record, FailureCode::DependencyDenied)
            .await
            .unwrap();
        assert_eq!(
            spool.load_record(record.operation_id).unwrap().state,
            UploadState::Failed
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn acceptance_survives_an_unremovable_ftp_ingress_copy() {
        use std::os::unix::fs::PermissionsExt;
        let temp = tempfile::tempdir().unwrap();
        let ingress = temp.path().join("ingress");
        std::fs::create_dir(&ingress).unwrap();
        let source = ingress.join("source");
        std::fs::write(&source, b"content").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let service = Arc::new(test_service(spool.clone()));
        let session = SessionHandle::new(session());
        std::fs::set_permissions(&ingress, std::fs::Permissions::from_mode(0o555)).unwrap();
        let accepted = service
            .accept_ftp(session, "source".into(), source.clone())
            .await;
        std::fs::set_permissions(&ingress, std::fs::Permissions::from_mode(0o755)).unwrap();
        let id = accepted.unwrap();
        assert!(source.exists());
        spool
            .verify_content(&spool.load_record(id).unwrap())
            .unwrap();
    }

    #[tokio::test]
    async fn local_s3_delivery_retries_refreshes_and_callbacks_preserve_durable_state() {
        use crate::test_support;
        let endpoint = match std::env::var("COLOMBO_S3_FIXTURE_ENDPOINT") {
            Ok(url) => url,
            Err(_) => {
                let mock = test_support::Mock::new(vec![(200, String::new())]).await;
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command.args(["--exact","upload::tests::local_s3_delivery_retries_refreshes_and_callbacks_preserve_durable_state","--nocapture"])
                    .env("COLOMBO_S3_FIXTURE_ENDPOINT",&mock.url).env("AWS_ENDPOINT_URL_S3",&mock.url)
                    .env("AWS_EC2_METADATA_DISABLED","true").env("AWS_MAX_ATTEMPTS","1");
                let status = tokio::task::spawn_blocking(move || command.status().unwrap())
                    .await
                    .unwrap();
                assert!(status.success());
                return;
            }
        };
        async fn replies(endpoint: &str, values: Vec<(u16, String)>) {
            assert!(
                reqwest::Client::new()
                    .post(format!("{endpoint}/__control"))
                    .json(&values)
                    .send()
                    .await
                    .unwrap()
                    .status()
                    .is_success()
            );
        }
        fn s3_error(code: &str) -> String {
            format!(
                "<Error><Code>{code}</Code><Message>fixture</Message><RequestId>test</RequestId></Error>"
            )
        }
        let temp = tempfile::tempdir().unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let source = temp.path().join("source.jpg");
        std::fs::write(&source, b"durable media").unwrap();
        let pool = test_support::database().await;
        let mut session = test_support::session(&endpoint);
        test_support::seed(&pool, &session).await;
        let mut service = test_service(spool.clone());
        service.pool = pool;
        assert!(format!("{service:?}").contains("UploadService"));
        let record = spool
            .accept(&session, SourceProtocol::Http, "source.jpg", &source)
            .unwrap();
        replies(&endpoint, vec![(200, String::new())]).await;
        service.process(record.operation_id).await.unwrap();
        let complete = spool.load_record(record.operation_id).unwrap();
        assert_eq!(complete.state, UploadState::CallbackConfirmed);
        assert_eq!(complete.upload_attempts, 1);
        assert_eq!(complete.callback_attempts, 1);
        assert_eq!(
            complete.object_key.as_deref(),
            Some("assignment-123/source.jpg")
        );
        assert!(spool.load_private(record.operation_id).is_err());
        service.process(record.operation_id).await.unwrap();

        // The same stable key survives both retryable S3 failure and a later successful delivery.
        for (status, code, failure) in [
            (403, "AccessDenied", FailureCode::DependencyDenied),
            (400, "AccessDenied", FailureCode::DependencyDenied),
            (500, "InternalError", FailureCode::RetryExhausted),
            (403, "ExpiredToken", FailureCode::DependencyDenied),
        ] {
            let mut record = spool
                .accept(&session, SourceProtocol::Ftp, "source.jpg", &source)
                .unwrap();
            let handle = SessionHandle::new(session.clone());
            service
                .session_handles
                .insert(record.operation_id, Arc::downgrade(&handle));
            if code == "InternalError" {
                record.upload_attempts = UPLOAD_ATTEMPT_LIMIT - 1;
                spool.save_record(&record).unwrap();
            }
            if code == "ExpiredToken" {
                let mut private = spool.load_private(record.operation_id).unwrap();
                private.validation_key = None;
                spool.save_private(record.operation_id, &private).unwrap();
            }
            replies(&endpoint, vec![(status, s3_error(code))]).await;
            service.process(record.operation_id).await.unwrap();
            let failed = spool.load_record(record.operation_id).unwrap();
            assert_eq!(failed.state, UploadState::Failed);
            assert_eq!(failed.failure_code, Some(failure));
            assert_eq!(handle.is_valid(), failure != FailureCode::DependencyDenied);
        }
        let mut expired = spool
            .accept(
                &session,
                SourceProtocol::Http,
                "expired-budget.jpg",
                &source,
            )
            .unwrap();
        expired.upload_attempts = UPLOAD_ATTEMPT_LIMIT - 1;
        spool.save_record(&expired).unwrap();
        replies(&endpoint, vec![(403, s3_error("ExpiredToken"))]).await;
        service.process(expired.operation_id).await.unwrap();
        assert_eq!(
            spool
                .load_record(expired.operation_id)
                .unwrap()
                .failure_code,
            Some(FailureCode::RetryExhausted)
        );
        let mut record = spool
            .accept(&session, SourceProtocol::Http, "retry.jpg", &source)
            .unwrap();
        replies(&endpoint, vec![(500, s3_error("InternalError"))]).await;
        service.process(record.operation_id).await.unwrap();
        record = spool.load_record(record.operation_id).unwrap();
        assert_eq!(record.state, UploadState::Accepted);
        assert!(record.next_attempt_at > Utc::now());
        let key = record.object_key.clone();
        service.process(record.operation_id).await.unwrap();
        assert_eq!(
            spool
                .load_record(record.operation_id)
                .unwrap()
                .upload_attempts,
            1
        );
        record.next_attempt_at = Utc::now();
        spool.save_record(&record).unwrap();
        replies(&endpoint, vec![(200, String::new()), (503, String::new())]).await;
        service.process(record.operation_id).await.unwrap();
        record = spool.load_record(record.operation_id).unwrap();
        assert_eq!(record.state, UploadState::Delivered);
        assert_eq!(record.object_key, key);
        record.next_attempt_at = Utc::now();
        spool.save_record(&record).unwrap();
        replies(&endpoint, vec![(204, String::new())]).await;
        service.process(record.operation_id).await.unwrap();
        assert_eq!(
            spool.load_record(record.operation_id).unwrap().state,
            UploadState::CallbackConfirmed
        );

        for (validation_status, mut fresh, expected) in [
            (
                200,
                test_support::session(&endpoint),
                UploadState::CallbackConfirmed,
            ),
            (
                200,
                {
                    let mut s = test_support::session(&endpoint);
                    s.assignment_id = "other-assignment".into();
                    s
                },
                UploadState::Failed,
            ),
            (401, test_support::session(&endpoint), UploadState::Failed),
            (503, test_support::session(&endpoint), UploadState::Accepted),
        ] {
            let record = spool
                .accept(&session, SourceProtocol::Ftp, "refresh.jpg", &source)
                .unwrap();
            let handle = SessionHandle::new(session.clone());
            service
                .session_handles
                .insert(record.operation_id, Arc::downgrade(&handle));
            fresh.upload.as_mut().unwrap().access_key_id = "fresh-access".into();
            replies(
                &endpoint,
                vec![
                    (403, s3_error("ExpiredToken")),
                    (validation_status, test_support::validation(&fresh)),
                    (200, String::new()),
                ],
            )
            .await;
            service.process(record.operation_id).await.unwrap();
            assert_eq!(
                spool.load_record(record.operation_id).unwrap().state,
                expected
            );
            if expected == UploadState::CallbackConfirmed {
                assert_eq!(
                    handle.snapshot().await.upload.unwrap().access_key_id,
                    "fresh-access"
                );
            }
        }
        // A refreshed credential that remains expired schedules a bounded retry.
        let record = spool
            .accept(&session, SourceProtocol::Http, "expired.jpg", &source)
            .unwrap();
        replies(
            &endpoint,
            vec![
                (403, s3_error("ExpiredToken")),
                (200, test_support::validation(&session)),
                (403, s3_error("InvalidToken")),
            ],
        )
        .await;
        service.process(record.operation_id).await.unwrap();
        assert_eq!(
            spool.load_record(record.operation_id).unwrap().state,
            UploadState::Accepted
        );

        for (status, attempts, expected) in [
            (403, 0, FailureCode::DependencyDenied),
            (503, CALLBACK_ATTEMPT_LIMIT - 1, FailureCode::RetryExhausted),
            (200, CALLBACK_ATTEMPT_LIMIT, FailureCode::RetryExhausted),
        ] {
            let mut record = spool
                .accept(&session, SourceProtocol::Http, "callback.jpg", &source)
                .unwrap();
            record.state = UploadState::Delivered;
            record.target_filename = Some("callback.jpg".into());
            record.s3_url = Some("s3://uploads/key".into());
            record.callback_attempts = attempts;
            spool.save_record(&record).unwrap();
            replies(&endpoint, vec![(status, String::new())]).await;
            service.process(record.operation_id).await.unwrap();
            assert_eq!(
                spool.load_record(record.operation_id).unwrap().failure_code,
                Some(expected)
            );
        }
        let mut record = spool
            .accept(&session, SourceProtocol::Http, "metadata.jpg", &source)
            .unwrap();
        record.state = UploadState::Delivered;
        spool.save_record(&record).unwrap();
        service.process(record.operation_id).await.unwrap();
        assert_eq!(
            spool.load_record(record.operation_id).unwrap().failure_code,
            Some(FailureCode::InvalidMetadata)
        );
        session.upload.as_mut().unwrap().key_prefix.push('/');
        let record = spool
            .accept(&session, SourceProtocol::Http, "slash.jpg", &source)
            .unwrap();
        replies(&endpoint, vec![(200, String::new())]).await;
        service.process(record.operation_id).await.unwrap();
        assert_eq!(
            spool
                .load_record(record.operation_id)
                .unwrap()
                .object_key
                .as_deref(),
            Some("assignment-123/slash.jpg")
        );
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
            }),
            validation_key: Some("client-secret".into()),
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
            pending: Arc::new(DashMap::new()),
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
        assert_eq!(retry_delay(0), Duration::seconds(1));
        assert_eq!(retry_delay(2), Duration::seconds(5));
        assert_eq!(retry_delay(3), Duration::seconds(30));
        assert_eq!(retry_delay(4), Duration::minutes(2));
        assert_eq!(retry_delay(5), Duration::minutes(5));
    }

    #[tokio::test]
    async fn recovery_rejects_missing_tenants_context_and_corrupt_content() {
        use crate::test_support;
        let temp = tempfile::tempdir().unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let source = temp.path().join("source");
        std::fs::write(&source, b"content").unwrap();
        let pool = test_support::database().await;
        let session = test_support::session("http://unused");
        test_support::seed(&pool, &session).await;
        let mut service = test_service(spool.clone());
        service.pool = pool;
        for code in [
            FailureCode::InvalidMetadata,
            FailureCode::TenantMissing,
            FailureCode::CorruptContent,
        ] {
            let mut record = spool
                .accept(&session, SourceProtocol::Http, "source", &source)
                .unwrap();
            match code {
                FailureCode::InvalidMetadata => std::fs::remove_file(
                    spool
                        .root()
                        .join("operations")
                        .join(record.operation_id.to_string())
                        .join("private.json"),
                )
                .unwrap(),
                FailureCode::TenantMissing => {
                    record.tenant_id = 99;
                    spool.save_record(&record).unwrap();
                }
                _ => std::fs::write(spool.content_path(record.operation_id), b"corrupt").unwrap(),
            }
            service.process(record.operation_id).await.unwrap();
            assert_eq!(
                spool.load_record(record.operation_id).unwrap().failure_code,
                Some(code)
            );
        }
        let mut record = spool
            .accept(&session, SourceProtocol::Http, "locked", &source)
            .unwrap();
        let lock = spool.try_lock(record.operation_id).unwrap().unwrap();
        service.process(record.operation_id).await.unwrap();
        assert_eq!(
            spool
                .load_record(record.operation_id)
                .unwrap()
                .upload_attempts,
            0
        );
        drop(lock);
        record.next_attempt_at = Utc::now() + Duration::minutes(1);
        spool.save_record(&record).unwrap();
        service.process(record.operation_id).await.unwrap();
        assert_eq!(
            spool
                .load_record(record.operation_id)
                .unwrap()
                .upload_attempts,
            0
        );
        for (tenant, assignment) in [
            (99, session.assignment_id.clone()),
            (session.tenant.id, "wrong".into()),
        ] {
            assert!(
                service
                    .receipt_for_assignment(record.operation_id, tenant, assignment)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert!(
            service
                .receipt_for_assignment(Uuid::new_v4(), 1, "assignment-123".into())
                .await
                .unwrap()
                .is_none()
        );
        std::fs::write(
            spool
                .root()
                .join("operations")
                .join(record.operation_id.to_string())
                .join("record.json"),
            b"invalid",
        )
        .unwrap();
        assert!(
            service
                .receipt_for_assignment(record.operation_id, 1, "assignment-123".into())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn background_workers_release_reservations_and_scheduler_survives_scan_failures() {
        let temp = tempfile::tempdir().unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let service = Arc::new(test_service(spool.clone()));
        let (sender, receiver) = mpsc::channel(2);
        service.start_workers(receiver);
        let missing = Uuid::new_v4();
        service.scheduled.insert(missing, ());
        sender.send(missing).await.unwrap();
        for _ in 0..100 {
            if !service.scheduled.contains_key(&missing) {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(1)).await;
        }
        assert!(!service.scheduled.contains_key(&missing));
        let source = temp.path().join("terminal");
        std::fs::write(&source, b"content").unwrap();
        let mut record = spool
            .accept(&session(), SourceProtocol::Http, "terminal", &source)
            .unwrap();
        record.state = UploadState::CallbackConfirmed;
        spool.save_record(&record).unwrap();
        service.scheduled.insert(record.operation_id, ());
        sender.send(record.operation_id).await.unwrap();
        for _ in 0..100 {
            if !service.scheduled.contains_key(&record.operation_id) {
                break;
            }
            tokio::time::sleep(StdDuration::from_millis(1)).await;
        }
        assert!(!service.scheduled.contains_key(&record.operation_id));
        drop(sender);
        let operations = spool.root().join("operations");
        let saved = spool.root().join("saved-operations");
        std::fs::rename(&operations, &saved).unwrap();
        service.rescan().await;
        tokio::time::pause();
        service.start_scheduler();
        tokio::task::spawn_blocking(|| std::thread::sleep(StdDuration::from_millis(30)))
            .await
            .unwrap();
        tokio::time::advance(StdDuration::from_secs(61)).await;
        tokio::task::spawn_blocking(|| std::thread::sleep(StdDuration::from_millis(30)))
            .await
            .unwrap();
        std::fs::rename(saved, operations).unwrap();
        tokio::time::advance(StdDuration::from_secs(61)).await;
        tokio::task::spawn_blocking(|| std::thread::sleep(StdDuration::from_millis(30)))
            .await
            .unwrap();
        let mut service_with_receiver = test_service(spool);
        let (sender, mut receiver) = mpsc::channel(1);
        service_with_receiver.sender = sender;
        let id = Uuid::new_v4();
        service_with_receiver.enqueue(id);
        service_with_receiver.enqueue(id);
        assert_eq!(receiver.try_recv().unwrap(), id);
        assert!(receiver.try_recv().is_err());
    }

    #[tokio::test]
    async fn scheduler_tracks_pending_retries_without_rechecking_terminal_receipts() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        std::fs::write(&source, b"durable-media").unwrap();
        let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
        let mut pending = spool
            .accept(&session(), SourceProtocol::Http, "pending.bin", &source)
            .unwrap();
        pending.next_attempt_at = Utc::now() + Duration::minutes(1);
        spool.save_record(&pending).unwrap();
        let mut terminal = spool
            .accept(&session(), SourceProtocol::Http, "terminal.bin", &source)
            .unwrap();
        terminal.state = UploadState::CallbackConfirmed;
        terminal.expires_at = Some(Utc::now() + Duration::days(1));
        spool.save_record(&terminal).unwrap();

        let mut service = test_service(spool.clone());
        let (sender, mut receiver) = mpsc::channel(1);
        service.sender = sender;
        service.scan_and_schedule(true).await.unwrap();
        assert_eq!(service.pending.len(), 1);
        assert!(service.pending.contains_key(&pending.operation_id));
        service.schedule_due();
        assert!(receiver.try_recv().is_err());

        pending.next_attempt_at = Utc::now() - Duration::seconds(1);
        spool.save_record(&pending).unwrap();
        service.refresh_pending(pending.operation_id).unwrap();
        service.schedule_due();
        assert_eq!(receiver.try_recv().unwrap(), pending.operation_id);
        assert!(!service.pending.contains_key(&terminal.operation_id));

        pending.state = UploadState::Failed;
        spool.save_record(&pending).unwrap();
        service.refresh_pending(pending.operation_id).unwrap();
        service.scheduled.remove(&pending.operation_id);
        service.schedule_due();
        assert!(receiver.try_recv().is_err());
        assert_eq!(service.pending.len(), 0);

        let metrics = String::from_utf8(service.metrics.encode().unwrap()).unwrap();
        assert!(
            metrics.contains("colombo_upload_spool_operations{state=\"callback_confirmed\"} 1")
        );
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
