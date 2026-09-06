use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::domain::{SessionData, UploadCredentials};

pub const UPLOAD_ATTEMPT_LIMIT: u32 = 5;
pub const CALLBACK_ATTEMPT_LIMIT: u32 = 5;
pub const CONFIRMED_RETENTION_DAYS: i64 = 7;
pub const FAILED_RETENTION_DAYS: i64 = 30;
pub const EXPIRED_RECEIPT_RETENTION_DAYS: i64 = 30;

const RECORD_FILE: &str = "record.json";
const PRIVATE_FILE: &str = "private.json";
const CONTENT_FILE: &str = "content";
const LOCK_FILE: &str = "operation.lock";

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceProtocol {
    Http,
    Ftp,
}

impl SourceProtocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Ftp => "ftp",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum UploadState {
    Accepted,
    Uploading,
    Delivered,
    CallbackConfirmed,
    Failed,
    Expired,
}

impl UploadState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Uploading => "uploading",
            Self::Delivered => "delivered",
            Self::CallbackConfirmed => "callback_confirmed",
            Self::Failed => "failed",
            Self::Expired => "expired",
        }
    }

    pub fn pending(self) -> bool {
        matches!(self, Self::Accepted | Self::Uploading | Self::Delivered)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureCode {
    CorruptContent,
    DependencyDenied,
    InvalidMetadata,
    RetryExhausted,
    TenantMissing,
}

impl FailureCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CorruptContent => "corrupt_content",
            Self::DependencyDenied => "dependency_denied",
            Self::InvalidMetadata => "invalid_metadata",
            Self::RetryExhausted => "retry_exhausted",
            Self::TenantMissing => "tenant_missing",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct UploadRecord {
    pub version: u8,
    pub operation_id: Uuid,
    pub tenant_id: i64,
    pub assignment_id: String,
    pub original_filename: String,
    pub source_protocol: SourceProtocol,
    pub content_length: u64,
    pub checksum_sha256: String,
    pub content_type: String,
    pub accepted_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub state: UploadState,
    pub upload_attempts: u32,
    pub callback_attempts: u32,
    pub next_attempt_at: DateTime<Utc>,
    pub target_filename: Option<String>,
    pub object_bucket: Option<String>,
    pub object_key: Option<String>,
    pub s3_url: Option<String>,
    pub delivered_at: Option<DateTime<Utc>>,
    pub terminal_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub failure_code: Option<FailureCode>,
}

impl UploadRecord {
    pub fn due(&self, now: DateTime<Utc>) -> bool {
        self.state.pending() && self.next_attempt_at <= now
    }

    pub fn receipt(&self) -> UploadReceipt {
        UploadReceipt {
            operation_id: self.operation_id,
            assignment_id: self.assignment_id.clone(),
            state: self.state,
            source_protocol: self.source_protocol,
            content_length: self.content_length,
            checksum_sha256: self.checksum_sha256.clone(),
            accepted_at: self.accepted_at,
            updated_at: self.updated_at,
            upload_attempts: self.upload_attempts,
            callback_attempts: self.callback_attempts,
            failure_code: self.failure_code,
            expires_at: self.expires_at,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UploadReceipt {
    pub operation_id: Uuid,
    pub assignment_id: String,
    pub state: UploadState,
    pub source_protocol: SourceProtocol,
    pub content_length: u64,
    pub checksum_sha256: String,
    pub accepted_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub upload_attempts: u32,
    pub callback_attempts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_code: Option<FailureCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PrivateUploadContext {
    pub upload: UploadCredentials,
    pub validation_key: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Spool {
    root: PathBuf,
}

impl Spool {
    pub fn open(root: PathBuf) -> Result<Self> {
        let spool = Self { root };
        fs::create_dir_all(spool.operations_dir())?;
        fs::create_dir_all(spool.staging_dir())?;
        fs::create_dir_all(spool.quarantine_dir())?;
        fs::create_dir_all(spool.root.join("ftp-incoming"))?;
        set_private_dir(&spool.root)?;
        set_private_dir(&spool.operations_dir())?;
        set_private_dir(&spool.staging_dir())?;
        set_private_dir(&spool.quarantine_dir())?;
        set_private_dir(&spool.root.join("ftp-incoming"))?;
        spool.clear_incomplete_staging()?;
        sync_dir(&spool.root)?;
        Ok(spool)
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn accept(
        &self,
        session: &SessionData,
        source_protocol: SourceProtocol,
        original_filename: &str,
        source_path: &Path,
    ) -> Result<UploadRecord> {
        let upload = session
            .upload
            .clone()
            .context("upload credentials missing at acceptance")?;
        let operation_id = Uuid::new_v4();
        let staging = self.staging_dir().join(format!("{operation_id}.part"));
        fs::create_dir(&staging)?;
        set_private_dir(&staging)?;

        let result = (|| {
            let content_path = staging.join(CONTENT_FILE);
            let mut input = File::open(source_path).context("open accepted upload source")?;
            let mut output = private_file(&content_path)?;
            let mut digest = Sha256::new();
            let mut length = 0_u64;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = input.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                output.write_all(&buffer[..read])?;
                digest.update(&buffer[..read]);
                length += read as u64;
            }
            if length == 0 {
                bail!("accepted upload is empty");
            }
            output.sync_all()?;

            let now = Utc::now();
            let record = UploadRecord {
                version: 1,
                operation_id,
                tenant_id: session.tenant.id,
                assignment_id: session.assignment_id.clone(),
                original_filename: original_filename.to_owned(),
                source_protocol,
                content_length: length,
                checksum_sha256: format!("{:x}", digest.finalize()),
                content_type: mime_guess::from_path(original_filename)
                    .first_or_octet_stream()
                    .essence_str()
                    .to_owned(),
                accepted_at: now,
                updated_at: now,
                state: UploadState::Accepted,
                upload_attempts: 0,
                callback_attempts: 0,
                next_attempt_at: now,
                target_filename: None,
                object_bucket: None,
                object_key: None,
                s3_url: None,
                delivered_at: None,
                terminal_at: None,
                expires_at: None,
                failure_code: None,
            };
            let private = PrivateUploadContext {
                upload,
                validation_key: session.validation_key.clone(),
            };
            write_json_new(&staging.join(RECORD_FILE), &record)?;
            write_json_new(&staging.join(PRIVATE_FILE), &private)?;
            sync_dir(&staging)?;

            let operation = self.operation_dir(operation_id);
            fs::rename(&staging, &operation)?;
            sync_dir(&self.operations_dir())?;
            Ok(record)
        })();

        if result.is_err() {
            let _ = fs::remove_dir_all(&staging);
        }
        result
    }

    pub fn operation_ids(&self) -> Result<Vec<Uuid>> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(self.operations_dir())? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            if let Some(value) = entry
                .file_name()
                .to_str()
                .and_then(|v| Uuid::parse_str(v).ok())
            {
                ids.push(value);
            }
        }
        Ok(ids)
    }

    pub fn load_record(&self, operation_id: Uuid) -> Result<UploadRecord> {
        read_json(&self.operation_dir(operation_id).join(RECORD_FILE))
    }

    pub fn load_private(&self, operation_id: Uuid) -> Result<PrivateUploadContext> {
        read_json(&self.operation_dir(operation_id).join(PRIVATE_FILE))
    }

    pub fn save_record(&self, record: &UploadRecord) -> Result<()> {
        atomic_write_json(
            &self.operation_dir(record.operation_id).join(RECORD_FILE),
            record,
        )
    }

    pub fn save_private(&self, operation_id: Uuid, private: &PrivateUploadContext) -> Result<()> {
        atomic_write_json(
            &self.operation_dir(operation_id).join(PRIVATE_FILE),
            private,
        )
    }

    pub fn content_path(&self, operation_id: Uuid) -> PathBuf {
        self.operation_dir(operation_id).join(CONTENT_FILE)
    }

    pub fn verify_content(&self, record: &UploadRecord) -> Result<()> {
        let path = self.content_path(record.operation_id);
        let mut input = File::open(path)?;
        let mut digest = Sha256::new();
        let mut length = 0_u64;
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
            length += read as u64;
        }
        if length != record.content_length
            || format!("{:x}", digest.finalize()) != record.checksum_sha256
        {
            bail!("accepted upload content checksum does not match its durable receipt");
        }
        Ok(())
    }

    pub fn try_lock(&self, operation_id: Uuid) -> Result<Option<File>> {
        let path = self.operation_dir(operation_id).join(LOCK_FILE);
        let file = private_open(&path)?;
        match file.try_lock() {
            Ok(()) => Ok(Some(file)),
            Err(std::fs::TryLockError::WouldBlock) => Ok(None),
            Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
        }
    }

    pub fn quarantine_content(&self, operation_id: Uuid) -> Result<()> {
        let source = self.content_path(operation_id);
        if !source.exists() {
            return Ok(());
        }
        let destination = self
            .quarantine_dir()
            .join(format!("{operation_id}.content"));
        fs::rename(source, destination)?;
        sync_dir(&self.operation_dir(operation_id))?;
        sync_dir(&self.quarantine_dir())?;
        Ok(())
    }

    pub fn delete_private_data(&self, operation_id: Uuid) -> Result<()> {
        let operation = self.operation_dir(operation_id);
        for name in [CONTENT_FILE, PRIVATE_FILE] {
            match fs::remove_file(operation.join(name)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        sync_dir(&operation)?;
        Ok(())
    }

    pub fn remove_operation(&self, operation_id: Uuid) -> Result<()> {
        match fs::remove_dir_all(self.operation_dir(operation_id)) {
            Ok(()) => sync_dir(&self.operations_dir()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub fn maintain(&self, now: DateTime<Utc>) -> Result<Vec<SourceProtocol>> {
        let mut expired = Vec::new();
        for operation_id in self.operation_ids()? {
            let mut remove = false;
            {
                let Some(_lock) = self.try_lock(operation_id)? else {
                    continue;
                };
                let mut record = self.load_record(operation_id)?;
                if matches!(
                    record.state,
                    UploadState::CallbackConfirmed | UploadState::Failed
                ) && record
                    .expires_at
                    .is_some_and(|expires_at| expires_at <= now)
                {
                    if record.state == UploadState::Failed {
                        self.delete_quarantine(operation_id)?;
                    }
                    self.delete_private_data(operation_id)?;
                    record.state = UploadState::Expired;
                    record.updated_at = now;
                    record.expires_at =
                        Some(now + chrono::Duration::days(EXPIRED_RECEIPT_RETENTION_DAYS));
                    self.save_record(&record)?;
                    expired.push(record.source_protocol);
                } else if record.state == UploadState::Expired
                    && record
                        .expires_at
                        .is_some_and(|expires_at| expires_at <= now)
                {
                    remove = true;
                }
            }
            if remove {
                self.remove_operation(operation_id)?;
                self.delete_quarantine(operation_id)?;
            }
        }
        Ok(expired)
    }

    fn operation_dir(&self, operation_id: Uuid) -> PathBuf {
        self.operations_dir().join(operation_id.to_string())
    }

    fn delete_quarantine(&self, operation_id: Uuid) -> Result<()> {
        match fs::remove_file(
            self.quarantine_dir()
                .join(format!("{operation_id}.content")),
        ) {
            Ok(()) => sync_dir(&self.quarantine_dir()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn operations_dir(&self) -> PathBuf {
        self.root.join("operations")
    }

    fn staging_dir(&self) -> PathBuf {
        self.root.join("staging")
    }

    fn quarantine_dir(&self) -> PathBuf {
        self.root.join("quarantine")
    }

    fn clear_incomplete_staging(&self) -> Result<()> {
        for entry in fs::read_dir(self.staging_dir())? {
            let entry = entry?;
            let path = entry.path();
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(path)?;
            } else {
                fs::remove_file(path)?;
            }
        }
        sync_dir(&self.staging_dir())
    }
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path)?;
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn write_json_new(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut file = private_file(path)?;
    serde_json::to_writer(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn atomic_write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("JSON path has no parent")?;
    let temporary = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name().unwrap().to_string_lossy(),
        Uuid::new_v4()
    ));
    let result = (|| {
        write_json_new(&temporary, value)?;
        fs::rename(&temporary, path)?;
        sync_dir(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn private_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    private_options(&mut options);
    Ok(options.open(path)?)
}

fn private_open(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    private_options(&mut options);
    Ok(options.open(path)?)
}

#[cfg(unix)]
fn private_options(options: &mut OpenOptions) {
    use std::os::unix::fs::OpenOptionsExt;
    options.mode(0o600);
}

#[cfg(not(unix))]
fn private_options(_options: &mut OpenOptions) {}

#[cfg(unix)]
fn set_private_dir(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_dir(_path: &Path) -> Result<()> {
    Ok(())
}

fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{Tenant, UploadCredentials};

    fn session() -> SessionData {
        SessionData {
            tenant: Tenant {
                id: 7,
                name: "Test".into(),
                ftp_username: "photographer".into(),
                api_key: "not-serialized-here".into(),
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

    #[test]
    fn acceptance_atomically_persists_bytes_metadata_and_private_context() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.jpg");
        fs::write(&source, b"durable-media").unwrap();
        let spool = Spool::open(temp.path().join("spool")).unwrap();

        let record = spool
            .accept(&session(), SourceProtocol::Http, "camera.jpg", &source)
            .unwrap();

        assert_eq!(record.state, UploadState::Accepted);
        assert_eq!(record.content_length, 13);
        assert_eq!(record.content_type, "image/jpeg");
        assert_eq!(spool.operation_ids().unwrap(), vec![record.operation_id]);
        assert_eq!(
            spool
                .load_record(record.operation_id)
                .unwrap()
                .checksum_sha256,
            record.checksum_sha256
        );
        assert_eq!(
            spool
                .load_private(record.operation_id)
                .unwrap()
                .validation_key
                .as_deref(),
            Some("client-secret")
        );
        spool.verify_content(&record).unwrap();
        let serialized =
            fs::read_to_string(spool.operation_dir(record.operation_id).join(RECORD_FILE)).unwrap();
        assert!(!serialized.contains("client-secret"));
        assert!(!serialized.contains("not-serialized-here"));
    }

    #[test]
    fn checksum_verification_detects_corrupt_accepted_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        fs::write(&source, b"original").unwrap();
        let spool = Spool::open(temp.path().join("spool")).unwrap();
        let record = spool
            .accept(&session(), SourceProtocol::Ftp, "source.bin", &source)
            .unwrap();
        fs::write(spool.content_path(record.operation_id), b"corrupt").unwrap();
        assert!(spool.verify_content(&record).is_err());
    }

    #[test]
    fn operation_lock_excludes_duplicate_execution() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        fs::write(&source, b"durable-media").unwrap();
        let spool = Spool::open(temp.path().join("spool")).unwrap();
        let record = spool
            .accept(&session(), SourceProtocol::Http, "source.bin", &source)
            .unwrap();

        let first = spool.try_lock(record.operation_id).unwrap().unwrap();
        assert!(spool.try_lock(record.operation_id).unwrap().is_none());
        drop(first);
        assert!(spool.try_lock(record.operation_id).unwrap().is_some());
    }

    #[test]
    fn startup_discards_only_never_accepted_staging_entries() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("spool");
        let spool = Spool::open(root.clone()).unwrap();
        fs::create_dir(spool.staging_dir().join("interrupted.part")).unwrap();
        drop(spool);

        let reopened = Spool::open(root).unwrap();
        assert_eq!(fs::read_dir(reopened.staging_dir()).unwrap().count(), 0);
    }

    #[test]
    fn terminal_receipts_expire_then_are_removed_on_the_retention_schedule() {
        let temp = tempfile::tempdir().unwrap();
        let source = temp.path().join("source.bin");
        fs::write(&source, b"terminal").unwrap();
        let spool = Spool::open(temp.path().join("spool")).unwrap();
        let mut record = spool
            .accept(&session(), SourceProtocol::Http, "source.bin", &source)
            .unwrap();
        let now = Utc::now();
        record.state = UploadState::CallbackConfirmed;
        record.expires_at = Some(now - chrono::Duration::seconds(1));
        spool.save_record(&record).unwrap();

        assert_eq!(spool.maintain(now).unwrap(), vec![SourceProtocol::Http]);
        let expired = spool.load_record(record.operation_id).unwrap();
        assert_eq!(expired.state, UploadState::Expired);
        assert!(!spool.content_path(record.operation_id).exists());

        spool
            .maintain(now + chrono::Duration::days(EXPIRED_RECEIPT_RETENTION_DAYS + 1))
            .unwrap();
        assert!(spool.load_record(record.operation_id).is_err());
    }
}
