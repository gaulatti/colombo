use std::{
    fmt::{Debug, Display},
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use dashmap::DashMap;
use libunftp::{ServerBuilder, options::PassiveHost};
use sqlx::PgPool;
use unftp_core::{
    auth::{
        AuthenticationError, Authenticator, Credentials, Principal, UserDetail, UserDetailError,
        UserDetailProvider,
    },
    storage::{
        Error as StorageError, ErrorKind, Fileinfo, Result as StorageResult, StorageBackend,
    },
};
use unftp_sbe_fs::{Filesystem, Meta};
use uuid::Uuid;

use crate::{
    cms::{CmsClient, ValidationError},
    config::Config,
    db,
    domain::SessionData,
    metrics::Metrics,
    upload::{SessionHandle, UploadService},
};

#[derive(Clone)]
pub struct ColomboUser {
    username: String,
    lease: Arc<SessionLease>,
}

impl Display for ColomboUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.username)
    }
}
impl Debug for ColomboUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColomboUser")
            .field("username", &self.username)
            .finish_non_exhaustive()
    }
}
impl UserDetail for ColomboUser {}

struct SessionLease {
    session: Arc<SessionHandle>,
    metrics: Arc<Metrics>,
}
impl Drop for SessionLease {
    fn drop(&mut self) {
        self.metrics.ftp_sessions.dec();
        self.metrics
            .ftp_connection_events
            .with_label_values(&["disconnect"])
            .inc();
    }
}

struct FtpAuth {
    pool: PgPool,
    cms: CmsClient,
    master_password: Option<String>,
    pending: Arc<DashMap<String, ColomboUser>>,
    metrics: Arc<Metrics>,
}

impl Debug for FtpAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtpAuth").finish_non_exhaustive()
    }
}

#[async_trait]
impl Authenticator for FtpAuth {
    async fn authenticate(
        &self,
        username: &str,
        creds: &Credentials,
    ) -> Result<Principal, AuthenticationError> {
        let password = match creds.password.as_deref() {
            Some(password) => password,
            None => {
                self.metrics
                    .authentication_attempts
                    .with_label_values(&["ftp", "unsupported"])
                    .inc();
                return Err(AuthenticationError::BadPassword);
            }
        };
        let tenant = match db::tenant_by_username(&self.pool, username).await {
            Ok(Some(tenant)) => tenant,
            Ok(None) => {
                self.metrics
                    .authentication_attempts
                    .with_label_values(&["ftp", "unknown_user"])
                    .inc();
                return Err(AuthenticationError::BadUser);
            }
            Err(_) => {
                self.metrics
                    .authentication_attempts
                    .with_label_values(&["ftp", "unavailable"])
                    .inc();
                return Err(AuthenticationError::new("tenant lookup failed"));
            }
        };
        let session = if self
            .master_password
            .as_deref()
            .is_some_and(|master| master == password)
        {
            self.metrics
                .authentication_attempts
                .with_label_values(&["ftp", "support_bypass"])
                .inc();
            tracing::warn!(username, "master-password FTP support bypass used");
            SessionData {
                tenant,
                assignment_id: format!("support-assignment-{username}"),
                upload: None,
                validation_key: None,
            }
        } else {
            match self
                .cms
                .validate(&tenant, password, "validation_login")
                .await
            {
                Ok(value) => value,
                Err(ValidationError::Denied) => {
                    self.metrics
                        .authentication_attempts
                        .with_label_values(&["ftp", "denied"])
                        .inc();
                    return Err(AuthenticationError::BadPassword);
                }
                Err(_) => {
                    self.metrics
                        .authentication_attempts
                        .with_label_values(&["ftp", "unavailable"])
                        .inc();
                    return Err(AuthenticationError::new("CMS validation failed"));
                }
            }
        };
        if self.master_password.as_deref() != Some(password) {
            self.metrics
                .authentication_attempts
                .with_label_values(&["ftp", "success"])
                .inc();
        }
        let token = Uuid::new_v4().to_string();
        let user = ColomboUser {
            username: username.to_owned(),
            lease: Arc::new(SessionLease {
                session: SessionHandle::new(session),
                metrics: self.metrics.clone(),
            }),
        };
        self.pending.insert(token.clone(), user);
        Ok(Principal { username: token })
    }
}

struct UserProvider {
    pending: Arc<DashMap<String, ColomboUser>>,
    metrics: Arc<Metrics>,
}
impl Debug for UserProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UserProvider").finish_non_exhaustive()
    }
}

#[async_trait]
impl UserDetailProvider for UserProvider {
    type User = ColomboUser;
    async fn provide_user_detail(
        &self,
        principal: &Principal,
    ) -> Result<Self::User, UserDetailError> {
        let (_, user) = self.pending.remove(&principal.username).ok_or_else(|| {
            UserDetailError::UserNotFound {
                username: principal.username.clone(),
            }
        })?;
        self.metrics.ftp_sessions.inc();
        self.metrics
            .ftp_connection_events
            .with_label_values(&["connect"])
            .inc();
        Ok(user)
    }
}

pub struct ColomboStorage {
    inner: Filesystem,
    root: PathBuf,
    uploads: Arc<UploadService>,
}

impl Debug for ColomboStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColomboStorage")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl ColomboStorage {
    fn new(root: PathBuf, uploads: Arc<UploadService>) -> std::io::Result<Self> {
        Ok(Self {
            inner: Filesystem::new(root.clone())?,
            root,
            uploads,
        })
    }
}

#[async_trait]
impl StorageBackend<ColomboUser> for ColomboStorage {
    type Metadata = Meta;
    fn enter(&mut self, user: &ColomboUser) -> std::io::Result<()> {
        self.inner.enter(user)
    }
    fn supported_features(&self) -> u32 {
        <Filesystem as StorageBackend<ColomboUser>>::supported_features(&self.inner)
    }
    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        path: P,
    ) -> StorageResult<Self::Metadata> {
        self.inner.metadata(user, path).await
    }
    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        path: P,
    ) -> StorageResult<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
        self.inner.list(user, path).await
    }
    async fn get<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        path: P,
        start: u64,
    ) -> StorageResult<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>> {
        self.inner.get(user, path, start).await
    }
    async fn put<P, R>(
        &self,
        user: &ColomboUser,
        input: R,
        path: P,
        start: u64,
    ) -> StorageResult<u64>
    where
        P: AsRef<Path> + Send + Debug,
        R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
    {
        let relative = path
            .as_ref()
            .strip_prefix("/")
            .unwrap_or(path.as_ref())
            .to_path_buf();
        let bytes = self.inner.put(user, input, &relative, start).await?;
        let filename = relative
            .file_name()
            .and_then(|v| v.to_str())
            .unwrap_or("upload")
            .to_owned();
        let operation_id = self
            .uploads
            .accept_ftp(
                user.lease.session.clone(),
                filename,
                self.root.join(relative),
            )
            .await
            .map_err(|error| StorageError::new(ErrorKind::LocalError, error))?;
        tracing::info!(%operation_id, "FTP upload durably accepted");
        Ok(bytes)
    }
    async fn del<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.del(user, path).await
    }
    async fn mkd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.mkd(user, path).await
    }
    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        from: P,
        to: P,
    ) -> StorageResult<()> {
        self.inner.rename(user, from, to).await
    }
    async fn rmd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.rmd(user, path).await
    }
    async fn cwd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &ColomboUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.cwd(user, path).await
    }
}

// The builder first checks the storage type against DefaultUser before the custom provider changes it.
#[async_trait]
impl StorageBackend<unftp_core::auth::DefaultUser> for ColomboStorage {
    type Metadata = Meta;
    async fn metadata<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        path: P,
    ) -> StorageResult<Self::Metadata> {
        self.inner.metadata(user, path).await
    }
    async fn list<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        path: P,
    ) -> StorageResult<Vec<Fileinfo<PathBuf, Self::Metadata>>> {
        self.inner.list(user, path).await
    }
    async fn get<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        path: P,
        start: u64,
    ) -> StorageResult<Box<dyn tokio::io::AsyncRead + Send + Sync + Unpin>> {
        self.inner.get(user, path, start).await
    }
    async fn put<P, R>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        input: R,
        path: P,
        start: u64,
    ) -> StorageResult<u64>
    where
        P: AsRef<Path> + Send + Debug,
        R: tokio::io::AsyncRead + Send + Sync + Unpin + 'static,
    {
        self.inner.put(user, input, path, start).await
    }
    async fn del<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.del(user, path).await
    }
    async fn mkd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.mkd(user, path).await
    }
    async fn rename<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        from: P,
        to: P,
    ) -> StorageResult<()> {
        self.inner.rename(user, from, to).await
    }
    async fn rmd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.rmd(user, path).await
    }
    async fn cwd<P: AsRef<Path> + Send + Debug>(
        &self,
        user: &unftp_core::auth::DefaultUser,
        path: P,
    ) -> StorageResult<()> {
        self.inner.cwd(user, path).await
    }
}

pub async fn serve(
    config: Config,
    pool: PgPool,
    cms: CmsClient,
    uploads: Arc<UploadService>,
    metrics: Arc<Metrics>,
) -> anyhow::Result<()> {
    let pending = Arc::new(DashMap::new());
    let root = config.ftp_root.clone();
    let storage_uploads = uploads.clone();
    let auth = Arc::new(FtpAuth {
        pool,
        cms,
        master_password: config.master_password.clone(),
        pending: pending.clone(),
        metrics: metrics.clone(),
    });
    let provider = Arc::new(UserProvider { pending, metrics });
    let mut builder = ServerBuilder::new(Box::new(move || {
        ColomboStorage::new(root.clone(), storage_uploads.clone())
            .expect("FTP root must remain available")
    }))
    .user_detail_provider(provider)
    .authenticator(auth)
    .passive_ports(config.passive_ports.clone())
    .pooled_listener_mode()
    .idle_session_timeout(300)
    .metrics();
    if let Some(host) = config.passive_external_address.as_deref() {
        builder = builder.passive_host(PassiveHost::Dns(host.to_owned()));
    }
    if let (Some(cert), Some(key)) = (&config.ftps_certificate_path, &config.ftps_private_key_path)
    {
        builder = builder.ftps(cert, key);
    }
    let server = builder.build()?;
    tracing::info!(
        port = config.ftp_port,
        ftps = config.ftps_certificate_path.is_some(),
        "FTP listener ready"
    );
    server
        .listen(format!("0.0.0.0:{}", config.ftp_port))
        .await?;
    Ok(())
}
