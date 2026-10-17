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
    .shutdown_indicator(async {
        let _ = tokio::signal::ctrl_c().await;
        libunftp::options::Shutdown::new()
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        spool::Spool,
        test_support::{self, Mock},
    };
    use tokio::io::AsyncReadExt;

    fn auth(pool: PgPool, metrics: Arc<Metrics>) -> FtpAuth {
        FtpAuth {
            pool,
            cms: CmsClient::new(metrics.clone()).unwrap(),
            master_password: Some("support-password".into()),
            pending: Arc::new(DashMap::new()),
            metrics,
        }
    }

    #[tokio::test]
    async fn authentication_denials_and_dependency_failures_never_create_users() {
        let metrics = Metrics::new("test").unwrap();
        let pool = test_support::database().await;
        let auth = auth(pool.clone(), metrics.clone());
        assert!(format!("{auth:?}").contains("FtpAuth"));
        let mut creds = Credentials::from("key");
        creds.password = None;
        assert!(matches!(
            auth.authenticate("photographer", &creds).await,
            Err(AuthenticationError::BadPassword)
        ));
        assert!(matches!(
            auth.authenticate("unknown", &Credentials::from("key"))
                .await,
            Err(AuthenticationError::BadUser)
        ));
        pool.close().await;
        assert!(
            auth.authenticate("photographer", &Credentials::from("key"))
                .await
                .is_err()
        );
        assert!(auth.pending.is_empty());
        for status in [401, 503] {
            let mock = Mock::new(vec![(status, "{}".into())]).await;
            let pool = test_support::database().await;
            test_support::seed(&pool, &test_support::session(&mock.url)).await;
            let auth = super::tests::auth(pool, metrics.clone());
            assert!(
                auth.authenticate("photographer", &Credentials::from("key"))
                    .await
                    .is_err()
            );
            assert!(auth.pending.is_empty());
        }
        let text = String::from_utf8(metrics.encode().unwrap()).unwrap();
        for result in ["unsupported", "unknown_user", "unavailable", "denied"] {
            assert!(text.contains(&format!("result=\"{result}\",source=\"ftp\"")));
        }
    }

    #[tokio::test]
    async fn connections_get_distinct_one_use_principals_and_balanced_session_leases() {
        let mock = Mock::new(vec![(
            200,
            test_support::validation(&test_support::session("http://unused")),
        )])
        .await;
        let pool = test_support::database().await;
        test_support::seed(&pool, &test_support::session(&mock.url)).await;
        let metrics = Metrics::new("test").unwrap();
        let auth = auth(pool, metrics.clone());
        let provider = UserProvider {
            pending: auth.pending.clone(),
            metrics: metrics.clone(),
        };
        assert!(format!("{provider:?}").contains("UserProvider"));
        let first = auth
            .authenticate("photographer", &Credentials::from("key"))
            .await
            .unwrap();
        let second = auth
            .authenticate("photographer", &Credentials::from("key"))
            .await
            .unwrap();
        assert_ne!(first.username, second.username);
        let first_user = provider.provide_user_detail(&first).await.unwrap();
        let second_user = provider.provide_user_detail(&second).await.unwrap();
        assert_eq!(first_user.to_string(), "photographer");
        assert!(format!("{first_user:?}").contains("photographer"));
        assert!(!Arc::ptr_eq(
            &first_user.lease.session,
            &second_user.lease.session
        ));
        assert!(provider.provide_user_detail(&first).await.is_err());
        assert_eq!(metrics.ftp_sessions.get(), 2);
        let clone = first_user.clone();
        drop(first_user);
        assert_eq!(metrics.ftp_sessions.get(), 2);
        drop(clone);
        assert_eq!(metrics.ftp_sessions.get(), 1);
        drop(second_user);
        assert_eq!(metrics.ftp_sessions.get(), 0);
        let support = auth
            .authenticate("photographer", &Credentials::from("support-password"))
            .await
            .unwrap();
        let user = provider.provide_user_detail(&support).await.unwrap();
        let snapshot = user.lease.session.snapshot().await;
        assert!(snapshot.upload.is_none());
        assert!(snapshot.validation_key.is_none());
        assert_eq!(snapshot.assignment_id, "support-assignment-photographer");
        drop(user);
        assert_eq!(metrics.ftp_sessions.get(), 0);
    }

    #[tokio::test]
    async fn storage_preserves_filesystem_contract_and_requires_durable_acceptance() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("spool");
        let spool = Arc::new(Spool::open(root.clone()).unwrap());
        let pool = test_support::database().await;
        let uploads = UploadService::isolated(pool, spool.clone());
        let metrics = Metrics::new("test").unwrap();
        metrics.ftp_sessions.inc();
        let user = ColomboUser {
            username: "photographer".into(),
            lease: Arc::new(SessionLease {
                session: SessionHandle::new(test_support::session("http://unused")),
                metrics,
            }),
        };
        let mut storage = ColomboStorage::new(root.join("ftp-incoming"), uploads).unwrap();
        assert!(format!("{storage:?}").contains("ColomboStorage"));
        <ColomboStorage as StorageBackend<ColomboUser>>::enter(&mut storage, &user).unwrap();
        assert_eq!(
            <ColomboStorage as StorageBackend<ColomboUser>>::supported_features(&storage),
            <Filesystem as StorageBackend<ColomboUser>>::supported_features(&storage.inner)
        );
        storage.mkd(&user, "folder").await.unwrap();
        storage.cwd(&user, "folder").await.unwrap();
        assert_eq!(
            storage
                .put(
                    &user,
                    std::io::Cursor::new(b"file bytes"),
                    "/folder/photo.jpg",
                    0
                )
                .await
                .unwrap(),
            10
        );
        assert_eq!(spool.operation_ids().unwrap().len(), 1);
        std::fs::write(root.join("ftp-incoming/folder/file.txt"), b"readable").unwrap();
        storage.metadata(&user, "folder/file.txt").await.unwrap();
        assert_eq!(storage.list(&user, "folder").await.unwrap().len(), 1);
        let mut file = storage.get(&user, "folder/file.txt", 0).await.unwrap();
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"readable");
        storage
            .rename(&user, "folder/file.txt", "folder/renamed.txt")
            .await
            .unwrap();
        storage.del(&user, "folder/renamed.txt").await.unwrap();
        storage.rmd(&user, "folder").await.unwrap();
        let default = unftp_core::auth::DefaultUser {};
        storage.mkd(&default, "default").await.unwrap();
        storage.cwd(&default, "default").await.unwrap();
        assert_eq!(
            storage
                .put(
                    &default,
                    std::io::Cursor::new(b"default bytes"),
                    "default/file",
                    0
                )
                .await
                .unwrap(),
            13
        );
        storage.metadata(&default, "default/file").await.unwrap();
        assert_eq!(storage.list(&default, "default").await.unwrap().len(), 1);
        let mut file = storage.get(&default, "default/file", 0).await.unwrap();
        let mut text = String::new();
        file.read_to_string(&mut text).await.unwrap();
        assert_eq!(text, "default bytes");
        storage
            .rename(&default, "default/file", "default/renamed")
            .await
            .unwrap();
        storage.del(&default, "default/renamed").await.unwrap();
        storage.rmd(&default, "default").await.unwrap();
        user.lease.session.invalidate();
        assert!(
            storage
                .put(
                    &user,
                    std::io::Cursor::new(b"must not accept"),
                    "rejected.jpg",
                    0
                )
                .await
                .is_err()
        );
        assert!(ColomboStorage::new(temp.path().join("missing"), storage.uploads.clone()).is_err());
    }
}
