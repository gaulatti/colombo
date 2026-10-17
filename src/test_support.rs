use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
    routing::any,
};
use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::{
    config::Config,
    domain::{SessionData, Tenant, UploadCredentials},
};

pub fn config() -> Config {
    Config {
        http_port: 0,
        ftp_enabled: false,
        ftp_port: 0,
        passive_ports: 60000..=60001,
        passive_external_address: None,
        ftps_certificate_path: None,
        ftps_private_key_path: None,
        database_url: database_url(),
        database_user: "colombo".into(),
        database_password: "colombo".into(),
        migrations_enabled: true,
        master_password: None,
        metrics_token: None,
        build_version: "test".into(),
        spool_path: std::env::temp_dir().join("colombo-test"),
        ftp_root: std::env::temp_dir().join("colombo-test-ftp"),
    }
}

pub fn database_url() -> String {
    std::env::var("COLOMBO_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://colombo:colombo@127.0.0.1:55436/colombo".into())
}

pub async fn database() -> PgPool {
    struct TestLogger;
    impl log::Log for TestLogger {
        fn enabled(&self, _: &log::Metadata<'_>) -> bool {
            true
        }
        fn log(&self, record: &log::Record<'_>) {
            let _ = record.args().to_string();
        }
        fn flush(&self) {}
    }
    static LOGGER: TestLogger = TestLogger;
    static LOGGING: std::sync::Once = std::sync::Once::new();
    LOGGING.call_once(|| {
        log::set_logger(&LOGGER).unwrap();
        log::set_max_level(log::LevelFilter::Info);
        log::logger().flush();
    });
    // The identifier contains only a fixed prefix and a generated UUID, never external input.
    let schema = format!("test_{}", uuid::Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .connect(&database_url())
        .await
        .expect("start the documented disposable test PostgreSQL service");
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .after_connect(move |connection, _| {
            let query = format!("SET search_path TO {schema}");
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(query))
                    .execute(connection)
                    .await?;
                Ok(())
            })
        })
        .connect(&database_url())
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    pool
}

pub fn session(url: &str) -> SessionData {
    SessionData {
        tenant: Tenant {
            id: 1,
            name: "Test tenant".into(),
            ftp_username: "photographer".into(),
            api_key: "fictional-api-key".into(),
            validation_endpoint: format!("{url}/validate"),
            photo_endpoint: format!("{url}/photo"),
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
        validation_key: Some("fictional-client-key".into()),
    }
}

pub async fn seed(pool: &PgPool, session: &SessionData) {
    sqlx::query("INSERT INTO tenants (id, name, ftp_username, api_key, validation_endpoint, photo_endpoint) VALUES ($1,$2,$3,$4,$5,$6)")
        .bind(session.tenant.id).bind(&session.tenant.name).bind(&session.tenant.ftp_username).bind(&session.tenant.api_key)
        .bind(&session.tenant.validation_endpoint).bind(&session.tenant.photo_endpoint).execute(pool).await.unwrap();
}

pub fn validation(session: &SessionData) -> String {
    serde_json::json!({"assignmentId":session.assignment_id,"upload":session.upload}).to_string()
}

#[derive(Clone, Debug)]
pub struct Request {
    pub uri: String,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}
struct MockState {
    replies: VecDeque<(u16, String)>,
    requests: Vec<Request>,
}
pub struct Mock {
    pub url: String,
    state: Arc<Mutex<MockState>>,
    task: tokio::task::JoinHandle<()>,
}
impl Mock {
    pub async fn new(replies: Vec<(u16, String)>) -> Self {
        assert!(!replies.is_empty());
        let state = Arc::new(Mutex::new(MockState {
            replies: replies.into(),
            requests: vec![],
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        async fn respond(
            State(state): State<Arc<Mutex<MockState>>>,
            uri: Uri,
            headers: HeaderMap,
            body: Bytes,
        ) -> (StatusCode, String) {
            let mut state = state.lock().unwrap();
            if uri.path() == "/__control" {
                state.replies = serde_json::from_slice::<VecDeque<(u16, String)>>(&body).unwrap();
                assert!(!state.replies.is_empty());
                return (StatusCode::OK, String::new());
            }
            state.requests.push(Request {
                uri: uri.to_string(),
                headers,
                body: body.to_vec(),
            });
            let (status, body) = if state.replies.len() > 1 {
                state.replies.pop_front().unwrap()
            } else {
                state.replies.front().unwrap().clone()
            };
            (StatusCode::from_u16(status).unwrap(), body)
        }
        let router = Router::new()
            .fallback(any(respond))
            .with_state(state.clone());
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self { url, state, task }
    }
    pub fn requests(&self) -> Vec<Request> {
        self.state.lock().unwrap().requests.clone()
    }
}
impl Drop for Mock {
    fn drop(&mut self) {
        self.task.abort();
    }
}
