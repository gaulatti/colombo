use axum::{body::Body, http::Request};
use colombo::{
    cms::CmsClient,
    config::Config,
    domain::{SessionData, Tenant, UploadCredentials},
    http::{AppState, router},
    metrics::Metrics,
    upload::UploadService,
};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tower::ServiceExt;

struct Logger(Mutex<Vec<String>>);
impl log::Log for Logger {
    fn enabled(&self, _: &log::Metadata<'_>) -> bool {
        true
    }
    fn log(&self, record: &log::Record<'_>) {
        self.0.lock().unwrap().push(record.args().to_string());
    }
    fn flush(&self) {}
}
static LOGGER: Logger = Logger(Mutex::new(Vec::new()));

#[tokio::test]
async fn library_users_without_a_tracing_subscriber_receive_operational_logs() {
    log::set_logger(&LOGGER).unwrap();
    log::set_max_level(log::LevelFilter::Info);
    let temp = tempfile::tempdir().unwrap();
    let database = std::env::var("COLOMBO_TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://colombo:colombo@127.0.0.1:55436/colombo".into());
    let schema = format!("logging_{}", uuid::Uuid::new_v4().simple());
    let admin = sqlx::PgPool::connect(&database).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let pool_schema = schema.clone();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .after_connect(move |conn, _| {
            let sql = format!("SET search_path TO {pool_schema}");
            Box::pin(async move {
                sqlx::query(sqlx::AssertSqlSafe(sql)).execute(conn).await?;
                Ok(())
            })
        })
        .connect(&database)
        .await
        .unwrap();
    sqlx::migrate!("./migrations").run(&pool).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let upload = UploadCredentials {
        access_key_id: "access".into(),
        secret_access_key: "secret".into(),
        session_token: "token".into(),
        region: "us-east-1".into(),
        bucket: "uploads".into(),
        key_prefix: "logging".into(),
        expires_at: "2099-01-01T00:00:00Z".into(),
        naming_policy: None,
        sequence_endpoint: None,
    };
    let body = serde_json::json!({"assignmentId":"logging","upload":upload});
    let mock = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().fallback(move || {
                let body = body.clone();
                async move { axum::Json(body) }
            }),
        )
        .await
        .unwrap()
    });
    sqlx::query("INSERT INTO tenants(id,name,ftp_username,api_key,validation_endpoint,photo_endpoint) VALUES(1,'Log fixture','photographer','fictional-key',$1,$1)").bind(&url).execute(&pool).await.unwrap();
    let metrics = Metrics::new("log-adapter").unwrap();
    let cms = CmsClient::new(metrics.clone()).unwrap();
    let spool = temp.path().join("spool");
    let service = UploadService::new(pool.clone(), cms.clone(), metrics.clone(), spool.clone())
        .await
        .unwrap();
    let source = temp.path().join("file");
    std::fs::write(&source, b"content").unwrap();
    let session = SessionData {
        tenant: Tenant {
            id: -1,
            name: "Deleted tenant".into(),
            ftp_username: "missing".into(),
            api_key: "fictional".into(),
            validation_endpoint: url.clone(),
            photo_endpoint: url,
        },
        assignment_id: "logging".into(),
        upload: Some(upload),
        validation_key: Some("fictional".into()),
    };
    // An absent tenant prevents this accepted fixture from reaching any S3 endpoint.
    service
        .accept_http(session, "source".into(), source)
        .await
        .unwrap();
    let state = AppState {
        pool: pool.clone(),
        cms: cms.clone(),
        uploads: service.clone(),
        metrics: metrics.clone(),
        metrics_token: Arc::from("logging-metrics-token"),
    };
    for body in [
        "--test--\r\n",
        "--test\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\n\r\n--test--\r\n",
        "--test\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nunclosed",
    ] {
        let response = router(state.clone())
            .oneshot(
                Request::post("/upload")
                    .header("X-Colombo-Username", "photographer")
                    .header("X-Colombo-Password", "key")
                    .header("content-type", "multipart/form-data; boundary=test")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
    }
    std::fs::remove_dir(spool.join("staging")).unwrap();
    let response=router(state.clone()).oneshot(Request::post("/upload").header("X-Colombo-Username","photographer").header("X-Colombo-Password","key").header("content-type","multipart/form-data; boundary=test").body(Body::from("--test\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\ncontent\r\n--test--\r\n")).unwrap()).await.unwrap();
    assert_eq!(response.status(), 500);
    std::fs::create_dir(spool.join("staging")).unwrap();
    let occupied = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
    let config = Config {
        http_port: 0,
        ftp_enabled: true,
        ftp_port: occupied.local_addr().unwrap().port(),
        passive_ports: 0..=0,
        passive_external_address: None,
        ftps_certificate_path: None,
        ftps_private_key_path: None,
        database_url: database,
        database_user: "colombo".into(),
        database_password: "colombo".into(),
        migrations_enabled: true,
        master_password: None,
        metrics_token: None,
        build_version: "test".into(),
        ftp_root: spool.join("ftp-incoming"),
        spool_path: spool,
    };
    let _ = tokio::time::timeout(
        Duration::from_secs(1),
        colombo::ftp::serve(config, pool.clone(), cms, service, metrics),
    )
    .await;
    pool.close().await;
    for uri in ["/upload", "/uploads/00000000-0000-4000-8000-000000000000"] {
        let mut request = Request::builder()
            .uri(uri)
            .header("X-Colombo-Username", "photographer")
            .header("X-Colombo-Password", "key")
            .header("content-type", "multipart/form-data; boundary=test");
        request = request.method(if uri == "/upload" { "POST" } else { "GET" });
        assert_eq!(
            router(state.clone())
                .oneshot(request.body(Body::from("--test--\r\n")).unwrap())
                .await
                .unwrap()
                .status(),
            500
        );
    }
    let messages = LOGGER.0.lock().unwrap();
    assert!(
        messages
            .iter()
            .any(|m| m.contains("upload durably accepted"))
    );
    assert!(messages.iter().any(|m| m.contains("FTP listener ready")));
    drop(messages);
    mock.abort();
    log::logger().flush();
    sqlx::query(sqlx::AssertSqlSafe(format!("DROP SCHEMA {schema} CASCADE")))
        .execute(&admin)
        .await
        .unwrap();
    admin.close().await;
}
