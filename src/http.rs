use std::{path::Path, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, Multipart, Path as AxumPath, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Serialize;
use sqlx::PgPool;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tower_http::{catch_panic::CatchPanicLayer, trace::TraceLayer};
use uuid::Uuid;

use crate::{
    cms::{CmsClient, ValidationError},
    db,
    metrics::Metrics,
    spool::UploadState,
    upload::UploadService,
};

const MAX_UPLOAD_BYTES: usize = 100 * 1024 * 1024;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    pub cms: CmsClient,
    pub uploads: Arc<UploadService>,
    pub metrics: Arc<Metrics>,
    pub metrics_token: Arc<str>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/upload", post(upload))
        .route("/uploads/{operation_id}", get(upload_status))
        .route("/actuator/health", get(health))
        .route("/actuator/prometheus", get(prometheus))
        .fallback(|| async { StatusCode::UNAUTHORIZED })
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(CatchPanicLayer::new())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn root() -> Response {
    Response::builder()
        .status(StatusCode::FOUND)
        .header(
            header::LOCATION,
            "https://www.youtube.com/watch?v=KieE_MLv-ZY",
        )
        .body(Body::empty())
        .expect("static redirect response")
}

pub async fn serve(port: u16, state: AppState) -> anyhow::Result<()> {
    async {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        tracing::info!(port, "HTTP listener ready");
        axum::serve(listener, router(state))
            .with_graceful_shutdown(shutdown_signal())
            .await
    }
    .await
    .map_err(anyhow::Error::from)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Serialize)]
struct Accepted {
    status: &'static str,
    assignment_id: String,
    operation_id: Uuid,
}

async fn upload(
    State(state): State<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Response {
    let username = match required_header(&headers, "X-Colombo-Username") {
        Some(value) if !value.trim().is_empty() => value,
        _ => {
            return error(
                StatusCode::BAD_REQUEST,
                "X-Colombo-Username header is required",
            );
        }
    };
    let password = match required_header(&headers, "X-Colombo-Password") {
        Some(value) => value,
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                "X-Colombo-Password header is required",
            );
        }
    };
    let tenant = match db::tenant_by_username(&state.pool, &username).await {
        Ok(Some(value)) => value,
        Ok(None) => return error(StatusCode::NOT_FOUND, "No tenant registered for username"),
        Err(err) => {
            tracing::error!(error = %err, "tenant lookup failed");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "Tenant lookup failed");
        }
    };
    let session = match state
        .cms
        .validate(&tenant, &password, "validation_upload")
        .await
    {
        Ok(value) => {
            state
                .metrics
                .authentication_attempts
                .with_label_values(&["http_upload", "success"])
                .inc();
            value
        }
        Err(ValidationError::Denied) => {
            state
                .metrics
                .authentication_attempts
                .with_label_values(&["http_upload", "denied"])
                .inc();
            return error(StatusCode::UNAUTHORIZED, "Invalid credentials");
        }
        Err(err) => {
            state
                .metrics
                .authentication_attempts
                .with_label_values(&["http_upload", "unavailable"])
                .inc();
            tracing::warn!(error = %err, "HTTP upload authentication failed");
            return error(StatusCode::UNAUTHORIZED, "Invalid credentials");
        }
    };
    let mut accepted: Option<(String, tempfile::TempPath)> = None;
    loop {
        let field = match multipart.next_field().await {
            Ok(Some(value)) => value,
            Ok(None) => break,
            Err(err) => return UploadReadError::multipart(err).into_response(),
        };
        if field.name() != Some("file") || accepted.is_some() {
            continue;
        }
        let original = field
            .file_name()
            .filter(|v| !v.trim().is_empty())
            .and_then(|v| Path::new(v).file_name())
            .and_then(|v| v.to_str())
            .unwrap_or("upload")
            .to_owned();
        let temp = match temporary_upload(
            tempfile::Builder::new()
                .prefix("colombo-upload-")
                .tempfile(),
        ) {
            Ok(value) => value,
            Err(err) => return err.into_response(),
        };
        let (file, path) = temp.into_parts();
        let mut output = tokio::fs::File::from_std(file);
        if let Err(response) = copy_upload(field, &mut output, MAX_UPLOAD_BYTES).await {
            return response.into_response();
        }
        drop(output);
        accepted = Some((original, path));
    }
    let Some((original, path)) = accepted else {
        return error(
            StatusCode::BAD_REQUEST,
            "file part is required and must not be empty",
        );
    };
    let assignment_id = session.assignment_id.clone();
    let receipt = match state
        .uploads
        .accept_http(session, original, path.to_path_buf())
        .await
    {
        Ok(value) => value,
        Err(err) => {
            tracing::error!(error = %err, "durable HTTP upload acceptance failed");
            let _ = tokio::fs::remove_file(&path).await;
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to durably store uploaded file",
            );
        }
    };
    let _ = tokio::fs::remove_file(&path).await;
    (
        StatusCode::ACCEPTED,
        Json(Accepted {
            status: "accepted",
            assignment_id,
            operation_id: receipt.operation_id,
        }),
    )
        .into_response()
}

#[async_trait::async_trait]
trait DurableWrite: AsyncWrite + Unpin + Send + Sync {
    async fn sync(&self) -> std::io::Result<()>;
}
#[async_trait::async_trait]
impl DurableWrite for tokio::fs::File {
    async fn sync(&self) -> std::io::Result<()> {
        self.sync_all().await
    }
}

async fn copy_upload(
    mut field: axum::extract::multipart::Field<'_>,
    output: &mut dyn DurableWrite,
    limit: usize,
) -> Result<(), UploadReadError> {
    let mut total = 0usize;
    loop {
        match field.chunk().await {
            Ok(Some(chunk)) => {
                total += chunk.len();
                if total > limit {
                    return Err(UploadReadError::too_large());
                }
                if output.write_all(&chunk).await.is_err() {
                    return Err(UploadReadError {
                        status: StatusCode::INTERNAL_SERVER_ERROR,
                        message: "Failed to process uploaded file",
                    });
                }
            }
            Ok(None) => break,
            Err(err) => return Err(UploadReadError::multipart(err)),
        }
    }
    if total == 0 {
        return Err(UploadReadError {
            status: StatusCode::BAD_REQUEST,
            message: "file part is required and must not be empty",
        });
    }
    if output.sync().await.is_err() {
        return Err(UploadReadError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Failed to durably store uploaded file",
        });
    }
    Ok(())
}

struct UploadReadError {
    status: StatusCode,
    message: &'static str,
}

impl UploadReadError {
    fn too_large() -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: "Upload exceeds the size limit",
        }
    }

    fn multipart(err: axum::extract::multipart::MultipartError) -> Self {
        if err.status() == StatusCode::PAYLOAD_TOO_LARGE {
            Self::too_large()
        } else {
            Self {
                status: StatusCode::BAD_REQUEST,
                message: "Invalid multipart request",
            }
        }
    }
}

fn temporary_upload(
    result: std::io::Result<tempfile::NamedTempFile>,
) -> Result<tempfile::NamedTempFile, UploadReadError> {
    result.map_err(|err| {
        tracing::error!(error = %err, "temp upload creation failed");
        UploadReadError {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Failed to process uploaded file",
        }
    })
}
impl IntoResponse for UploadReadError {
    fn into_response(self) -> Response {
        error(self.status, self.message)
    }
}

async fn upload_status(
    State(state): State<AppState>,
    AxumPath(operation_id): AxumPath<String>,
    headers: HeaderMap,
) -> Response {
    let Some(username) =
        required_header(&headers, "X-Colombo-Username").filter(|value| !value.trim().is_empty())
    else {
        return error(
            StatusCode::BAD_REQUEST,
            "X-Colombo-Username header is required",
        );
    };
    let Some(password) = required_header(&headers, "X-Colombo-Password") else {
        return error(
            StatusCode::BAD_REQUEST,
            "X-Colombo-Password header is required",
        );
    };
    let tenant = match db::tenant_by_username(&state.pool, &username).await {
        Ok(Some(value)) => value,
        Ok(None) => return error(StatusCode::UNAUTHORIZED, "Invalid credentials"),
        Err(err) => {
            tracing::error!(error = %err, "status tenant lookup failed");
            return error(StatusCode::INTERNAL_SERVER_ERROR, "Tenant lookup failed");
        }
    };
    let authenticated = match state
        .cms
        .validate(&tenant, &password, "validation_status")
        .await
    {
        Ok(value) => value,
        Err(ValidationError::Denied) => {
            return error(StatusCode::UNAUTHORIZED, "Invalid credentials");
        }
        Err(err) => {
            tracing::warn!(error = %err, "upload status authentication failed");
            return error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Authentication unavailable",
            );
        }
    };
    let Ok(operation_id) = Uuid::parse_str(&operation_id) else {
        return error(StatusCode::NOT_FOUND, "Upload operation not found");
    };
    match state
        .uploads
        .receipt_for_assignment(operation_id, tenant.id, authenticated.assignment_id)
        .await
    {
        Ok(Some(receipt)) if receipt.state == UploadState::Expired => {
            (StatusCode::GONE, Json(receipt)).into_response()
        }
        Ok(Some(receipt)) => (StatusCode::OK, Json(receipt)).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "Upload operation not found"),
        Err(err) => {
            tracing::error!(error = %err, "upload receipt lookup failed");
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "Upload receipt lookup failed",
            )
        }
    }
}

fn required_header(headers: &HeaderMap, name: &'static str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

#[derive(Serialize)]
struct ErrorBody<'a> {
    error: &'a str,
}
fn error(status: StatusCode, message: &str) -> Response {
    (status, Json(ErrorBody { error: message })).into_response()
}

async fn health(State(state): State<AppState>) -> Response {
    if sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(&state.pool)
        .await
        .is_ok()
    {
        (StatusCode::OK, Json(serde_json::json!({"status":"UP"}))).into_response()
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"status":"DOWN"})),
        )
            .into_response()
    }
}

async fn prometheus(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let supplied = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !metrics_authorized(&state.metrics_token, supplied) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    metrics_response(state.metrics.encode())
}

fn metrics_response(encoded: anyhow::Result<Vec<u8>>) -> Response {
    match encoded {
        Ok(body) => Response::builder()
            .status(StatusCode::OK)
            .header(
                header::CONTENT_TYPE,
                "text/plain; version=0.0.4; charset=utf-8",
            )
            .body(Body::from(body))
            .unwrap(),
        Err(err) => {
            tracing::error!(error = %err, "metrics encoding failed");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn metrics_authorized(expected: &str, supplied: &str) -> bool {
    !expected.is_empty() && supplied.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        spool::{SourceProtocol, Spool},
        test_support::{self, Mock},
    };
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    struct Fixture {
        _temp: tempfile::TempDir,
        _mock: Mock,
        state: AppState,
        spool: Arc<Spool>,
    }
    impl Fixture {
        async fn new(status: u16) -> Self {
            let temp = tempfile::tempdir().unwrap();
            let mock = Mock::new(vec![(
                status,
                test_support::validation(&test_support::session("http://unused")),
            )])
            .await;
            let pool = test_support::database().await;
            test_support::seed(&pool, &test_support::session(&mock.url)).await;
            let spool = Arc::new(Spool::open(temp.path().join("spool")).unwrap());
            let metrics = Metrics::new("http-test").unwrap();
            let state = AppState {
                pool: pool.clone(),
                cms: CmsClient::new(metrics.clone()).unwrap(),
                uploads: UploadService::isolated(pool, spool.clone()),
                metrics,
                metrics_token: Arc::from("long-test-metrics-token"),
            };
            Self {
                _temp: temp,
                _mock: mock,
                state,
                spool,
            }
        }
        async fn request(
            &self,
            method: &str,
            uri: &str,
            credentials: Option<(&str, &str)>,
            body: &str,
        ) -> Response {
            let mut request = axum::http::Request::builder()
                .method(method)
                .uri(uri)
                .header("content-type", "multipart/form-data; boundary=test");
            if let Some((username, password)) = credentials {
                request = request
                    .header("X-Colombo-Username", username)
                    .header("X-Colombo-Password", password);
            }
            router(self.state.clone())
                .oneshot(request.body(Body::from(body.to_owned())).unwrap())
                .await
                .unwrap()
        }
    }
    const FILE: &str = "--test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"photo.jpg\"\r\n\r\nphoto-content\r\n--test--\r\n";
    const AUTH: Option<(&str, &str)> = Some(("photographer", "key"));

    #[tokio::test]
    async fn temporary_creation_failure_rejects_an_authenticated_upload() {
        if let Ok(database) = std::env::var("COLOMBO_TEMP_FIXTURE_DATABASE") {
            let pool = sqlx::PgPool::connect(&database).await.unwrap();
            let metrics = Metrics::new("temp-failure").unwrap();
            let spool = Arc::new(
                Spool::open(std::env::var("COLOMBO_TEMP_FIXTURE_SPOOL").unwrap().into()).unwrap(),
            );
            let state = AppState {
                pool: pool.clone(),
                cms: CmsClient::new(metrics.clone()).unwrap(),
                uploads: UploadService::isolated(pool.clone(), spool),
                metrics,
                metrics_token: Arc::from(""),
            };
            let request = axum::http::Request::post("/upload")
                .header("X-Colombo-Username", "photographer")
                .header("X-Colombo-Password", "key")
                .header("content-type", "multipart/form-data; boundary=test")
                .body(Body::from(FILE))
                .unwrap();
            let response = router(state).oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"],
                "Failed to process uploaded file"
            );
            pool.close().await;
            return;
        }
        let fixture = Fixture::new(200).await;
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&fixture.state.pool)
            .await
            .unwrap();
        let mut database = url::Url::parse(&test_support::database_url()).unwrap();
        database
            .query_pairs_mut()
            .append_pair("options", &format!("-csearch_path={schema}"));
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "http::tests::temporary_creation_failure_rejects_an_authenticated_upload",
            ])
            .env("COLOMBO_TEMP_FIXTURE_DATABASE", database.as_str())
            .env("COLOMBO_TEMP_FIXTURE_SPOOL", fixture.spool.root())
            .env(
                "TMPDIR",
                fixture._temp.path().join("missing-temporary-directory"),
            );
        let status = tokio::task::spawn_blocking(move || command.status().unwrap())
            .await
            .unwrap();
        assert!(status.success());
    }

    #[tokio::test]
    async fn streaming_storage_failures_and_limits_do_not_acknowledge_a_file() {
        use std::{
            pin::Pin,
            task::{Context, Poll},
        };
        struct FailingWriter {
            write_fails: bool,
        }
        impl AsyncWrite for FailingWriter {
            fn poll_write(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
                buffer: &[u8],
            ) -> Poll<std::io::Result<usize>> {
                Poll::Ready(if self.write_fails {
                    Err(std::io::Error::other("fixture write failure"))
                } else {
                    Ok(buffer.len())
                })
            }
            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
            fn poll_shutdown(
                self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<std::io::Result<()>> {
                Poll::Ready(Ok(()))
            }
        }
        #[async_trait::async_trait]
        impl DurableWrite for FailingWriter {
            async fn sync(&self) -> std::io::Result<()> {
                Err(std::io::Error::other("fixture sync failure"))
            }
        }
        for (write_fails, limit) in [(true, 100), (false, 100), (false, 1)] {
            let app = Router::new().route(
                "/",
                post(move |mut multipart: Multipart| async move {
                    let field = multipart.next_field().await.unwrap().unwrap();
                    copy_upload(field, &mut FailingWriter { write_fails }, limit)
                        .await
                        .unwrap_err()
                }),
            );
            let response = app
                .oneshot(
                    axum::http::Request::post("/")
                        .header("content-type", "multipart/form-data; boundary=test")
                        .body(Body::from(FILE))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                if limit == 1 {
                    StatusCode::PAYLOAD_TOO_LARGE
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            );
        }
        assert_eq!(
            metrics_response(Err(anyhow::anyhow!("fixture encoding failure"))).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        let mut writer = FailingWriter { write_fails: false };
        writer.flush().await.unwrap();
        writer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn oversized_multipart_requests_are_rejected_without_durable_acceptance() {
        use tokio::io::AsyncReadExt;
        use tokio_util::io::ReaderStream;

        // Exercise the production router's real limit with bounded streaming memory.
        // Overflow can occur inside the file or while skipping another field,
        // including after a complete file has already been written temporarily.
        for prefix in [
            "--test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"large.jpg\"\r\n\r\n".to_owned(),
            "--test\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n".to_owned(),
            FILE.replace("--test--\r\n", "--test\r\nContent-Disposition: form-data; name=\"metadata\"\r\n\r\n"),
        ] {
            let f = Fixture::new(200).await;
            let input = std::io::Cursor::new(prefix.into_bytes())
                .chain(tokio::io::repeat(b'x').take(MAX_UPLOAD_BYTES as u64 + 1))
                .chain(std::io::Cursor::new(b"\r\n--test--\r\n"));
            let response = router(f.state.clone())
                .oneshot(
                    axum::http::Request::post("/upload")
                        .header("X-Colombo-Username", "photographer")
                        .header("X-Colombo-Password", "key")
                        .header("content-type", "multipart/form-data; boundary=test")
                        .body(Body::from_stream(ReaderStream::new(input)))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
            let json: serde_json::Value = serde_json::from_slice(
                &response.into_body().collect().await.unwrap().to_bytes(),
            ).unwrap();
            assert_eq!(json["error"], "Upload exceeds the size limit");
            assert!(f.spool.operation_ids().unwrap().is_empty());
            assert!(std::fs::read_dir(f.spool.root().join("staging")).unwrap().next().is_none());
        }
    }

    #[tokio::test]
    async fn upload_authentication_and_database_failures_have_explicit_responses() {
        let f = Fixture::new(200).await;
        assert_eq!(
            f.request("POST", "/upload", None, FILE).await.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            f.request("POST", "/upload", Some((" ", "key")), FILE)
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        let request = axum::http::Request::post("/upload")
            .header("X-Colombo-Username", "photographer")
            .header("content-type", "multipart/form-data; boundary=test")
            .body(Body::from(FILE))
            .unwrap();
        assert_eq!(
            router(f.state.clone())
                .oneshot(request)
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            f.request("POST", "/upload", Some(("unknown", "key")), FILE)
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
        f.state.pool.close().await;
        assert_eq!(
            f.request("POST", "/upload", AUTH, FILE).await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        for status in [401, 503] {
            let f = Fixture::new(status).await;
            assert_eq!(
                f.request("POST", "/upload", AUTH, FILE).await.status(),
                StatusCode::UNAUTHORIZED
            );
        }
    }

    #[tokio::test]
    async fn upload_persists_one_complete_file_and_receipt_without_exposing_private_context() {
        let f = Fixture::new(200).await;
        let body=FILE.replace("--test--\r\n","--test\r\nContent-Disposition: form-data; name=\"file\"; filename=\"second.jpg\"\r\n\r\nignored\r\n--test--\r\n");
        let response = f.request("POST", "/upload", AUTH, &body).await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let json: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let id = Uuid::parse_str(json["operation_id"].as_str().unwrap()).unwrap();
        assert_eq!(json["status"], "accepted");
        let record = f.spool.load_record(id).unwrap();
        assert_eq!(record.original_filename, "photo.jpg");
        assert_eq!(record.content_length, 13);
        f.spool.verify_content(&record).unwrap();
        let uri = format!("/uploads/{id}");
        let response = f.request("GET", &uri, AUTH, "").await;
        assert_eq!(response.status(), StatusCode::OK);
        let receipt = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        assert!(!receipt.contains("fictional"));
        assert!(!receipt.contains("object_key"));
        assert!(!receipt.contains("private"));
        let mut record = record;
        record.state = UploadState::Expired;
        f.spool.save_record(&record).unwrap();
        assert_eq!(
            f.request("GET", &uri, AUTH, "").await.status(),
            StatusCode::GONE
        );
        std::fs::write(
            f.spool
                .root()
                .join("operations")
                .join(id.to_string())
                .join("record.json"),
            b"corrupt",
        )
        .unwrap();
        assert_eq!(
            f.request("GET", &uri, AUTH, "").await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn malformed_empty_and_unpersistable_uploads_are_never_acknowledged() {
        let f = Fixture::new(200).await;
        for body in [
            "--test--\r\n",
            "malformed\r\n",
            "--test\r\nContent-Disposition: form-data; name=\"file\"\r\n\r\nunclosed",
            &FILE.replace("photo-content", ""),
            &FILE.replace("name=\"file\"", "name=\"other\""),
        ] {
            assert_eq!(
                f.request("POST", "/upload", AUTH, body).await.status(),
                StatusCode::BAD_REQUEST
            );
        }
        let response = f
            .request(
                "POST",
                "/upload",
                AUTH,
                &FILE.replace("filename=\"photo.jpg\"", "filename=\"/\""),
            )
            .await;
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let record = f
            .spool
            .load_record(f.spool.operation_ids().unwrap()[0])
            .unwrap();
        assert_eq!(record.original_filename, "upload");
        std::fs::remove_dir(f.spool.root().join("staging")).unwrap();
        assert_eq!(
            f.request("POST", "/upload", AUTH, FILE).await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[tokio::test]
    async fn status_authorization_checks_tenant_assignment_and_dependency_health() {
        let f = Fixture::new(200).await;
        let uri = format!("/uploads/{}", Uuid::new_v4());
        assert_eq!(
            f.request("GET", &uri, None, "").await.status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            f.request("GET", &uri, Some((" ", "key")), "")
                .await
                .status(),
            StatusCode::BAD_REQUEST
        );
        let request = axum::http::Request::get(&uri)
            .header("X-Colombo-Username", "photographer")
            .body(Body::empty())
            .unwrap();
        assert_eq!(
            router(f.state.clone())
                .oneshot(request)
                .await
                .unwrap()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            f.request("GET", &uri, Some(("unknown", "key")), "")
                .await
                .status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            f.request("GET", &uri, AUTH, "").await.status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            f.request("GET", "/uploads/invalid", AUTH, "")
                .await
                .status(),
            StatusCode::NOT_FOUND
        );
        let source = f._temp.path().join("source");
        std::fs::write(&source, b"content").unwrap();
        let mut s = test_support::session(&f._mock.url);
        s.assignment_id = "other-assignment".into();
        let record = f
            .spool
            .accept(&s, SourceProtocol::Http, "photo.jpg", &source)
            .unwrap();
        assert_eq!(
            f.request(
                "GET",
                &format!("/uploads/{}", record.operation_id),
                AUTH,
                ""
            )
            .await
            .status(),
            StatusCode::NOT_FOUND
        );
        f.state.pool.close().await;
        assert_eq!(
            f.request("GET", &uri, AUTH, "").await.status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        for (status, expected) in [
            (401, StatusCode::UNAUTHORIZED),
            (503, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let f = Fixture::new(status).await;
            assert_eq!(f.request("GET", &uri, AUTH, "").await.status(), expected);
        }
    }

    #[tokio::test]
    async fn health_and_metrics_follow_the_database_and_token_boundaries() {
        let f = Fixture::new(200).await;
        assert_eq!(
            temporary_upload(Err(std::io::Error::other(
                "fixture temporary storage failure"
            )))
            .unwrap_err()
            .into_response()
            .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        let occupied = std::net::TcpListener::bind("0.0.0.0:0").unwrap();
        assert!(
            serve(occupied.local_addr().unwrap().port(), f.state.clone())
                .await
                .is_err()
        );
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            serve(0, f.state.clone()),
        )
        .await;
        assert_eq!(
            f.request("GET", "/actuator/health", None, "")
                .await
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            f.request("GET", "/private", None, "").await.status(),
            StatusCode::UNAUTHORIZED
        );
        for token in [
            None,
            Some("Basic invalid"),
            Some("Bearer wrong"),
            Some("Bearer long-test-metrics-token"),
        ] {
            let mut request = axum::http::Request::get("/actuator/prometheus");
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, token);
            }
            let response = router(f.state.clone())
                .oneshot(request.body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                if token == Some("Bearer long-test-metrics-token") {
                    StatusCode::OK
                } else {
                    StatusCode::UNAUTHORIZED
                }
            );
            if response.status() == StatusCode::OK {
                assert!(
                    response.headers()[header::CONTENT_TYPE]
                        .to_str()
                        .unwrap()
                        .starts_with("text/plain")
                );
                let body = response.into_body().collect().await.unwrap().to_bytes();
                assert!(
                    std::str::from_utf8(&body)
                        .unwrap()
                        .contains("colombo_build_identity")
                );
            }
        }
        f.state.pool.close().await;
        assert_eq!(
            f.request("GET", "/actuator/health", None, "")
                .await
                .status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn root_preserves_v1_redirect_contract() {
        let response = root().await;
        assert_eq!(response.status(), StatusCode::FOUND);
        assert_eq!(
            response.headers()[header::LOCATION],
            "https://www.youtube.com/watch?v=KieE_MLv-ZY"
        );
    }

    #[test]
    fn required_header_rejects_missing_and_invalid_values() {
        let mut headers = HeaderMap::new();
        assert!(required_header(&headers, "X-Colombo-Username").is_none());
        headers.insert("X-Colombo-Username", "photographer".parse().unwrap());
        assert_eq!(
            required_header(&headers, "X-Colombo-Username").as_deref(),
            Some("photographer")
        );
        headers.insert(
            "X-Colombo-Username",
            axum::http::HeaderValue::from_bytes(b"\xff").unwrap(),
        );
        assert!(required_header(&headers, "X-Colombo-Username").is_none());
    }

    #[test]
    fn metrics_auth_fails_closed_without_a_token() {
        assert!(!metrics_authorized("", ""));
        assert!(!metrics_authorized("configured-long-token", "wrong"));
        assert!(metrics_authorized(
            "configured-long-token",
            "configured-long-token"
        ));
    }
}
